//! Output column names the way Athena reports them.
//!
//! Trino names an anonymous output column `_colN` (N = its 0-based position
//! in the select list), allows duplicate output names, and resolves aliases
//! case-insensitively while reporting them in the case they were written
//! (`SELECT a AS "Total" ... ORDER BY total` works and the column is
//! `Total`). DataFusion names anonymous columns after the expression text
//! (`count(*)`, `Int64(1) + Int64(2)`), refuses projections with duplicate
//! names, and keeps quoted aliases case-sensitive.
//!
//! [`name_outputs`] rewrites the top-level select list *before* the
//! rewriter runs: anonymous items get an explicit `_colN` alias, mixed-case
//! aliases are lower-cased (so every later reference, which the rewriter
//! also lower-cases, resolves) and mapped back to their written case, and
//! items whose name repeats an earlier one get a unique placeholder alias.
//! [`restore_names`](super::restore_names) undoes the renames after
//! execution. Nested select lists get the `_colN` treatment from
//! [`name_nested_select`], called by the rewriter for every query level.

use sqlparser::ast::{Expr, Ident, Query, Select, SelectItem, SetExpr, Statement};

/// Placeholder prefix for duplicate output names; never a plausible user
/// alias.
const DUPLICATE_PREFIX: &str = "__glaux_dup_";

/// Placeholder alias → the name Athena should report.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutputRenames(Vec<(String, String)>);

impl OutputRenames {
    /// Whether any column needs renaming after execution.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The name to report for a result column, if it was renamed.
    pub fn original(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(placeholder, _)| placeholder == name)
            .map(|(_, original)| original.as_str())
    }
}

/// Whether an expression is the bare `localtimestamp` keyword, which the
/// parser reads as an identifier and the rewriter turns into a call.
fn is_localtimestamp(id: &Ident) -> bool {
    id.quote_style.is_none() && id.value.eq_ignore_ascii_case("localtimestamp")
}

/// The output name a select item will have after the rewriter has folded
/// identifiers to lower case, when it is a name Athena would also report
/// (alias or column reference); `None` for anonymous expressions.
fn item_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_lowercase()),
        SelectItem::UnnamedExpr(Expr::Identifier(id)) if !is_localtimestamp(id) => {
            Some(id.value.to_lowercase())
        }
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(ids)) => {
            ids.last().map(|id| id.value.to_lowercase())
        }
        _ => None,
    }
}

/// Give an anonymous item the explicit alias `name`.
fn alias_item(item: &mut SelectItem, name: &str) {
    let expr = match std::mem::replace(item, SelectItem::Wildcard(Default::default())) {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
        _ => unreachable!("only expression items are aliased"),
    };
    *item = SelectItem::ExprWithAlias {
        expr,
        alias: Ident::new(name),
    };
}

/// Name the anonymous items of a nested select list `_colN`. Duplicate
/// names are left for DataFusion to refuse (Trino allows them but no
/// outer query can reference them either way).
pub fn name_nested_select(select: &mut Select) {
    for (position, item) in select.projection.iter_mut().enumerate() {
        if item_name(item).is_none() && matches!(item, SelectItem::UnnamedExpr(_)) {
            alias_item(item, &format!("_col{position}"));
        }
    }
}

fn name_select(select: &mut Select, renames: &mut Vec<(String, String)>) {
    let mut seen: Vec<String> = Vec::new();
    for (position, item) in select.projection.iter_mut().enumerate() {
        let name = match item_name(item) {
            Some(name) => name,
            None => match item {
                SelectItem::UnnamedExpr(_) => {
                    let name = format!("_col{position}");
                    alias_item(item, &name);
                    name
                }
                // Wildcards expand to the source's names.
                _ => continue,
            },
        };
        if seen.contains(&name) {
            let placeholder = format!("{DUPLICATE_PREFIX}{position}");
            let written = written_name(item).unwrap_or_else(|| name.clone());
            alias_item(item, &placeholder);
            renames.push((placeholder, written));
            continue;
        }
        seen.push(name.clone());
        let written = written_name(item);
        if let SelectItem::ExprWithAlias { alias, .. } = item
            && let Some(written) = written
            && written != name
        {
            // Trino resolves the alias case-insensitively but reports a
            // quoted alias as written; DataFusion sees the lower-case form.
            renames.push((name.clone(), written));
            alias.value = name;
        }
    }
}

/// The alias name Trino reports: a quoted alias as written, an unquoted
/// one lower-cased.
fn written_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } if alias.quote_style.is_some() => {
            Some(alias.value.clone())
        }
        SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_lowercase()),
        _ => None,
    }
}

fn name_set_expr(body: &mut SetExpr, renames: &mut Vec<(String, String)>) {
    match body {
        SetExpr::Select(select) => name_select(select, renames),
        SetExpr::Query(query) => name_query(query, renames),
        SetExpr::SetOperation { left, right, .. } => {
            // Output names come from the left side; the right side gets the
            // same treatment so positional `_colN` names line up.
            name_set_expr(left, renames);
            let mut ignored = Vec::new();
            name_set_expr(right, &mut ignored);
        }
        _ => {}
    }
}

fn name_query(query: &mut Query, renames: &mut Vec<(String, String)>) {
    name_set_expr(&mut query.body, renames);
}

/// Apply Athena's output naming to the top-level select list of a query
/// statement. Returns the renames to undo after execution.
pub fn name_outputs(statement: &mut Statement) -> OutputRenames {
    let mut renames = Vec::new();
    if let Statement::Query(query) = statement {
        name_query(query, &mut renames);
    }
    OutputRenames(renames)
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::*;

    fn named(sql: &str) -> (String, OutputRenames) {
        let mut stmt = Parser::parse_sql(&GenericDialect, sql).unwrap().remove(0);
        let renames = name_outputs(&mut stmt);
        (stmt.to_string(), renames)
    }

    #[test]
    fn anonymous_columns_become_col_n() {
        let (sql, renames) =
            named("SELECT count(*), id, t.name, 1 + 1 AS two, x.*, localtimestamp FROM t");
        assert_eq!(
            sql,
            "SELECT count(*) AS _col0, id, t.name, 1 + 1 AS two, x.*, localtimestamp AS _col5 FROM t"
        );
        assert!(renames.is_empty());
    }

    #[test]
    fn duplicates_get_placeholders_that_map_back() {
        let (sql, renames) = named("SELECT 1 AS a, 2 AS A, id, id, 3 AS \"A\" FROM t");
        assert_eq!(
            sql,
            "SELECT 1 AS a, 2 AS __glaux_dup_1, id, id AS __glaux_dup_3, 3 AS __glaux_dup_4 FROM t"
        );
        assert_eq!(renames.original("__glaux_dup_1"), Some("a"));
        assert_eq!(renames.original("__glaux_dup_3"), Some("id"));
        assert_eq!(renames.original("__glaux_dup_4"), Some("A"));
        assert_eq!(renames.original("a"), None);
    }

    #[test]
    fn mixed_case_aliases_are_lower_cased_and_mapped_back() {
        let (sql, renames) = named("SELECT a AS \"Total\", b AS Count FROM t ORDER BY \"Total\"");
        assert_eq!(
            sql,
            // The unquoted alias is left for the rewriter's identifier fold.
            "SELECT a AS \"total\", b AS Count FROM t ORDER BY \"Total\""
        );
        assert_eq!(renames.original("total"), Some("Total"));
        assert_eq!(renames.original("count"), None);
    }

    #[test]
    fn set_operations_and_ctes_are_named_at_the_top_level_only() {
        let (sql, _) =
            named("WITH c AS (SELECT count(*) FROM t) SELECT max(n) FROM c UNION ALL SELECT 1");
        assert_eq!(
            sql,
            "WITH c AS (SELECT count(*) FROM t) SELECT max(n) AS _col0 FROM c UNION ALL SELECT 1 AS _col0"
        );
    }

    #[test]
    fn nested_naming_only_fills_anonymous_items() {
        let mut stmt =
            Parser::parse_sql(&GenericDialect, "SELECT 1, x, y AS \"Y\", 2 AS a, 3 AS a")
                .unwrap()
                .remove(0);
        let Statement::Query(query) = &mut stmt else {
            unreachable!()
        };
        let SetExpr::Select(select) = query.body.as_mut() else {
            unreachable!()
        };
        name_nested_select(select);
        assert_eq!(
            stmt.to_string(),
            "SELECT 1 AS _col0, x, y AS \"Y\", 2 AS a, 3 AS a"
        );
    }
}
