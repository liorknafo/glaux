//! Output column names the way Athena reports them.
//!
//! Trino names an anonymous output column `_colN` (N = its 0-based position
//! in the select list) and allows duplicate output names. DataFusion names
//! anonymous columns after the expression text (`count(*)`,
//! `Int64(1) + Int64(2)`) and refuses projections with duplicate names.
//!
//! [`name_outputs`] rewrites the top-level select list: anonymous items get
//! an explicit `_colN` alias, and items whose name repeats an earlier one
//! get a unique placeholder alias that [`restore_names`] maps back to the
//! original after execution. Columns named by an alias or by a plain column
//! reference keep their names.

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

/// The output name DataFusion will give a select item, when it is a name
/// Athena would also report (alias or column reference); `None` for
/// anonymous expressions.
fn item_name(item: &SelectItem) -> Option<String> {
    match item {
        SelectItem::ExprWithAlias { alias, .. } => Some(ident_name(alias)),
        SelectItem::UnnamedExpr(Expr::Identifier(id)) => Some(ident_name(id)),
        SelectItem::UnnamedExpr(Expr::CompoundIdentifier(ids)) => ids.last().map(ident_name),
        _ => None,
    }
}

/// DataFusion lower-cases unquoted identifiers and keeps quoted ones.
fn ident_name(id: &Ident) -> String {
    if id.quote_style.is_some() {
        id.value.clone()
    } else {
        id.value.to_lowercase()
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
                    let SelectItem::UnnamedExpr(expr) =
                        std::mem::replace(item, SelectItem::Wildcard(Default::default()))
                    else {
                        unreachable!()
                    };
                    *item = SelectItem::ExprWithAlias {
                        expr,
                        alias: Ident::new(name.clone()),
                    };
                    name
                }
                // Wildcards expand to the source's names.
                _ => continue,
            },
        };
        if seen.contains(&name) {
            let placeholder = format!("{DUPLICATE_PREFIX}{position}");
            let expr = match std::mem::replace(item, SelectItem::Wildcard(Default::default())) {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => unreachable!("only expression items reach here"),
            };
            *item = SelectItem::ExprWithAlias {
                expr,
                alias: Ident::new(placeholder.clone()),
            };
            renames.push((placeholder, name));
        } else {
            seen.push(name);
        }
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
        let (sql, renames) = named("SELECT count(*), id, t.name, 1 + 1 AS two, x.* FROM t");
        assert_eq!(
            sql,
            "SELECT count(*) AS _col0, id, t.name, 1 + 1 AS two, x.* FROM t"
        );
        assert!(renames.is_empty());
    }

    #[test]
    fn duplicates_get_placeholders_that_map_back() {
        let (sql, renames) = named("SELECT 1 AS a, 2 AS A, id, id, 3 AS \"A\" FROM t");
        assert_eq!(
            sql,
            "SELECT 1 AS a, 2 AS __glaux_dup_1, id, id AS __glaux_dup_3, 3 AS \"A\" FROM t"
        );
        assert_eq!(renames.original("__glaux_dup_1"), Some("a"));
        assert_eq!(renames.original("__glaux_dup_3"), Some("id"));
        assert_eq!(renames.original("a"), None);
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
}
