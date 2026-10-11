//! Corpus loading: every `*.sql` under the glaux-athena corpus directory,
//! with its directives and the metadata the differential run needs.
//!
//! Directives are `--` comments at the top of the file:
//!
//! ```sql
//! -- error: <substring>       negative case: must fail, naming <substring>
//! -- fidelity: unordered      compare rows as a multiset even though the
//!                             query has an ORDER BY (ties)
//! ```
//!
//! Whether rows are compared positionally is otherwise decided from the
//! query itself: a top-level `ORDER BY` makes the order part of the
//! contract; anything else is compared as a multiset, because Athena makes
//! no ordering promise there.

use std::fs;
use std::path::{Path, PathBuf};

use glaux_athena::dialect::translate;
use sqlparser::ast::Statement;

use crate::fixtures::TABLE_NAMES;
use crate::{HarnessError, Result};

/// How two result sets are compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compare {
    /// Row order is part of the contract (`ORDER BY`).
    Ordered,
    /// Rows are a multiset.
    Unordered,
}

impl Compare {
    /// Snapshot-header spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Compare::Ordered => "ordered",
            Compare::Unordered => "unordered",
        }
    }

    /// Parse the snapshot-header spelling.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "ordered" => Some(Compare::Ordered),
            "unordered" => Some(Compare::Unordered),
            _ => None,
        }
    }
}

/// One corpus query.
#[derive(Debug, Clone)]
pub struct Case {
    /// File stem, used as the snapshot name.
    pub name: String,
    /// The SQL text, directives included (Athena ignores the comments).
    pub sql: String,
    /// `-- error:` needle for negative cases.
    pub expect_error: Option<String>,
    /// How to compare rows.
    pub compare: Compare,
    /// Fixture tables the query references, in [`TABLE_NAMES`] order.
    pub tables: Vec<&'static str>,
}

impl Case {
    /// Positive or negative.
    pub fn is_negative(&self) -> bool {
        self.expect_error.is_some()
    }
}

/// Load every case in `dir`, sorted by name.
pub fn load(dir: &Path) -> Result<Vec<Case>> {
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|e| HarnessError::new(format!("reading corpus dir {}: {e}", dir.display())))?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::result::Result<_, _>>()?;
    paths.retain(|p| p.extension().is_some_and(|e| e == "sql"));
    paths.sort();
    if paths.len() < 20 {
        return Err(HarnessError::new(format!(
            "corpus at {} looks truncated: {} files",
            dir.display(),
            paths.len()
        )));
    }
    paths.iter().map(|p| parse_case(p)).collect()
}

fn parse_case(path: &Path) -> Result<Case> {
    let sql = fs::read_to_string(path)?;
    let name = path
        .file_stem()
        .ok_or_else(|| HarnessError::new(format!("{}: no file stem", path.display())))?
        .to_string_lossy()
        .into_owned();
    let directives: Vec<&str> = sql.lines().take_while(|l| l.starts_with("--")).collect();
    let expect_error = directives
        .iter()
        .find_map(|l| l.strip_prefix("-- error:").map(|s| s.trim().to_string()));
    let forced = directives
        .iter()
        .find_map(|l| l.strip_prefix("-- fidelity:").map(str::trim));
    let compare = match forced {
        Some("unordered") => Compare::Unordered,
        Some("ordered") => Compare::Ordered,
        Some(other) => {
            return Err(HarnessError::new(format!(
                "{name}: unknown `-- fidelity:` directive {other:?} (expected ordered|unordered)"
            )));
        }
        None => {
            if has_top_level_order_by(&sql) {
                Compare::Ordered
            } else {
                Compare::Unordered
            }
        }
    };
    Ok(Case {
        tables: referenced_tables(&sql),
        name,
        sql,
        expect_error,
        compare,
    })
}

/// `true` when the statement parses and its outermost query carries an
/// `ORDER BY`. Unparsable SQL (negative syntax cases) is `false`; it never
/// produces rows to order.
fn has_top_level_order_by(sql: &str) -> bool {
    match translate(sql) {
        Ok(Statement::Query(query)) => query.order_by.is_some(),
        _ => false,
    }
}

/// Fixture tables named in `sql` as whole identifiers.
fn referenced_tables(sql: &str) -> Vec<&'static str> {
    let words: Vec<String> = sql
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect();
    TABLE_NAMES
        .iter()
        .copied()
        .filter(|t| words.iter().any(|w| w == t))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_by_detection_follows_the_outermost_query() {
        assert!(has_top_level_order_by("SELECT id FROM orders ORDER BY id"));
        assert!(has_top_level_order_by(
            "WITH t AS (SELECT id FROM orders) SELECT * FROM t ORDER BY 1 LIMIT 3"
        ));
        assert!(!has_top_level_order_by(
            "SELECT * FROM (SELECT id FROM orders ORDER BY id) t"
        ));
        assert!(!has_top_level_order_by("SELECT count(*) FROM orders"));
        assert!(!has_top_level_order_by("SELEC broken"));
    }

    #[test]
    fn referenced_tables_match_whole_identifiers_only() {
        assert_eq!(
            referenced_tables("SELECT c.id FROM customers c JOIN countries co ON 1=1"),
            vec!["customers", "countries"]
        );
        assert_eq!(
            referenced_tables("SELECT my_orders FROM t"),
            Vec::<&str>::new()
        );
        assert_eq!(referenced_tables("select * from ORDERS"), vec!["orders"]);
    }
}
