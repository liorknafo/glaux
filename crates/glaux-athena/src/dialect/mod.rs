//! The Trino dialect layer: Athena SQL → DataFusion logical plans.
//!
//! Athena engine version 3 speaks Trino SQL. This module parses that
//! dialect with `sqlparser` (under an [`AthenaDialect`] that enables the
//! Trino syntax the generic dialect leaves off, such as lambdas, so they can
//! be refused *by name*), rewrites the AST onto DataFusion's functions and
//! syntax via the [shim registry](registry), registers the Rust [UDFs](udf)
//! DataFusion lacks, and plans the result with DataFusion's own SQL planner.
//!
//! # Never silently wrong
//!
//! - Every function call is checked against the registry. Unknown names,
//!   including DataFusion-only names, are refused with
//!   [`GlauxSqlError::UnknownFunction`].
//! - Constructs with no faithful translation (lambda expressions,
//!   `AT TIME ZONE`, zoned timestamp literals, ROW/MAP values, `date -
//!   date`, multi-statement batches) and syntax Trino does not have
//!   (`DISTINCT ON`, `QUALIFY`, `[1, 2]`, `::`, ...) are refused with
//!   [`GlauxSqlError::Unsupported`] naming the construct.
//! - Trino's operand-type rules are enforced on the planned query
//!   ([`strict`]), so queries Athena rejects with `TYPE_MISMATCH` are not
//!   quietly coerced.
//! - Format strings are translated specifier by specifier and unknown
//!   specifiers are errors, never dropped.
//! - `docs/sql-coverage.md` is rendered from the same registry, so the docs
//!   cannot drift from the engine.

pub mod error;
pub mod formats;
pub mod naming;
pub mod registry;
pub mod resolve;
pub mod rewrite;
pub mod strict;
pub mod udf;

use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SessionStateBuilder;
use datafusion::optimizer::Analyzer;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DFStatement;
use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

pub use error::GlauxSqlError;
pub use naming::OutputRenames;

use crate::engine::{DataFusionEngine, EngineError, QueryEngine, QueryOutput, QueryRequest};

/// The parsing dialect: `sqlparser`'s generic rules plus the Trino syntax
/// Athena queries use (lambda arrows, `FILTER (WHERE ...)`, expression
/// `GROUP BY`, `ARRAY[...]` literals via the generic dialect).
#[derive(Debug, Default, Clone, Copy)]
pub struct AthenaDialect;

impl Dialect for AthenaDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_'
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn supports_lambda_functions(&self) -> bool {
        true
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }

    fn supports_window_function_null_treatment_arg(&self) -> bool {
        true
    }

    fn supports_named_fn_args_with_expr_name(&self) -> bool {
        false
    }

    /// Trino's `EXTRACT(DAY_OF_WEEK FROM x)` etc. are not sqlparser
    /// keywords; let them through as custom fields for the rewriter.
    fn allow_extract_custom(&self) -> bool {
        true
    }
}

/// A translated statement plus the output-column renames to undo after
/// execution (see [`naming`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Translation {
    /// The DataFusion-dialect statement.
    pub statement: Statement,
    /// Placeholder aliases → Athena output names.
    pub renames: OutputRenames,
}

/// Parse one Trino statement and rewrite it for DataFusion.
///
/// The returned statement renders (via `Display`) as DataFusion-dialect
/// SQL, which is handy for debugging; [`TrinoEngine`] hands the AST to the
/// planner directly so nothing is re-parsed.
pub fn translate(sql: &str) -> Result<Statement, GlauxSqlError> {
    translate_full(sql).map(|t| t.statement)
}

/// [`translate`], also returning the output renames the engine must apply.
pub fn translate_full(sql: &str) -> Result<Translation, GlauxSqlError> {
    let tokens = preprocess_tokens(sql)?;
    let mut statements = Parser::new(&AthenaDialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .map_err(|e| GlauxSqlError::Parse {
            message: e.to_string(),
        })?;
    match statements.len() {
        0 => {
            return Err(GlauxSqlError::Parse {
                message: "the query is empty".to_string(),
            });
        }
        1 => {}
        n => {
            return Err(GlauxSqlError::unsupported(
                "Multiple statements",
                format!("got {n} statements; Athena runs exactly one per query execution"),
            ));
        }
    }
    let mut statement = statements.remove(0);
    naming::check_order_by_ambiguity(&statement)?;
    // Naming runs first so it sees aliases as written (case preserved) and
    // can map the folded names back; the rewriter then folds everything.
    let renames = naming::name_outputs(&mut statement);
    rewrite::rewrite_statement(&mut statement)?;
    Ok(Translation { statement, renames })
}

/// Tokenize `sql` and apply the token-level fixes the parser cannot do
/// itself:
///
/// - refuse digit-glued identifiers and `==` (Trino syntax errors);
/// - `ARRAY(T)` type syntax (Trino's) → `ARRAY<T>` (the form sqlparser
///   parses), and Hive's `ARRAY<T>` — not Trino syntax — refused by name;
/// - bare-row `VALUES 1, 2` (valid Trino) → `VALUES (1), (2)`.
fn preprocess_tokens(sql: &str) -> Result<Vec<sqlparser::tokenizer::TokenWithSpan>, GlauxSqlError> {
    let tokens = Tokenizer::new(&AthenaDialect, sql)
        .tokenize_with_location()
        .map_err(|e| GlauxSqlError::Parse {
            message: e.to_string(),
        })?;
    reject_digit_identifiers(&tokens)?;
    let tokens = convert_array_type_parens(tokens)?;
    let tokens = rewrite_bare_trim_specification(tokens);
    Ok(wrap_bare_values_rows(tokens))
}

/// The trim function a bare `TRIM(<specification> FROM x)` is: Trino's
/// default trim characters are whitespace, which is exactly `ltrim` /
/// `rtrim` / `trim`.
fn bare_trim_function(token: &Token) -> Option<&'static str> {
    match token {
        Token::Word(w) if w.quote_style.is_none() => match w.value.to_ascii_lowercase().as_str() {
            "leading" => Some("ltrim"),
            "trailing" => Some("rtrim"),
            "both" => Some("trim"),
            _ => None,
        },
        _ => None,
    }
}

/// `TRIM(LEADING FROM x)` — valid Trino, and the only `TRIM` spelling
/// sqlparser cannot parse (`Expected: ), found: ...`; the
/// `TRIM(LEADING 'x' FROM y)` form with trim characters parses fine). The
/// specification and `FROM` are dropped and `TRIM` becomes the equivalent
/// `ltrim` / `rtrim` / `trim` call, which trims whitespace exactly as
/// Trino's default does.
fn rewrite_bare_trim_specification(
    tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    let mut out: Vec<sqlparser::tokenizer::TokenWithSpan> = Vec::with_capacity(tokens.len());
    let mut i = 0;
    while i < tokens.len() {
        if is_keyword(&tokens[i].token, "trim")
            && let Some(lparen) = next_significant(&tokens, i)
            && matches!(tokens[lparen].token, Token::LParen)
            && let Some(spec) = next_significant(&tokens, lparen)
            && let Some(name) = bare_trim_function(&tokens[spec].token)
            && let Some(from) = next_significant(&tokens, spec)
            && is_keyword(&tokens[from].token, "from")
        {
            let mut renamed = tokens[i].clone();
            renamed.token = Token::make_word(name, None);
            out.push(renamed);
            out.extend(tokens[i + 1..=lparen].iter().cloned());
            i = from + 1;
            continue;
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

/// Trino lexes a digit run glued to identifier characters (`1_000`, `1AS`,
/// `0x1F`) as one token and rejects it ("identifier must not start with a
/// digit"); sqlparser splits it into a number and an identifier, which would
/// make `SELECT 1_000` a query returning `1` aliased `_000`.
fn reject_digit_identifiers(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
) -> Result<(), GlauxSqlError> {
    // `==` parses to the same AST as `=`; Trino has only `=`.
    if tokens.iter().any(|t| matches!(t.token, Token::DoubleEq)) {
        return Err(GlauxSqlError::Parse {
            message: "mismatched input '==': Trino's equality operator is '='".to_string(),
        });
    }
    for pair in tokens.windows(2) {
        if let (Token::Number(number, _), Token::Word(word)) = (&pair[0].token, &pair[1].token)
            && word.quote_style.is_none()
            && pair[0].span.end == pair[1].span.start
        {
            return Err(GlauxSqlError::Parse {
                message: format!(
                    "identifier must not start with a digit: {number}{}; surround the identifier \
                     with double quotes or separate the number from it",
                    word.value
                ),
            });
        }
    }
    Ok(())
}

/// Whether a token is whitespace (the tokenizer keeps whitespace tokens;
/// the parser skips them).
fn is_whitespace(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

/// The index of the next non-whitespace token after `i`, if any.
fn next_significant(tokens: &[sqlparser::tokenizer::TokenWithSpan], i: usize) -> Option<usize> {
    tokens
        .iter()
        .enumerate()
        .skip(i + 1)
        .find(|(_, t)| !is_whitespace(&t.token))
        .map(|(j, _)| j)
}

/// Whether the token is the unquoted keyword `word` (case-insensitive).
fn is_keyword(token: &Token, word: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

/// Trino writes array types `ARRAY(INTEGER)`; sqlparser only parses the
/// Hive/BigQuery form `ARRAY<INTEGER>` (`Expected: <, found: (`). The
/// parentheses directly after an `ARRAY` keyword are converted to angle
/// brackets so Trino's form parses, and a literal `ARRAY<...>` — not Trino
/// syntax — is refused by name.
fn convert_array_type_parens(
    mut tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Result<Vec<sqlparser::tokenizer::TokenWithSpan>, GlauxSqlError> {
    // Depth-indexed stack: `true` for parens opened directly after ARRAY.
    let mut stack: Vec<bool> = Vec::new();
    let mut previous_significant: Option<Token> = None;
    for entry in &mut tokens {
        let token = entry.token.clone();
        if is_whitespace(&token) {
            continue;
        }
        let after_array = previous_significant
            .as_ref()
            .is_some_and(|t| is_keyword(t, "array"));
        match &token {
            Token::Lt if after_array => {
                return Err(GlauxSqlError::unsupported(
                    "ARRAY<...> type syntax",
                    "`ARRAY<INTEGER>` is Hive syntax, not Trino's; write ARRAY(INTEGER)",
                ));
            }
            Token::LParen => {
                if after_array {
                    entry.token = Token::Lt;
                }
                stack.push(after_array);
            }
            Token::RParen if stack.pop().unwrap_or(false) => {
                entry.token = Token::Gt;
            }
            _ => {}
        }
        previous_significant = Some(token);
    }
    Ok(tokens)
}

/// Keywords that end a `VALUES` row list at depth 0.
fn ends_values_list(token: &Token) -> bool {
    [
        "order",
        "limit",
        "offset",
        "fetch",
        "union",
        "except",
        "intersect",
    ]
    .iter()
    .any(|k| is_keyword(token, k))
        || matches!(token, Token::SemiColon)
}

/// Trino allows `VALUES 1, 2` — each row a bare expression; sqlparser
/// requires parenthesised rows (`Expected: (, found: 1`). When the first
/// row after `VALUES` is bare, every top-level comma-separated row is
/// wrapped in parentheses. Applied repeatedly so `VALUES` bodies nested in
/// subqueries are covered too.
fn wrap_bare_values_rows(
    mut tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    fn starts_query_body(previous: Option<&Token>) -> bool {
        match previous {
            // Statement start.
            None => true,
            Some(Token::LParen) => true,
            Some(t) => ["union", "except", "intersect", "all", "distinct"]
                .iter()
                .any(|k| is_keyword(t, k)),
        }
    }
    let empty = |token: Token| sqlparser::tokenizer::TokenWithSpan {
        token,
        span: sqlparser::tokenizer::Span::empty(),
    };
    'restart: loop {
        let mut previous_significant: Option<usize> = None;
        for i in 0..tokens.len() {
            if is_whitespace(&tokens[i].token) {
                continue;
            }
            let starts = starts_query_body(previous_significant.map(|p| &tokens[p].token));
            if is_keyword(&tokens[i].token, "values")
                && starts
                && let Some(first) = next_significant(&tokens, i)
                && !matches!(tokens[first].token, Token::LParen)
                && !is_keyword(&tokens[first].token, "row")
                && !ends_values_list(&tokens[first].token)
            {
                // Wrap each depth-0 comma-separated row in parentheses.
                let mut out = tokens[..first].to_vec();
                out.push(empty(Token::LParen));
                let mut depth = 0usize;
                let mut rest = first;
                while rest < tokens.len() {
                    let token = &tokens[rest].token;
                    match token {
                        Token::LParen | Token::LBracket => depth += 1,
                        Token::RParen | Token::RBracket => {
                            if depth == 0 {
                                break;
                            }
                            depth -= 1;
                        }
                        Token::Comma if depth == 0 => {
                            out.push(empty(Token::RParen));
                            out.push(tokens[rest].clone());
                            out.push(empty(Token::LParen));
                            rest += 1;
                            continue;
                        }
                        t if depth == 0 && ends_values_list(t) => break,
                        _ => {}
                    }
                    out.push(tokens[rest].clone());
                    rest += 1;
                }
                out.push(empty(Token::RParen));
                out.extend_from_slice(&tokens[rest..]);
                tokens = out;
                continue 'restart;
            }
            previous_significant = Some(i);
        }
        return tokens;
    }
}

/// Result types Athena cannot return: an `interval` (DataFusion produces one
/// for `interval + interval`) or a decimal above precision 38. Checked on
/// the planned output schema so the refusal is a planning error naming the
/// type.
fn check_output_types(plan: &datafusion::logical_expr::LogicalPlan) -> Result<(), GlauxSqlError> {
    for field in plan.schema().fields() {
        match field.data_type() {
            arrow::datatypes::DataType::Interval(_) | arrow::datatypes::DataType::Duration(_) => {
                return Err(GlauxSqlError::unsupported(
                    "interval result",
                    format!(
                        "column {} is an interval; glaux cannot return interval values in v0.1 \
                         (apply the interval to a date or timestamp instead)",
                        field.name()
                    ),
                ));
            }
            arrow::datatypes::DataType::Decimal256(..) => {
                return Err(GlauxSqlError::unsupported(
                    "DECIMAL precision above 38",
                    format!(
                        "column {} exceeds Trino's maximum decimal precision",
                        field.name()
                    ),
                ));
            }
            arrow::datatypes::DataType::Decimal128(p, _) if *p > 38 => {
                return Err(GlauxSqlError::unsupported(
                    "DECIMAL precision above 38",
                    format!(
                        "column {} exceeds Trino's maximum decimal precision",
                        field.name()
                    ),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Rename result columns back to the names Athena reports.
fn restore_names(mut output: QueryOutput, renames: &OutputRenames) -> QueryOutput {
    if renames.is_empty() {
        return output;
    }
    let fields: Vec<Field> = output
        .schema
        .fields()
        .iter()
        .map(|f| match renames.original(f.name()) {
            Some(original) => f.as_ref().clone().with_name(original),
            None => f.as_ref().clone(),
        })
        .collect();
    let schema = Arc::new(Schema::new_with_metadata(
        fields,
        output.schema.metadata().clone(),
    ));
    output.batches = output
        .batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                .expect("renaming fields keeps the batch valid")
        })
        .collect();
    output.schema = schema;
    output
}

/// The Trino-dialect engine: [`translate`] + DataFusion planning and
/// execution through the wrapped [`DataFusionEngine`].
pub struct TrinoEngine {
    inner: DataFusionEngine,
}

impl std::fmt::Debug for TrinoEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoEngine")
            .field("inner", &self.inner)
            .finish()
    }
}

impl TrinoEngine {
    /// Wrap a context, registering the Trino UDFs on it and configuring
    /// DataFusion for Trino semantics (decimal literals, overflow-checked
    /// integer arithmetic). `default_catalog` is the DataFusion catalog name
    /// Athena's `AwsDataCatalog` maps to.
    pub fn new(ctx: SessionContext, default_catalog: impl Into<String>) -> Self {
        for udf in udf::all() {
            ctx.register_udf(udf);
        }
        for udaf in udf::all_aggregates() {
            ctx.register_udaf(udaf);
        }
        let mut state = ctx.state();
        // Trino: `1.5` is DECIMAL(2,1), not DOUBLE.
        state
            .config_mut()
            .options_mut()
            .sql_parser
            .parse_float_as_decimal = true;
        // Trino's typing rules must see the operand types before DataFusion
        // coerces them, so the rule goes in front of the default analyzer
        // rules (type coercion included).
        let mut rules = Analyzer::default().rules;
        rules.insert(0, Arc::new(udf::arithmetic::TrinoSemantics));
        let state = SessionStateBuilder::new_from_existing(state)
            .with_analyzer_rules(rules)
            .build();
        Self {
            inner: DataFusionEngine::new(SessionContext::new_with_state(state), default_catalog),
        }
    }

    /// The wrapped context.
    pub fn context(&self) -> &SessionContext {
        self.inner.context()
    }
}

#[async_trait]
impl QueryEngine for TrinoEngine {
    async fn execute(&self, request: QueryRequest) -> Result<QueryOutput, EngineError> {
        let started = Instant::now();
        let ctx = self.inner.context_for(&request)?;
        let Translation {
            mut statement,
            renames,
        } = translate_full(&request.sql)?;
        resolve::resolve(&ctx, &mut statement).await?;
        let plan = ctx
            .state()
            .statement_to_plan(DFStatement::Statement(Box::new(statement)))
            .await
            .map_err(crate::engine::plan_error)?;
        strict::check(&plan)?;
        check_output_types(&plan)?;
        let output = crate::engine::run_logical_plan(&ctx, plan, started).await?;
        Ok(restore_names(output, &renames))
    }
}

/// Convenience: a [`TrinoEngine`] over `ctx` as a shareable trait object.
pub fn trino_engine(
    ctx: SessionContext,
    default_catalog: impl Into<String>,
) -> Arc<dyn QueryEngine> {
    Arc::new(TrinoEngine::new(ctx, default_catalog))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translate_rejects_batches_and_empty_input_by_name() {
        let err = translate("SELECT 1; SELECT 2").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::Unsupported { construct, .. } if construct == "Multiple statements"),
            "{err}"
        );
        assert!(matches!(translate("   "), Err(GlauxSqlError::Parse { .. })));
        assert!(
            matches!(translate("SELEC 1"), Err(GlauxSqlError::Parse { message }) if message.contains("SELEC"))
        );
    }

    #[test]
    fn digit_identifiers_are_syntax_errors() {
        for sql in ["SELECT 1_000", "SELECT 1AS x", "SELECT 1.5x FROM t"] {
            let err = translate(sql).unwrap_err();
            assert!(
                matches!(&err, GlauxSqlError::Parse { message } if message.contains("must not start with a digit")),
                "{sql}: {err}"
            );
        }
        translate("SELECT 1 AS x, 1e5, 2.5 FROM t WHERE a = 1").unwrap();
    }

    #[test]
    fn lambdas_parse_under_the_athena_dialect_and_are_refused_by_name() {
        let err = translate("SELECT transform(a, x -> x + 1) FROM t").unwrap_err();
        // `transform` itself is refused first (post-order: the lambda is an
        // argument, so it is visited before the call).
        assert!(err.to_string().contains("lambda"), "{err}");
        let err = translate("SELECT array_sort(a, (x, y) -> 1) FROM t").unwrap_err();
        assert!(err.to_string().contains("lambda"), "{err}");
    }

    #[test]
    fn bare_values_rows_are_wrapped_in_parens() {
        assert_eq!(
            translate("VALUES 1, 2").unwrap().to_string(),
            "SELECT column1 AS _col0 FROM (VALUES (1), (2)) AS __glaux_values"
        );
        assert_eq!(
            translate("SELECT x FROM (VALUES 1, (2), 1 + 2) t(x)")
                .unwrap()
                .to_string(),
            "SELECT x FROM (SELECT column1 AS _col0 FROM (VALUES (1), ((2)), (1 + 2)) AS __glaux_values) t (x)"
        );
        // Terminators stop the row list; nested commas stay inside their
        // parens / brackets.
        translate("VALUES abs(-1), 2 ORDER BY 1 LIMIT 1").unwrap();
        translate("SELECT x FROM (VALUES ARRAY[1, 2], ARRAY[3]) t(x)").unwrap();
        translate("VALUES 1 UNION ALL VALUES 2").unwrap();
        // Parenthesised rows are untouched (a two-column row is not a
        // nested expression).
        assert_eq!(
            translate("SELECT * FROM (VALUES (1, 2))")
                .unwrap()
                .to_string(),
            "SELECT * FROM (SELECT column1 AS _col0, column2 AS _col1 FROM (VALUES (1, 2)) AS __glaux_values)"
        );
    }

    #[test]
    fn array_type_syntax_is_trinos_not_hives() {
        // Trino's ARRAY(T) parses (the tokens are converted to the angle-
        // bracket form sqlparser understands), nesting included.
        translate("SELECT CAST(NULL AS ARRAY(INTEGER))").unwrap();
        translate("SELECT CAST(NULL AS ARRAY(DECIMAL(2,1)))").unwrap();
        translate("SELECT CAST(NULL AS ARRAY(ARRAY(INTEGER)))").unwrap();
        // Hive's ARRAY<T> is refused by name.
        let err = translate("SELECT CAST(NULL AS ARRAY<INTEGER>)").unwrap_err();
        assert!(
            matches!(&err, GlauxSqlError::Unsupported { construct, .. } if construct == "ARRAY<...> type syntax"),
            "{err}"
        );
        // ARRAY[...] literals are untouched.
        translate("SELECT ARRAY[1, 2]").unwrap();
    }

    #[test]
    fn translated_sql_renders_datafusion_dialect() {
        let stmt = translate("SELECT if(x > 1, 'a') AS v, TRY_CAST(s AS BIGINT) FROM t").unwrap();
        assert_eq!(
            stmt.to_string(),
            "SELECT CASE WHEN x > 1 THEN 'a' END AS v, TRY_CAST(trino_round_for_cast(s) AS BIGINT) AS _col1 FROM t"
        );
    }
}
