//! The Trino function shim registry and SQL construct coverage table.
//!
//! This table is the single source of truth for what the dialect layer
//! accepts: the rewriter consults it for every function call, and
//! `docs/sql-coverage.md` is rendered from it (a test asserts the checked-in
//! file is fresh). A function that is not listed here is refused up front —
//! even when DataFusion happens to have a function of the same name —
//! because same-named functions with different semantics (`concat` and
//! NULLs, `regexp_replace` and global replacement, `repeat` producing a
//! string instead of an array) are exactly how an emulator ends up silently
//! wrong.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::sync::LazyLock;

/// How a Trino function reaches DataFusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShimKind {
    /// Same name, same semantics: the call is handed to DataFusion as-is.
    Passthrough,
    /// Rewritten at the AST level onto DataFusion functions or syntax.
    Rewrite,
    /// Implemented as a Rust UDF registered under the Trino name.
    Udf,
    /// Recognised and refused with an explicit error naming the function.
    Unsupported,
}

impl ShimKind {
    /// Label used in the coverage table.
    pub fn label(self) -> &'static str {
        match self {
            Self::Passthrough => "passthrough",
            Self::Rewrite => "rewrite",
            Self::Udf => "udf",
            Self::Unsupported => "unsupported",
        }
    }
}

/// One row of the function shim table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FunctionShim {
    /// Lower-case Trino function name.
    pub name: &'static str,
    /// Trino signature, for the docs.
    pub signature: &'static str,
    /// Coverage table category.
    pub category: &'static str,
    /// How the call is handled.
    pub kind: ShimKind,
    /// Human description of the translation (or of why it is refused).
    pub translation: &'static str,
    /// DataFusion functions the translation emits (checked to exist by a
    /// test, so a DataFusion upgrade that renames one fails loudly).
    pub df_functions: &'static [&'static str],
}

/// Whether a SQL construct (as opposed to a function) is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstructStatus {
    /// Parses and plans; exercised by the corpus.
    Supported,
    /// Refused with an explicit error naming the construct.
    Unsupported,
}

/// One row of the construct coverage table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Construct {
    /// Construct name as it appears in error messages.
    pub name: &'static str,
    /// Category for the docs.
    pub category: &'static str,
    /// Supported or refused.
    pub status: ConstructStatus,
    /// Notes (semantics, caveats, what to use instead).
    pub notes: &'static str,
    /// A substring that must appear in at least one corpus query (upper-
    /// cased comparison) so the "supported" claim is backed by a test.
    pub corpus_marker: &'static str,
}

macro_rules! shim {
    ($name:literal, $sig:literal, $cat:literal, $kind:ident, $how:literal, [$($df:literal),*]) => {
        FunctionShim {
            name: $name,
            signature: $sig,
            category: $cat,
            kind: ShimKind::$kind,
            translation: $how,
            df_functions: &[$($df),*],
        }
    };
}

/// The function shim table. Keep it sorted by category, then name, so the
/// rendered docs stay diff-friendly.
pub static FUNCTIONS: &[FunctionShim] = &[
    // --- Aggregate ---------------------------------------------------------
    shim!(
        "approx_distinct",
        "approx_distinct(x) → bigint",
        "Aggregate",
        Passthrough,
        "DataFusion `approx_distinct` (HyperLogLog; estimates differ from Trino's within the usual error bound)",
        ["approx_distinct"]
    ),
    shim!(
        "approx_percentile",
        "approx_percentile(x, percentage) → same as x",
        "Aggregate",
        Rewrite,
        "`approx_percentile_cont(x, percentage)` (t-digest). The weighted and array-of-percentages forms are refused.",
        ["approx_percentile_cont"]
    ),
    shim!(
        "arbitrary",
        "arbitrary(x) → same as x",
        "Aggregate",
        Rewrite,
        "`first_value(x)`",
        ["first_value"]
    ),
    shim!(
        "array_agg",
        "array_agg(x) → array",
        "Aggregate",
        Passthrough,
        "DataFusion `array_agg` (supports `ORDER BY` inside the call and `DISTINCT`)",
        ["array_agg"]
    ),
    shim!(
        "avg",
        "avg(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `avg`",
        ["avg"]
    ),
    shim!(
        "bool_and",
        "bool_and(boolean) → boolean",
        "Aggregate",
        Passthrough,
        "DataFusion `bool_and`",
        ["bool_and"]
    ),
    shim!(
        "bool_or",
        "bool_or(boolean) → boolean",
        "Aggregate",
        Passthrough,
        "DataFusion `bool_or`",
        ["bool_or"]
    ),
    shim!(
        "corr",
        "corr(y, x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `corr`",
        ["corr"]
    ),
    shim!(
        "count",
        "count(*) / count(x) / count(DISTINCT x) → bigint",
        "Aggregate",
        Passthrough,
        "DataFusion `count`",
        ["count"]
    ),
    shim!(
        "count_if",
        "count_if(boolean) → bigint",
        "Aggregate",
        Rewrite,
        "`count(CASE WHEN x THEN 1 END)` (keeps `FILTER` / `OVER` clauses)",
        ["count"]
    ),
    shim!(
        "covar_pop",
        "covar_pop(y, x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `covar_pop`",
        ["covar_pop"]
    ),
    shim!(
        "covar_samp",
        "covar_samp(y, x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `covar_samp`",
        ["covar_samp"]
    ),
    shim!(
        "every",
        "every(boolean) → boolean",
        "Aggregate",
        Rewrite,
        "`bool_and(x)`",
        ["bool_and"]
    ),
    shim!(
        "listagg",
        "listagg(x, separator) WITHIN GROUP (...)",
        "Aggregate",
        Unsupported,
        "Refused. Use `array_join(array_agg(x ORDER BY ...), separator)`.",
        []
    ),
    shim!(
        "max",
        "max(x)",
        "Aggregate",
        Passthrough,
        "DataFusion `max`",
        ["max"]
    ),
    shim!(
        "max_by",
        "max_by(x, y)",
        "Aggregate",
        Unsupported,
        "Refused: DataFusion 55 has no `max_by`. Rewrite with a window function (`row_number() OVER (ORDER BY y DESC)`).",
        []
    ),
    shim!(
        "min",
        "min(x)",
        "Aggregate",
        Passthrough,
        "DataFusion `min`",
        ["min"]
    ),
    shim!(
        "min_by",
        "min_by(x, y)",
        "Aggregate",
        Unsupported,
        "Refused: DataFusion 55 has no `min_by`. Rewrite with a window function.",
        []
    ),
    shim!(
        "stddev",
        "stddev(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `stddev` (sample)",
        ["stddev"]
    ),
    shim!(
        "stddev_pop",
        "stddev_pop(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `stddev_pop`",
        ["stddev_pop"]
    ),
    shim!(
        "stddev_samp",
        "stddev_samp(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `stddev_samp`",
        ["stddev_samp"]
    ),
    shim!(
        "sum",
        "sum(x)",
        "Aggregate",
        Passthrough,
        "DataFusion `sum`",
        ["sum"]
    ),
    shim!(
        "var_pop",
        "var_pop(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `var_pop`",
        ["var_pop"]
    ),
    shim!(
        "var_samp",
        "var_samp(x) → double",
        "Aggregate",
        Passthrough,
        "DataFusion `var_samp`",
        ["var_samp"]
    ),
    shim!(
        "variance",
        "variance(x) → double",
        "Aggregate",
        Rewrite,
        "`var_samp(x)`",
        ["var_samp"]
    ),
    // --- Window ------------------------------------------------------------
    shim!(
        "cume_dist",
        "cume_dist() OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `cume_dist`",
        ["cume_dist"]
    ),
    shim!(
        "dense_rank",
        "dense_rank() OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `dense_rank`",
        ["dense_rank"]
    ),
    shim!(
        "first_value",
        "first_value(x) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `first_value`",
        ["first_value"]
    ),
    shim!(
        "lag",
        "lag(x[, offset[, default]]) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `lag`",
        ["lag"]
    ),
    shim!(
        "last_value",
        "last_value(x) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `last_value`",
        ["last_value"]
    ),
    shim!(
        "lead",
        "lead(x[, offset[, default]]) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `lead`",
        ["lead"]
    ),
    shim!(
        "nth_value",
        "nth_value(x, n) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `nth_value`",
        ["nth_value"]
    ),
    shim!(
        "ntile",
        "ntile(n) OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `ntile`",
        ["ntile"]
    ),
    shim!(
        "percent_rank",
        "percent_rank() OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `percent_rank`",
        ["percent_rank"]
    ),
    shim!(
        "rank",
        "rank() OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `rank`",
        ["rank"]
    ),
    shim!(
        "row_number",
        "row_number() OVER (...)",
        "Window",
        Passthrough,
        "DataFusion `row_number`",
        ["row_number"]
    ),
    // --- Conditional -------------------------------------------------------
    shim!(
        "coalesce",
        "coalesce(a, b, ...)",
        "Conditional",
        Passthrough,
        "DataFusion `coalesce`",
        ["coalesce"]
    ),
    shim!(
        "if",
        "if(condition, true_value[, false_value])",
        "Conditional",
        Rewrite,
        "`CASE WHEN condition THEN true_value ELSE false_value END` (`ELSE NULL` when omitted)",
        []
    ),
    shim!(
        "nullif",
        "nullif(a, b)",
        "Conditional",
        Passthrough,
        "DataFusion `nullif`",
        ["nullif"]
    ),
    shim!(
        "try",
        "try(expr)",
        "Conditional",
        Unsupported,
        "Refused: DataFusion has no error-suppressing wrapper. `TRY_CAST` covers the cast case.",
        []
    ),
    shim!(
        "typeof",
        "typeof(expr) → varchar",
        "Conditional",
        Unsupported,
        "Refused: DataFusion's type names (`Utf8`, `Int64`) are not Trino's.",
        []
    ),
    // --- String ------------------------------------------------------------
    shim!(
        "chr",
        "chr(n) → varchar",
        "String",
        Passthrough,
        "DataFusion `chr`",
        ["chr"]
    ),
    shim!(
        "codepoint",
        "codepoint(varchar) → integer",
        "String",
        Rewrite,
        "`ascii(x)`",
        ["ascii"]
    ),
    shim!(
        "concat",
        "concat(a, b, ...) → varchar | array",
        "String",
        Rewrite,
        "`a || b || ...`. DataFusion's own `concat` skips NULL arguments where Trino returns NULL; the operator form propagates NULL like Trino.",
        []
    ),
    shim!(
        "length",
        "length(varchar) → bigint",
        "String",
        Passthrough,
        "DataFusion `length` (characters, not bytes)",
        ["length"]
    ),
    shim!(
        "levenshtein_distance",
        "levenshtein_distance(a, b) → bigint",
        "String",
        Rewrite,
        "`levenshtein(a, b)`",
        ["levenshtein"]
    ),
    shim!(
        "lower",
        "lower(varchar)",
        "String",
        Passthrough,
        "DataFusion `lower`",
        ["lower"]
    ),
    shim!(
        "lpad",
        "lpad(varchar, size, padstring)",
        "String",
        Passthrough,
        "DataFusion `lpad`",
        ["lpad"]
    ),
    shim!(
        "ltrim",
        "ltrim(varchar)",
        "String",
        Passthrough,
        "DataFusion `ltrim`",
        ["ltrim"]
    ),
    shim!(
        "replace",
        "replace(varchar, search[, replacement])",
        "String",
        Rewrite,
        "DataFusion `replace`; the 2-argument (delete) form becomes `replace(x, search, '')`",
        ["replace"]
    ),
    shim!(
        "reverse",
        "reverse(varchar)",
        "String",
        Passthrough,
        "DataFusion `reverse`",
        ["reverse"]
    ),
    shim!(
        "rpad",
        "rpad(varchar, size, padstring)",
        "String",
        Passthrough,
        "DataFusion `rpad`",
        ["rpad"]
    ),
    shim!(
        "rtrim",
        "rtrim(varchar)",
        "String",
        Passthrough,
        "DataFusion `rtrim`",
        ["rtrim"]
    ),
    shim!(
        "split",
        "split(varchar, delimiter) → array(varchar)",
        "String",
        Rewrite,
        "`string_to_array(x, delimiter)`. The 3-argument `split(x, delimiter, limit)` form is refused.",
        ["string_to_array"]
    ),
    shim!(
        "split_part",
        "split_part(varchar, delimiter, index) → varchar",
        "String",
        Passthrough,
        "DataFusion `split_part` (1-based, like Trino)",
        ["split_part"]
    ),
    shim!(
        "starts_with",
        "starts_with(varchar, prefix) → boolean",
        "String",
        Passthrough,
        "DataFusion `starts_with`",
        ["starts_with"]
    ),
    shim!(
        "strpos",
        "strpos(varchar, substring) → bigint",
        "String",
        Passthrough,
        "DataFusion `strpos` (1-based, 0 when absent). The 3-argument `strpos(x, sub, instance)` form is refused.",
        ["strpos"]
    ),
    shim!(
        "substr",
        "substr(varchar, start[, length])",
        "String",
        Passthrough,
        "DataFusion `substr` (1-based)",
        ["substr"]
    ),
    shim!(
        "substring",
        "substring(varchar, start[, length])",
        "String",
        Passthrough,
        "DataFusion `substring` (1-based)",
        ["substring"]
    ),
    shim!(
        "translate",
        "translate(varchar, from, to)",
        "String",
        Passthrough,
        "DataFusion `translate`",
        ["translate"]
    ),
    shim!(
        "trim",
        "trim(varchar)",
        "String",
        Passthrough,
        "DataFusion `trim` (also the `TRIM(BOTH ... FROM ...)` syntax)",
        ["trim"]
    ),
    shim!(
        "upper",
        "upper(varchar)",
        "String",
        Passthrough,
        "DataFusion `upper`",
        ["upper"]
    ),
    // --- Regular expression ------------------------------------------------
    shim!(
        "regexp_extract",
        "regexp_extract(varchar, pattern[, group]) → varchar",
        "Regular expression",
        Rewrite,
        "`array_element(regexp_match(x, '(' || pattern || ')'), group + 1)` — the pattern is wrapped in a capturing group so group 0 (the whole match) is addressable; `group` must be a literal.",
        ["array_element", "regexp_match"]
    ),
    shim!(
        "regexp_extract_all",
        "regexp_extract_all(varchar, pattern[, group]) → array(varchar)",
        "Regular expression",
        Unsupported,
        "Refused: DataFusion's `regexp_match` returns only the first match.",
        []
    ),
    shim!(
        "regexp_like",
        "regexp_like(varchar, pattern) → boolean",
        "Regular expression",
        Passthrough,
        "DataFusion `regexp_like` (Rust `regex` syntax, a close superset of Java's for common patterns)",
        ["regexp_like"]
    ),
    shim!(
        "regexp_replace",
        "regexp_replace(varchar, pattern[, replacement]) → varchar",
        "Regular expression",
        Rewrite,
        "`regexp_replace(x, pattern, replacement, 'g')` — Trino replaces every match, DataFusion only the first unless flagged. The lambda form is refused.",
        ["regexp_replace"]
    ),
    shim!(
        "regexp_split",
        "regexp_split(varchar, pattern) → array(varchar)",
        "Regular expression",
        Unsupported,
        "Refused: no DataFusion equivalent.",
        []
    ),
    // --- Date and time -----------------------------------------------------
    shim!(
        "at_timezone",
        "at_timezone(timestamp, zone)",
        "Date and time",
        Unsupported,
        "Refused in v0.1 along with `AT TIME ZONE`.",
        []
    ),
    shim!(
        "current_date",
        "current_date → date",
        "Date and time",
        Passthrough,
        "DataFusion `current_date`",
        ["current_date"]
    ),
    shim!(
        "current_time",
        "current_time → time",
        "Date and time",
        Passthrough,
        "DataFusion `current_time`",
        ["current_time"]
    ),
    shim!(
        "current_timestamp",
        "current_timestamp → timestamp",
        "Date and time",
        Passthrough,
        "DataFusion `current_timestamp` (UTC)",
        ["current_timestamp"]
    ),
    shim!(
        "date",
        "date(x) → date",
        "Date and time",
        Rewrite,
        "`CAST(x AS DATE)`",
        []
    ),
    shim!(
        "date_add",
        "date_add(unit, value, timestamp) → same type",
        "Date and time",
        Udf,
        "Rust UDF: calendar arithmetic for month/quarter/year (clamps to month end), fixed lengths otherwise. Units: millisecond … year; adding sub-day units to a DATE is an error.",
        ["date_add"]
    ),
    shim!(
        "date_diff",
        "date_diff(unit, timestamp1, timestamp2) → bigint",
        "Date and time",
        Udf,
        "Rust UDF: `timestamp2 - timestamp1` in whole units, truncated toward zero; calendar months for month/quarter/year.",
        ["date_diff"]
    ),
    shim!(
        "date_format",
        "date_format(timestamp, format) → varchar",
        "Date and time",
        Rewrite,
        "`to_char(x, <strftime>)` with the MySQL-style format translated specifier by specifier; the format must be a literal and unknown specifiers are refused.",
        ["to_char"]
    ),
    shim!(
        "date_parse",
        "date_parse(varchar, format) → timestamp",
        "Date and time",
        Rewrite,
        "`to_timestamp(x, <strftime>)` with the MySQL-style format translated; the format must be a literal.",
        ["to_timestamp"]
    ),
    shim!(
        "date_trunc",
        "date_trunc(unit, timestamp) → timestamp",
        "Date and time",
        Passthrough,
        "DataFusion `date_trunc` (same argument order and unit names)",
        ["date_trunc"]
    ),
    shim!(
        "day",
        "day(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('day', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "day_of_month",
        "day_of_month(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('day', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "day_of_week",
        "day_of_week(x) → bigint (1 = Monday … 7 = Sunday)",
        "Date and time",
        Rewrite,
        "`date_part('dow', x)` remapped from DataFusion's 0 = Sunday to Trino's ISO numbering",
        ["date_part"]
    ),
    shim!(
        "day_of_year",
        "day_of_year(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('doy', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "dow",
        "dow(x) → bigint",
        "Date and time",
        Rewrite,
        "Alias of `day_of_week`",
        ["date_part"]
    ),
    shim!(
        "doy",
        "doy(x) → bigint",
        "Date and time",
        Rewrite,
        "Alias of `day_of_year`",
        ["date_part"]
    ),
    shim!(
        "format_datetime",
        "format_datetime(timestamp, pattern) → varchar",
        "Date and time",
        Rewrite,
        "`to_char(x, <strftime>)` with the Joda pattern translated; the pattern must be a literal and unknown pattern letters are refused.",
        ["to_char"]
    ),
    shim!(
        "from_iso8601_date",
        "from_iso8601_date(varchar) → date",
        "Date and time",
        Rewrite,
        "`to_date(x)`",
        ["to_date"]
    ),
    shim!(
        "from_iso8601_timestamp",
        "from_iso8601_timestamp(varchar) → timestamp with time zone",
        "Date and time",
        Rewrite,
        "`to_timestamp_millis(x)`; the instant is preserved but rendered in UTC rather than the input's offset.",
        ["to_timestamp_millis"]
    ),
    shim!(
        "from_unixtime",
        "from_unixtime(double) → timestamp",
        "Date and time",
        Rewrite,
        "`arrow_cast(CAST(x * 1000 AS BIGINT), 'Timestamp(Millisecond, None)')` (millisecond precision, like Athena). The zone-argument forms are refused.",
        ["arrow_cast"]
    ),
    shim!(
        "hour",
        "hour(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('hour', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "last_day_of_month",
        "last_day_of_month(x) → date",
        "Date and time",
        Unsupported,
        "Refused: no DataFusion equivalent. Use `date_add('day', -1, date_add('month', 1, date_trunc('month', x)))`.",
        []
    ),
    shim!(
        "localtimestamp",
        "localtimestamp → timestamp",
        "Date and time",
        Rewrite,
        "`now()` (UTC)",
        ["now"]
    ),
    shim!(
        "minute",
        "minute(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('minute', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "month",
        "month(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('month', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "now",
        "now() → timestamp with time zone",
        "Date and time",
        Passthrough,
        "DataFusion `now` (UTC)",
        ["now"]
    ),
    shim!(
        "parse_datetime",
        "parse_datetime(varchar, pattern) → timestamp",
        "Date and time",
        Rewrite,
        "`to_timestamp(x, <strftime>)` with the Joda pattern translated; the pattern must be a literal.",
        ["to_timestamp"]
    ),
    shim!(
        "quarter",
        "quarter(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('quarter', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "second",
        "second(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('second', x) AS BIGINT)`",
        ["date_part"]
    ),
    shim!(
        "to_iso8601",
        "to_iso8601(x) → varchar",
        "Date and time",
        Unsupported,
        "Refused: the output depends on the argument type. Use `format_datetime(x, 'yyyy-MM-dd''T''HH:mm:ss.SSS')` or `CAST(x AS VARCHAR)`.",
        []
    ),
    shim!(
        "to_unixtime",
        "to_unixtime(timestamp) → double",
        "Date and time",
        Rewrite,
        "`CAST(arrow_cast(arrow_cast(x, 'Timestamp(Microsecond, None)'), 'Int64') AS DOUBLE) / 1000000` (keeps fractional seconds)",
        ["arrow_cast"]
    ),
    shim!(
        "week",
        "week(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('week', x) AS BIGINT)` (ISO week)",
        ["date_part"]
    ),
    shim!(
        "week_of_year",
        "week_of_year(x) → bigint",
        "Date and time",
        Rewrite,
        "Alias of `week`",
        ["date_part"]
    ),
    shim!(
        "with_timezone",
        "with_timezone(timestamp, zone)",
        "Date and time",
        Unsupported,
        "Refused in v0.1 along with `AT TIME ZONE`.",
        []
    ),
    shim!(
        "year",
        "year(x) → bigint",
        "Date and time",
        Rewrite,
        "`CAST(date_part('year', x) AS BIGINT)`",
        ["date_part"]
    ),
    // --- Math --------------------------------------------------------------
    shim!(
        "abs",
        "abs(x)",
        "Math",
        Passthrough,
        "DataFusion `abs`",
        ["abs"]
    ),
    shim!(
        "cbrt",
        "cbrt(x) → double",
        "Math",
        Passthrough,
        "DataFusion `cbrt`",
        ["cbrt"]
    ),
    shim!(
        "ceil",
        "ceil(x)",
        "Math",
        Passthrough,
        "DataFusion `ceil`",
        ["ceil"]
    ),
    shim!(
        "ceiling",
        "ceiling(x)",
        "Math",
        Rewrite,
        "`ceil(x)`",
        ["ceil"]
    ),
    shim!(
        "exp",
        "exp(x) → double",
        "Math",
        Passthrough,
        "DataFusion `exp`",
        ["exp"]
    ),
    shim!(
        "floor",
        "floor(x)",
        "Math",
        Passthrough,
        "DataFusion `floor`",
        ["floor"]
    ),
    shim!(
        "greatest",
        "greatest(a, b, ...)",
        "Math",
        Passthrough,
        "DataFusion `greatest`",
        ["greatest"]
    ),
    shim!(
        "least",
        "least(a, b, ...)",
        "Math",
        Passthrough,
        "DataFusion `least`",
        ["least"]
    ),
    shim!(
        "ln",
        "ln(x) → double",
        "Math",
        Passthrough,
        "DataFusion `ln`",
        ["ln"]
    ),
    shim!(
        "log",
        "log(base, x) → double",
        "Math",
        Passthrough,
        "DataFusion `log(base, x)` (same argument order)",
        ["log"]
    ),
    shim!(
        "log10",
        "log10(x) → double",
        "Math",
        Passthrough,
        "DataFusion `log10`",
        ["log10"]
    ),
    shim!(
        "log2",
        "log2(x) → double",
        "Math",
        Passthrough,
        "DataFusion `log2`",
        ["log2"]
    ),
    shim!("mod", "mod(n, m)", "Math", Rewrite, "`n % m`", []),
    shim!(
        "pi",
        "pi() → double",
        "Math",
        Passthrough,
        "DataFusion `pi`",
        ["pi"]
    ),
    shim!(
        "pow",
        "pow(x, p) → double",
        "Math",
        Passthrough,
        "DataFusion `pow`",
        ["pow"]
    ),
    shim!(
        "power",
        "power(x, p) → double",
        "Math",
        Passthrough,
        "DataFusion `power`",
        ["power"]
    ),
    shim!(
        "rand",
        "rand() → double",
        "Math",
        Rewrite,
        "`random()`",
        ["random"]
    ),
    shim!(
        "random",
        "random() → double",
        "Math",
        Passthrough,
        "DataFusion `random`",
        ["random"]
    ),
    shim!(
        "round",
        "round(x[, d])",
        "Math",
        Passthrough,
        "DataFusion `round`",
        ["round"]
    ),
    shim!(
        "sign",
        "sign(x)",
        "Math",
        Rewrite,
        "`signum(x)` (always a double; Trino keeps the argument's type)",
        ["signum"]
    ),
    shim!(
        "sqrt",
        "sqrt(x) → double",
        "Math",
        Passthrough,
        "DataFusion `sqrt`",
        ["sqrt"]
    ),
    shim!(
        "truncate",
        "truncate(x) → double",
        "Math",
        Rewrite,
        "`trunc(x)`",
        ["trunc"]
    ),
    // --- Array -------------------------------------------------------------
    shim!(
        "all_match",
        "all_match(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "any_match",
        "any_match(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "array_distinct",
        "array_distinct(array) → array",
        "Array",
        Passthrough,
        "DataFusion `array_distinct`",
        ["array_distinct"]
    ),
    shim!(
        "array_join",
        "array_join(array, delimiter[, null_replacement]) → varchar",
        "Array",
        Rewrite,
        "`array_to_string(array, delimiter[, null_replacement])` (NULL elements are skipped unless a replacement is given, like Trino)",
        ["array_to_string"]
    ),
    shim!(
        "array_max",
        "array_max(array)",
        "Array",
        Passthrough,
        "DataFusion `array_max`",
        ["array_max"]
    ),
    shim!(
        "array_min",
        "array_min(array)",
        "Array",
        Passthrough,
        "DataFusion `array_min`",
        ["array_min"]
    ),
    shim!(
        "array_position",
        "array_position(array, element) → bigint",
        "Array",
        Rewrite,
        "`CASE WHEN array IS NULL THEN NULL ELSE coalesce(array_position(array, element), 0) END` — Trino returns 0 for a missing element where DataFusion returns NULL.",
        ["array_position", "coalesce"]
    ),
    shim!(
        "array_remove",
        "array_remove(array, element) → array",
        "Array",
        Rewrite,
        "`array_remove_all(array, element)` — Trino removes every occurrence, DataFusion's `array_remove` only the first.",
        ["array_remove_all"]
    ),
    shim!(
        "array_sort",
        "array_sort(array) → array",
        "Array",
        Passthrough,
        "DataFusion `array_sort` (ascending). The comparator-lambda form is refused.",
        ["array_sort"]
    ),
    shim!(
        "array_union",
        "array_union(a, b) → array",
        "Array",
        Passthrough,
        "DataFusion `array_union`",
        ["array_union"]
    ),
    shim!(
        "arrays_overlap",
        "arrays_overlap(a, b) → boolean",
        "Array",
        Passthrough,
        "DataFusion `arrays_overlap`",
        ["arrays_overlap"]
    ),
    shim!(
        "cardinality",
        "cardinality(array) → bigint",
        "Array",
        Rewrite,
        "`CAST(cardinality(array) AS BIGINT)` (DataFusion returns an unsigned integer)",
        ["cardinality"]
    ),
    shim!(
        "contains",
        "contains(array, element) → boolean",
        "Array",
        Rewrite,
        "`array_has(array, element)`",
        ["array_has"]
    ),
    shim!(
        "element_at",
        "element_at(array, index)",
        "Array",
        Rewrite,
        "`array_element(array, index)` (1-based, negative indexes count from the end, NULL when out of range). `element_at` on maps is refused by DataFusion's type check.",
        ["array_element"]
    ),
    shim!(
        "filter",
        "filter(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "flatten",
        "flatten(array(array)) → array",
        "Array",
        Passthrough,
        "DataFusion `flatten`",
        ["flatten"]
    ),
    shim!(
        "none_match",
        "none_match(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "reduce",
        "reduce(array, initial, lambda, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "repeat",
        "repeat(element, count) → array",
        "Array",
        Unsupported,
        "Refused: DataFusion's `repeat` repeats a *string* (`repeat('a', 3) = 'aaa'`) where Trino builds an array. Silently wrong if passed through.",
        []
    ),
    shim!(
        "sequence",
        "sequence(start, stop[, step]) → array",
        "Array",
        Unsupported,
        "Refused: DataFusion's `generate_series` / `range` differ in inclusivity and date handling.",
        []
    ),
    shim!(
        "slice",
        "slice(array, start, length) → array",
        "Array",
        Unsupported,
        "Refused: DataFusion's `array_slice` takes an end index, not a length, and handles negative starts differently.",
        []
    ),
    shim!(
        "transform",
        "transform(array, lambda)",
        "Array",
        Unsupported,
        "Refused: lambda expressions are not translated.",
        []
    ),
    shim!(
        "zip",
        "zip(a, b, ...) → array(row)",
        "Array",
        Unsupported,
        "Refused: ROW types are not supported in v0.1.",
        []
    ),
    // --- Row and map -------------------------------------------------------
    shim!(
        "map",
        "map(keys, values) → map",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "map_concat",
        "map_concat(a, b) → map",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "map_keys",
        "map_keys(map) → array",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "map_values",
        "map_values(map) → array",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    shim!(
        "row",
        "ROW(a, b, ...) → row",
        "Row and map",
        Unsupported,
        "Refused: ROW / MAP values are not supported in v0.1.",
        []
    ),
    // --- JSON --------------------------------------------------------------
    shim!(
        "json_array_contains",
        "json_array_contains(json, value) → boolean",
        "JSON",
        Unsupported,
        "Refused in v0.1. Use `json_extract_scalar` on known indexes.",
        []
    ),
    shim!(
        "json_array_get",
        "json_array_get(json, index) → json",
        "JSON",
        Unsupported,
        "Refused in v0.1. Use `json_extract(json, '$[index]')`.",
        []
    ),
    shim!(
        "json_array_length",
        "json_array_length(json) → bigint",
        "JSON",
        Udf,
        "Rust UDF (NULL when the value is not an array)",
        ["json_array_length"]
    ),
    shim!(
        "json_extract",
        "json_extract(json, json_path) → json",
        "JSON",
        Udf,
        "Rust UDF; the JSON type is represented as its text. JSONPath subset: `$`, `.key`, `[\"key\"]`, `[n]` — wildcards, recursive descent, slices and filters are refused.",
        ["json_extract"]
    ),
    shim!(
        "json_extract_scalar",
        "json_extract_scalar(json, json_path) → varchar",
        "JSON",
        Udf,
        "Rust UDF; same JSONPath subset. NULL for missing paths, JSON nulls, objects and arrays.",
        ["json_extract_scalar"]
    ),
    shim!(
        "json_format",
        "json_format(json) → varchar",
        "JSON",
        Udf,
        "Rust UDF (re-serialises compactly)",
        ["json_format"]
    ),
    shim!(
        "json_parse",
        "json_parse(varchar) → json",
        "JSON",
        Udf,
        "Rust UDF: validates the text (invalid JSON is an error, like Trino) and keeps it as text.",
        ["json_parse"]
    ),
    shim!(
        "json_size",
        "json_size(json, json_path) → bigint",
        "JSON",
        Udf,
        "Rust UDF: member count of the object/array at the path, 0 for scalars",
        ["json_size"]
    ),
    // --- Misc --------------------------------------------------------------
    shim!(
        "uuid",
        "uuid() → uuid",
        "Misc",
        Passthrough,
        "DataFusion `uuid` (returned as varchar)",
        ["uuid"]
    ),
];

/// The SQL construct coverage table.
pub static CONSTRUCTS: &[Construct] = &[
    Construct {
        name: "SELECT / DISTINCT / WHERE",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Full projection, `SELECT DISTINCT`, arbitrary predicates.",
        corpus_marker: "SELECT DISTINCT",
    },
    Construct {
        name: "JOIN (INNER, LEFT, RIGHT, FULL, CROSS)",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`ON` and `USING` forms.",
        corpus_marker: "FULL OUTER JOIN",
    },
    Construct {
        name: "Common table expressions (WITH)",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Including multiple and chained CTEs.",
        corpus_marker: "WITH ",
    },
    Construct {
        name: "Subqueries (derived tables, scalar, IN, EXISTS)",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Correlated `EXISTS` / `IN` are decorrelated by DataFusion.",
        corpus_marker: "EXISTS (",
    },
    Construct {
        name: "Window functions",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)` and named windows.",
        corpus_marker: "OVER (",
    },
    Construct {
        name: "GROUP BY / HAVING / ROLLUP / CUBE / GROUPING SETS",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "",
        corpus_marker: "ROLLUP",
    },
    Construct {
        name: "ORDER BY / LIMIT / OFFSET",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`NULLS FIRST/LAST` honoured. Trino's default (NULLS LAST for ASC) is applied when unspecified.",
        corpus_marker: "OFFSET",
    },
    Construct {
        name: "UNION / UNION ALL / INTERSECT / EXCEPT",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "",
        corpus_marker: "UNION ALL",
    },
    Construct {
        name: "VALUES",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Inline tables, also as a `FROM` source with column aliases.",
        corpus_marker: "VALUES",
    },
    Construct {
        name: "CASE",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "Simple and searched forms.",
        corpus_marker: "CASE WHEN",
    },
    Construct {
        name: "CAST",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "Trino type names (`VARCHAR`, `BIGINT`, `INTEGER`, `DOUBLE`, `REAL`, `DECIMAL(p,s)`, `BOOLEAN`, `DATE`, `TIMESTAMP`, `VARBINARY`) map to Arrow types. `JSON`, `ROW`, `MAP` targets are refused by the planner.",
        corpus_marker: "CAST(",
    },
    Construct {
        name: "TRY_CAST",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "NULL on conversion failure.",
        corpus_marker: "TRY_CAST(",
    },
    Construct {
        name: "INTERVAL literals",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "`INTERVAL '1' DAY`, `INTERVAL '2' HOUR`, and `timestamp ± interval` arithmetic.",
        corpus_marker: "INTERVAL '",
    },
    Construct {
        name: "String concatenation (||)",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "NULL-propagating, like Trino.",
        corpus_marker: "||",
    },
    Construct {
        name: "BETWEEN / IN (list) / LIKE / IS [NOT] NULL / IS DISTINCT FROM",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "",
        corpus_marker: "IS DISTINCT FROM",
    },
    Construct {
        name: "ARRAY[...] literals and 1-based subscripts",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "`arr[1]` is the first element, like Trino; out-of-range subscripts raise an error in Trino but return NULL here.",
        corpus_marker: "ARRAY[",
    },
    Construct {
        name: "EXTRACT(field FROM x) / POSITION / SUBSTRING / TRIM syntax",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "",
        corpus_marker: "EXTRACT(",
    },
    Construct {
        name: "Identifiers",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Unquoted identifiers are lower-cased; double-quoted identifiers keep their case (Trino rules). Glue catalogs are lower-case, so quoted mixed-case column names fail to resolve as they do on Athena.",
        corpus_marker: "\"",
    },
    Construct {
        name: "Read-only statements",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "`SELECT`, `WITH`, `VALUES`, `EXPLAIN` (DataFusion's plan text, not Trino's).",
        corpus_marker: "SELECT",
    },
    Construct {
        name: "lambda expression",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`x -> ...` arguments (`transform`, `filter`, `reduce`, comparator sorts) are refused by name.",
        corpus_marker: "",
    },
    Construct {
        name: "AT TIME ZONE",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Refused in v0.1; timestamps are handled as UTC instants.",
        corpus_marker: "",
    },
    Construct {
        name: "ROW / MAP literals and types",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`ROW(...)`, `MAP(...)`, `CAST(... AS ROW(...))` are refused.",
        corpus_marker: "",
    },
    Construct {
        name: "UNNEST",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`CROSS JOIN UNNEST(...)` is refused in v0.1 (Trino's `WITH ORDINALITY` and multi-array forms have no direct DataFusion mapping).",
        corpus_marker: "",
    },
    Construct {
        name: "Multiple statements",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "One statement per query execution, like Athena.",
        corpus_marker: "",
    },
    Construct {
        name: "DDL / DML / CTAS / INSERT / UNLOAD",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Writes arrive in v0.2; refused by statement kind.",
        corpus_marker: "",
    },
];

/// Look up a Trino function by (lower-case) name.
pub fn lookup(name: &str) -> Option<&'static FunctionShim> {
    FUNCTIONS.iter().find(|s| s.name == name)
}

/// Every DataFusion function name the translations emit or pass through.
pub fn datafusion_targets() -> &'static HashSet<&'static str> {
    static TARGETS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
        FUNCTIONS
            .iter()
            .flat_map(|s| s.df_functions.iter().copied())
            .collect()
    });
    &TARGETS
}

const DOC_HEADER: &str = "<!-- GENERATED by glaux-athena's shim registry (crates/glaux-athena/src/dialect/registry.rs).\n     Do not edit by hand: run `GLAUX_REGEN_DOCS=1 cargo test -p glaux-athena coverage_doc`. -->\n";

/// Render `docs/sql-coverage.md` from the registry.
pub fn render_coverage() -> String {
    let mut out = String::new();
    out.push_str(DOC_HEADER);
    out.push_str("# glaux Athena SQL coverage\n\n");
    out.push_str(
        "glaux executes Athena (Trino-dialect) SQL by translating it onto Apache DataFusion. \
         This table is generated from the shim registry that drives the translator, so it is \
         exactly what the engine accepts: anything not listed is refused with an error naming \
         the construct — never silently approximated.\n\n",
    );
    let (supported, refused): (Vec<&FunctionShim>, Vec<&FunctionShim>) = FUNCTIONS
        .iter()
        .partition(|s| s.kind != ShimKind::Unsupported);
    let _ = writeln!(
        out,
        "**Functions:** {} supported ({} passthrough, {} rewritten, {} Rust UDFs), {} refused by name.\n",
        supported.len(),
        supported
            .iter()
            .filter(|s| s.kind == ShimKind::Passthrough)
            .count(),
        supported
            .iter()
            .filter(|s| s.kind == ShimKind::Rewrite)
            .count(),
        supported.iter().filter(|s| s.kind == ShimKind::Udf).count(),
        refused.len(),
    );

    out.push_str("## SQL constructs\n\n");
    out.push_str("| Construct | Category | Status | Notes |\n|---|---|---|---|\n");
    for c in CONSTRUCTS {
        let status = match c.status {
            ConstructStatus::Supported => "supported",
            ConstructStatus::Unsupported => "refused",
        };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} |",
            escape(c.name),
            c.category,
            status,
            escape(c.notes)
        );
    }

    out.push_str("\n## Functions\n");
    let mut categories: Vec<&str> = Vec::new();
    for s in FUNCTIONS {
        if !categories.contains(&s.category) {
            categories.push(s.category);
        }
    }
    for category in categories {
        let _ = write!(out, "\n### {category}\n\n");
        out.push_str(
            "| Function | Trino signature | Handling | Translation |\n|---|---|---|---|\n",
        );
        for s in FUNCTIONS.iter().filter(|s| s.category == category) {
            let _ = writeln!(
                out,
                "| `{}` | `{}` | {} | {} |",
                s.name,
                s.signature,
                s.kind.label(),
                escape(s.translation)
            );
        }
    }
    out.push_str(
        "\n## Legend\n\n\
         - **passthrough** — same name and semantics in DataFusion; the call is handed over unchanged.\n\
         - **rewrite** — rewritten at the AST level onto DataFusion functions or syntax.\n\
         - **udf** — implemented in Rust inside glaux and registered under the Trino name.\n\
         - **unsupported** — recognised and refused with an explicit error naming the function.\n\n\
         Functions not in this table are refused with `FUNCTION_NOT_FOUND`, even when DataFusion \
         has a function of the same name, because same-named functions with different semantics \
         are the main way an emulator becomes silently wrong.\n",
    );
    out
}

fn escape(s: &str) -> String {
    s.replace('|', "\\|")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_names_are_unique_lowercase_and_sorted_within_category() {
        let mut seen = HashSet::new();
        for s in FUNCTIONS {
            assert_eq!(
                s.name,
                s.name.to_lowercase(),
                "{} must be lower-case",
                s.name
            );
            assert!(seen.insert(s.name), "duplicate registry entry {}", s.name);
            match s.kind {
                ShimKind::Unsupported => assert!(
                    s.df_functions.is_empty(),
                    "{} is unsupported but lists targets",
                    s.name
                ),
                ShimKind::Passthrough | ShimKind::Udf => assert!(
                    !s.df_functions.is_empty(),
                    "{} must name the DataFusion function it maps to",
                    s.name
                ),
                ShimKind::Rewrite => {}
            }
        }
        let mut by_category: Vec<(&str, Vec<&str>)> = Vec::new();
        for s in FUNCTIONS {
            match by_category.last_mut() {
                Some((c, names)) if *c == s.category => names.push(s.name),
                _ => by_category.push((s.category, vec![s.name])),
            }
        }
        for (category, names) in by_category {
            let mut sorted = names.clone();
            sorted.sort_unstable();
            assert_eq!(names, sorted, "category {category} is not sorted by name");
        }
        assert_eq!(lookup("date_add").map(|s| s.kind), Some(ShimKind::Udf));
        assert!(lookup("frobnicate").is_none());
    }

    #[test]
    fn coverage_doc_lists_every_entry() {
        let doc = render_coverage();
        for s in FUNCTIONS {
            assert!(
                doc.contains(&format!("| `{}` |", s.name)),
                "{} missing",
                s.name
            );
        }
        for c in CONSTRUCTS {
            assert!(doc.contains(&escape(c.name)), "{} missing", c.name);
        }
        assert!(doc.starts_with(DOC_HEADER));
    }
}
