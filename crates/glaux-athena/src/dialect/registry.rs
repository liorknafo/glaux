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
        "`CAST(approx_distinct(x) AS BIGINT)` (HyperLogLog; estimates differ from Trino's within the usual error bound)",
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
        "DataFusion `sum`; for integer inputs glaux substitutes an overflow-checked sum so a bigint overflow is an error (`NUMERIC_VALUE_OUT_OF_RANGE`), as in Trino.",
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
        "`CAST(dense_rank(...) AS BIGINT)` — DataFusion's result is unsigned",
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
        "`CAST(ntile(...) AS BIGINT)` — DataFusion's result is unsigned",
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
        "`CAST(rank(...) AS BIGINT)` — DataFusion's result is unsigned",
        ["rank"]
    ),
    shim!(
        "row_number",
        "row_number() OVER (...)",
        "Window",
        Passthrough,
        "`CAST(row_number(...) AS BIGINT)` — DataFusion's result is unsigned",
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
        "`CAST(length(x) AS BIGINT)` (characters, not bytes)",
        ["length"]
    ),
    shim!(
        "levenshtein_distance",
        "levenshtein_distance(a, b) → bigint",
        "String",
        Rewrite,
        "`CAST(levenshtein(a, b) AS BIGINT)`",
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
        "reverse(varchar) → varchar, reverse(array) → array",
        "String",
        Rewrite,
        "Rust UDF `trino_reverse`: reverses a string's code points or an array's elements (DataFusion's `reverse` is string-only and would stringify an array)",
        ["trino_reverse"]
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
        "Rust UDF `trino_split`: `split('', ',')` is `['']` and an empty delimiter splits into characters, as in Trino. The 3-argument `split(x, delimiter, limit)` form is refused.",
        ["trino_split"]
    ),
    shim!(
        "split_part",
        "split_part(varchar, delimiter, index) → varchar",
        "String",
        Rewrite,
        "Rust UDF `trino_split_part`: 1-based; NULL past the last field (DataFusion returns `''`); `index < 1` is an error; an empty delimiter splits into characters.",
        ["trino_split_part"]
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
        "`CAST(strpos(x, sub) AS BIGINT)` (1-based, 0 when absent). The 3-argument `strpos(x, sub, instance)` form is refused.",
        ["strpos"]
    ),
    shim!(
        "substr",
        "substr(varchar, start[, length])",
        "String",
        Rewrite,
        "Rust UDF `trino_substr` with Trino's rules: 1-based; negative `start` counts from the end; `start = 0`, a non-positive `length`, or a start past either end gives `''` (DataFusion's `substr` follows PostgreSQL, where `substr('hello', -3)` is `'hello'`).",
        ["trino_substr"]
    ),
    shim!(
        "substring",
        "substring(varchar, start[, length])",
        "String",
        Rewrite,
        "Same as `substr`, for both the call and the `SUBSTRING(x FROM s FOR n)` syntax.",
        ["trino_substr"]
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
        "`regexp_replace(x, pattern, trino_regexp_replacement(replacement), 'g')` — Trino replaces every match (DataFusion only the first unless flagged), and the replacement is translated from Java syntax (`$1x` is group 1 then `x`; `\\$` a literal dollar) to Rust's. Patterns use Rust `regex` syntax, which lacks look-around and back-references (those fail loudly). The lambda form is refused.",
        ["regexp_replace", "trino_regexp_replacement"]
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
        "Rust UDF: calendar arithmetic for month/quarter/year (clamps to month end), fixed lengths otherwise. Units: millisecond … year; adding sub-day units to a DATE is an error, and so is a fractional `value` (Trino requires bigint).",
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
        "`to_timestamp(x, <strftime>)` with the MySQL-style format translated; the format must be a literal. `%f` accepts 1-9 fractional digits when parsing, like Trino, but only directly after a `.`.",
        ["to_timestamp"]
    ),
    shim!(
        "date_trunc",
        "date_trunc(unit, x) → same as x",
        "Date and time",
        Rewrite,
        "Rust UDF `trino_date_trunc`: a `date` input stays a `date` (DataFusion's `date_trunc` returns a timestamp), sub-day units on a `date` are refused, and varchar input is refused (`TYPE_MISMATCH`, as in Trino). Weeks start on Monday.",
        ["trino_date_trunc"]
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
        "`to_char(x, <strftime>)` with the Joda pattern translated; the pattern must be a literal and unknown pattern letters are refused. `Z` / `ZZ` / `ZZZ` print `+0000` / `+00:00` / `UTC` (timestamps are UTC instants).",
        ["to_char"]
    ),
    shim!(
        "from_iso8601_date",
        "from_iso8601_date(varchar) → date",
        "Date and time",
        Udf,
        "Rust UDF: strict ISO-8601 calendar date (`YYYY-MM-DD`, also `YYYY-MM` / `YYYY`); anything else (`2024-1-1`, ordinal or week dates) is an error.",
        ["from_iso8601_date"]
    ),
    shim!(
        "from_iso8601_timestamp",
        "from_iso8601_timestamp(varchar) → timestamp with time zone",
        "Date and time",
        Udf,
        "Rust UDF: strict ISO-8601 (`YYYY-MM-DD[THH[:mm[:ss[.fff]]]][Z|±HH:mm]`); a space separator or single-digit fields are errors, as in Trino. The instant is returned as a UTC `timestamp(3)` (the input's offset is applied, not preserved).",
        ["from_iso8601_timestamp"]
    ),
    shim!(
        "from_unixtime",
        "from_unixtime(double) → timestamp",
        "Date and time",
        Rewrite,
        "`arrow_cast(CAST(round(x * 1000) AS BIGINT), 'Timestamp(Millisecond, None)')` (rounded to the millisecond, like Athena: `from_unixtime(1.9999)` is `…:02.000`). The zone-argument forms are refused.",
        ["arrow_cast", "round"]
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
        "`to_timestamp(x, <strftime>)` with the Joda pattern translated; the pattern must be a literal. `SSS` / `SSSSSS` parse exactly that many fractional digits (Joda accepts fewer).",
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
        "Rust UDF `trino_to_unixtime` (microsecond resolution). Varchar input is refused (`TYPE_MISMATCH`, as in Trino).",
        ["trino_to_unixtime"]
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
        "DataFusion `ceil` (on DECIMAL inputs the result keeps the input's scale — `2.0` where Trino gives `2`)",
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
        "DataFusion `floor` (on DECIMAL inputs the result keeps the input's scale — `-2.0` where Trino gives `-2`)",
        ["floor"]
    ),
    shim!(
        "greatest",
        "greatest(a, b, ...)",
        "Math",
        Rewrite,
        "`CASE WHEN a IS NULL OR b IS NULL ... THEN NULL ELSE greatest(a, b, ...) END`: NULL if any argument is NULL, as in Trino (DataFusion skips NULLs). Arguments must share a type.",
        ["greatest"]
    ),
    shim!(
        "least",
        "least(a, b, ...)",
        "Math",
        Rewrite,
        "`CASE WHEN a IS NULL OR b IS NULL ... THEN NULL ELSE least(a, b, ...) END`: NULL if any argument is NULL, as in Trino (DataFusion skips NULLs). Arguments must share a type.",
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
        "DataFusion `round` (half away from zero, like Trino). On DECIMAL inputs the result stays a decimal, so `round(2.5)` renders `3` and `round(2.789, 2)` renders `2.79`, matching Trino.",
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
        "truncate(x[, n])",
        "Math",
        Rewrite,
        "`trunc(x[, n])`. On DECIMAL inputs the result keeps the input's scale, so the value is right but the text has extra zeros (`truncate(2.789, 2)` renders `2.780`, and `truncate(2.7)` renders `2.0` where Trino gives `2`); on DOUBLE inputs the text matches Trino.",
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
        "`CASE WHEN array IS NULL THEN NULL ELSE coalesce(CAST(array_position(array, element) AS BIGINT), 0) END` — Trino returns 0 for a missing element where DataFusion returns NULL.",
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
        Rewrite,
        "`array_sort(x, 'ASC', 'NULLS LAST')`: ascending with NULL elements last, as in Trino (DataFusion's default puts them first). The comparator-lambda form is refused.",
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
        Rewrite,
        "Rust UDF `trino_arrays_overlap`: NULL (not false) when no element matches but either array has a NULL element, as in Trino. Element types must be comparable.",
        ["trino_arrays_overlap"]
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
        "Rust UDF `trino_contains`: NULL (not false) when the element is not found but the array has a NULL element, or when the element is NULL, as in Trino. The element type must be comparable with the array's.",
        ["trino_contains"]
    ),
    shim!(
        "element_at",
        "element_at(array, index)",
        "Array",
        Rewrite,
        "Rust UDF `trino_element_at`: 1-based, negative indexes count from the end, NULL past either end, `index = 0` is an error (`SQL array indices start at 1`). `element_at` on maps is refused.",
        ["trino_element_at"]
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
        notes: "Full projection, `SELECT DISTINCT`, arbitrary predicates. Anonymous output columns are named `_col0`, `_col1`, … by position (also inside derived tables and CTEs) and duplicate output names are allowed, as on Athena. `DISTINCT ON`, `QUALIFY`, `GROUP BY ALL`, `TOP`, `SELECT INTO`, `TABLESAMPLE`, `FOR UPDATE`, and `SELECT * EXCLUDE` are refused as non-Trino syntax.",
        corpus_marker: "SELECT DISTINCT",
    },
    Construct {
        name: "JOIN (INNER, LEFT, RIGHT, FULL, CROSS)",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "`ON` and `USING` forms; join keys must have comparable types (`TYPE_MISMATCH` otherwise). `NATURAL`, `SEMI` / `ANTI`, `APPLY`, and `ASOF` joins are refused as non-Trino syntax.",
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
        notes: "`OVER (PARTITION BY ... ORDER BY ... ROWS/RANGE ...)` and named windows. Ranking functions return `bigint` (cast from DataFusion's unsigned result). Window `ORDER BY` sorts NULLs last by default, as in Trino.",
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
        notes: "`NULLS FIRST/LAST` honoured. Trino's default — NULLs last whatever the direction, also for window `ORDER BY` and aggregate `ORDER BY` arguments — is applied when unspecified (DataFusion's own default would sort NULLs first under `DESC`). `ORDER BY ALL` is refused.",
        corpus_marker: "OFFSET",
    },
    Construct {
        name: "UNION / UNION ALL / INTERSECT / EXCEPT",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Corresponding columns must have comparable types (`SELECT 1 UNION SELECT 'a'` is a `TYPE_MISMATCH`, as on Athena).",
        corpus_marker: "UNION ALL",
    },
    Construct {
        name: "VALUES",
        category: "Query shape",
        status: ConstructStatus::Supported,
        notes: "Inline tables, also as a `FROM` source with column aliases; anonymous columns are `_col0`, `_col1`, … as on Athena.",
        corpus_marker: "VALUES",
    },
    Construct {
        name: "CASE",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "Simple and searched forms. The operand must be comparable with the `WHEN` values and all results must share a type (`TYPE_MISMATCH` otherwise, as on Athena); the same applies to `if`, `nullif`, and `coalesce`.",
        corpus_marker: "CASE WHEN",
    },
    Construct {
        name: "CAST",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "Trino type names (`VARCHAR[(n)]`, `BIGINT`, `INTEGER`, `SMALLINT`, `TINYINT`, `DOUBLE`, `REAL`, `DECIMAL(p,s)`, `BOOLEAN`, `DATE`, `TIMESTAMP`) map to Arrow types. The `x::type` form is refused as non-Trino syntax. Double/decimal → integer rounds half away from zero (`CAST(2.5 AS BIGINT)` is 3) and fails on overflow (`INVALID_CAST_ARGUMENT`; NULL under `TRY_CAST`). `CAST(... AS VARCHAR)` uses Trino's text forms (`2024-01-05 10:30:00.000`, `1.0E20`) and `VARCHAR(n)` truncates to `n` characters. `VARBINARY`, `JSON`, `ROW`, `MAP` targets are refused by name.",
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
        notes: "`INTERVAL '1' DAY`, `INTERVAL '2' HOUR`, and `timestamp ± interval` arithmetic. `date - date` and `timestamp - timestamp` (an `interval` result in Trino) are refused; use `date_diff`.",
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
        notes: "`arr[1]` is the first element; `arr[0]`, negative, and out-of-range subscripts are errors, as in Trino (use `element_at` for NULL instead). The bare `[1, 2]` form is refused as non-Trino syntax.",
        corpus_marker: "ARRAY[",
    },
    Construct {
        name: "EXTRACT(field FROM x) / POSITION / SUBSTRING / TRIM syntax",
        category: "Expressions",
        status: ConstructStatus::Supported,
        notes: "`EXTRACT` fields: YEAR, QUARTER, MONTH, WEEK, DAY, DAY_OF_MONTH, DAY_OF_WEEK/DOW (1 = Monday … 7 = Sunday, Trino numbering), DAY_OF_YEAR/DOY, HOUR, MINUTE, SECOND; other fields are refused by name. `SUBSTRING` follows `substr`'s rules; `POSITION` returns bigint.",
        corpus_marker: "EXTRACT(",
    },
    Construct {
        name: "Identifiers",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Identifiers are case-insensitive whether quoted or not, as in Trino: `\"Name\"` and `name` resolve to the same column. Output aliases are reported in the case they were written (`AS \"Total\"` is column `Total`) and resolve case-insensitively (`ORDER BY total`, `ORDER BY \"TOTAL\"`), also from outer queries.",
        corpus_marker: "\"",
    },
    Construct {
        name: "Numeric literals",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "`1.5` is `DECIMAL(2,1)` and `1e2` is `DOUBLE`, as in Trino, so `0.1 + 0.2` is exactly `0.3`. Decimal arithmetic follows DataFusion's result precision/scale rules (division and `avg` keep more fractional digits than Trino: `1.5 / 2` is `0.75000` here, `0.8` on Athena) and math functions compute decimal arguments in double precision.",
        corpus_marker: "0.5",
    },
    Construct {
        name: "Integer overflow",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "`bigint` `+`, `-`, `*`, and `sum` fail with `NUMERIC_VALUE_OUT_OF_RANGE` on overflow, as in Trino (DataFusion alone wraps around). Division by zero is `DIVISION_BY_ZERO`.",
        corpus_marker: "9223372036854775807",
    },
    Construct {
        name: "Operator type checking",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Comparisons, arithmetic, `||`, `IN` (lists and subqueries), `BETWEEN`, join keys, simple `CASE` operands, `CASE` / `if` / `nullif` / `coalesce` / `greatest` / `least` results, and set-operation columns between types Trino does not combine (`varchar = integer`, `'a' || 1`, `date = varchar`) are refused with `TYPE_MISMATCH` instead of being coerced, and the date-part functions (`year`, `date_trunc`, `date_format`, `to_unixtime`, `EXTRACT`) refuse varchar arguments. Numeric types compare with each other and `date` with `timestamp`, as in Trino.",
        corpus_marker: "'1' = 1",
    },
    Construct {
        name: "Runtime errors",
        category: "Semantics",
        status: ConstructStatus::Supported,
        notes: "Failures caused by the query's data (an invalid cast, an unparsable date, a bad subscript) are user errors (Athena `ErrorCategory` 2) with Trino's error code; only I/O and engine failures are category 1.",
        corpus_marker: "CAST('abc' AS INTEGER)",
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
        name: "timestamp with time zone literal",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`TIMESTAMP '2024-01-05 10:00:00 America/New_York'` (and `Z` / offset suffixes) are refused rather than silently converted to a zone-less UTC instant.",
        corpus_marker: "",
    },
    Construct {
        name: "date subtraction",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "`date - date` and `timestamp - timestamp` produce an `interval` in Trino, which glaux cannot return in v0.1; use `date_diff(unit, a, b)`.",
        corpus_marker: "",
    },
    Construct {
        name: "Non-Trino syntax",
        category: "Unsupported",
        status: ConstructStatus::Unsupported,
        notes: "Syntax DataFusion accepts but Trino does not is refused by name instead of running with DataFusion semantics: `DISTINCT ON`, `QUALIFY`, `GROUP BY ALL`, `ORDER BY ALL`, `TABLESAMPLE`, `FOR UPDATE`, `NATURAL` / `SEMI` / `ANTI` / `APPLY` / `ASOF` joins, `[1, 2]` array literals, `x::type` casts, `TOP`, `SELECT INTO`, `SELECT * EXCLUDE`, PostgreSQL operators.",
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
