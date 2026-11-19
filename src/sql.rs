//! Validate the deliberately narrow SQL surface before any warehouse request.
//!
//! Parsing uses the Redshift dialect, then inspects every nested query, statement,
//! expression and table factor. The allowlist is an application guard, not a
//! substitute for a database principal with SELECT-only grants and restricted
//! function execution. It cannot inspect SQL hidden inside views or resolve
//! overloaded function names against a live catalogue.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sha2::{Digest, Sha256};
use sqlparser::ast::{
    BinaryOperator, DescribeAlias, Expr, FunctionArguments, InTarget, ObjectName, ObjectNamePart,
    Query, Select, SetExpr, Statement, TableFactor, Value, ValueWithSpan, Visit, Visitor,
};
use sqlparser::dialect::RedshiftSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use crate::backend::SqlParameter;
use crate::error::{AppError, AppResult};

pub const MAX_SQL_BYTES: usize = 100_000;
pub const MAX_SQL_TOKENS: usize = 1_024;
const MAX_RECURSION: usize = 64;
const MAX_AST_NODES: usize = 10_000;
const MAX_PARAMETERS: usize = 100;
const MAX_PARAMETER_NAME_BYTES: usize = 128;
const MAX_PARAMETER_VALUE_BYTES: usize = 16_384;
const MAX_PARAMETER_TOTAL_BYTES: usize = 100_000;

/// Holds SQL that passed the local structural and binding checks.
///
/// The fingerprint identifies the trimmed SQL bytes, excluding parameter values.
/// It must not be used alone to deduplicate executions with different bindings.
#[derive(Clone, Debug)]
pub struct ValidatedSql {
    pub sql: String,
    pub fingerprint: String,
    pub parameter_count: usize,
}

/// Validates one SELECT/WITH query or plain nonexecuting EXPLAIN of a query.
///
/// # Errors
/// Rejects SQL exceeding 100000 bytes or 1024 non-whitespace tokens, malformed,
/// unsupported or potentially mutating SQL, unknown function names, and invalid
/// or mismatched named parameters. Errors never echo SQL, names, parameter values
/// or parser diagnostics.
///
/// # Security
/// Only this validator's named built-in function allowlist is admitted. Database
/// permissions remain responsible for data access and function resolution.
pub fn validate(sql: &str, parameters: &[SqlParameter]) -> AppResult<ValidatedSql> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(AppError::new(
            "sql_too_large",
            "SQL exceeds the 100000-byte limit.",
        ));
    }
    let sql = sql.trim();
    if sql.is_empty() || sql.contains('\0') {
        return Err(AppError::new(
            "invalid_sql",
            "Provide one nonempty SQL query without NUL bytes.",
        ));
    }
    let supplied = validate_parameters(parameters)?;
    let dialect = RedshiftSqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql)
        .tokenize_with_location()
        .map_err(|_| invalid_sql())?;
    // A parser recursion limit alone cannot bound left-associated ASTs built by
    // a loop. Admit a conservative token budget before constructing any AST so
    // parsing, visiting and dropping rejected trees all have bounded input size.
    // Comments and whitespace do not add syntax; quoted strings are one token.
    if tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .take(MAX_SQL_TOKENS + 1)
        .count()
        > MAX_SQL_TOKENS
    {
        return Err(AppError::new(
            "sql_complexity_limit",
            "SQL exceeds the 1024-token syntax limit. Simplify the query or use a reviewed database view.",
        ));
    }
    let statements = Parser::new(&dialect)
        .with_recursion_limit(MAX_RECURSION)
        .with_tokens_with_locations(tokens)
        .parse_statements()
        .map_err(|_| invalid_sql())?;
    if statements.len() != 1 {
        return Err(AppError::new(
            "single_statement_required",
            "Provide exactly one SELECT/WITH query or plain EXPLAIN query.",
        ));
    }
    let mut guard = ReadGuard::default();
    if let ControlFlow::Break(reason) = statements.visit(&mut guard) {
        return Err(reason.error());
    }
    if supplied.len() != guard.parameters.len()
        || !guard
            .parameters
            .iter()
            .all(|name| supplied.contains(name.as_str()))
    {
        return Err(AppError::new(
            "parameter_mismatch",
            "Bind every named SQL parameter exactly once and omit unused bindings.",
        ));
    }
    Ok(ValidatedSql {
        sql: sql.to_string(),
        fingerprint: format!("{:x}", Sha256::digest(sql.as_bytes())),
        parameter_count: supplied.len(),
    })
}

fn invalid_sql() -> AppError {
    AppError::new(
        "invalid_sql",
        "SQL could not be parsed within the supported Redshift grammar and nesting limit.",
    )
}

fn valid_parameter_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_PARAMETER_NAME_BYTES
        && name
            .bytes()
            .next()
            .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn validate_parameters(parameters: &[SqlParameter]) -> AppResult<BTreeSet<&str>> {
    if parameters.len() > MAX_PARAMETERS {
        return Err(AppError::new(
            "invalid_parameters",
            "At most 100 named parameters are supported.",
        ));
    }
    let mut names = BTreeSet::new();
    let mut total_bytes = 0usize;
    for parameter in parameters {
        if !valid_parameter_name(&parameter.name) || !names.insert(parameter.name.as_str()) {
            return Err(AppError::new(
                "invalid_parameters",
                "Parameter names must be unique ASCII identifiers of at most 128 bytes.",
            ));
        }
        if parameter.value.is_empty()
            || parameter.value.len() > MAX_PARAMETER_VALUE_BYTES
            || parameter.value.contains('\0')
        {
            return Err(AppError::new("invalid_parameters", "Parameter values must be nonempty strings of at most 16384 bytes without NUL bytes."));
        }
        total_bytes += parameter.name.len() + parameter.value.len();
        if total_bytes > MAX_PARAMETER_TOTAL_BYTES {
            return Err(AppError::new(
                "invalid_parameters",
                "Combined parameter names and values exceed the 100000-byte limit.",
            ));
        }
    }
    Ok(names)
}

#[derive(Clone, Copy)]
enum Rejection {
    Statement,
    QueryShape,
    SelectInto,
    Function,
    TableFunction,
    Placeholder,
    Complexity,
}

impl Rejection {
    fn error(self) -> AppError {
        let (code, message) = match self {
            Self::Statement => ("read_only_sql_required", "Only SELECT/WITH and plain nonexecuting EXPLAIN queries are supported."),
            Self::QueryShape => ("unsupported_query", "This query contains a clause outside the supported read-only SQL surface."),
            Self::SelectInto => ("read_only_sql_required", "SELECT INTO is not supported by the read-only query tools."),
            Self::Function => ("function_not_allowed", "Use only unqualified built-in functions from the documented allowlist; custom functions and operators are not supported."),
            Self::TableFunction => ("table_function_not_allowed", "Table functions and dynamic relation names are not supported."),
            Self::Placeholder => ("invalid_parameters", "Use named :parameter placeholders with ASCII identifier names; positional placeholders are not supported."),
            Self::Complexity => ("sql_complexity_limit", "The query exceeds the supported syntax depth or node limit."),
        };
        AppError::new(code, message)
    }
}

#[derive(Default)]
struct ReadGuard {
    parameters: BTreeSet<String>,
    depth: usize,
    nodes: usize,
}

impl ReadGuard {
    fn enter(&mut self) -> ControlFlow<Rejection> {
        self.depth += 1;
        self.nodes += 1;
        if self.depth > MAX_RECURSION || self.nodes > MAX_AST_NODES {
            ControlFlow::Break(Rejection::Complexity)
        } else {
            ControlFlow::Continue(())
        }
    }

    fn leave(&mut self) -> ControlFlow<Rejection> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }
}

impl Visitor for ReadGuard {
    type Break = Rejection;

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        self.enter()?;
        match statement {
            Statement::Query(_) => ControlFlow::Continue(()),
            Statement::Explain {
                describe_alias: DescribeAlias::Explain,
                analyze: false,
                verbose: false,
                query_plan: false,
                estimate: false,
                statement,
                format: None,
                options: None,
            } if matches!(statement.as_ref(), Statement::Query(_)) => ControlFlow::Continue(()),
            _ => ControlFlow::Break(Rejection::Statement),
        }
    }

    fn post_visit_statement(&mut self, _: &Statement) -> ControlFlow<Self::Break> {
        self.leave()
    }

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.enter()?;
        if !query.locks.is_empty()
            || query.for_clause.is_some()
            || query.settings.is_some()
            || query.format_clause.is_some()
            || !query.pipe_operators.is_empty()
        {
            return ControlFlow::Break(Rejection::QueryShape);
        }
        // Set-operation nodes have no visitor hook. Check their shape iteratively,
        // while the normal visitor checks every contained SELECT and subquery.
        let mut bodies = vec![(query.body.as_ref(), 0usize)];
        while let Some((body, depth)) = bodies.pop() {
            if depth > MAX_RECURSION {
                return ControlFlow::Break(Rejection::Complexity);
            }
            match body {
                SetExpr::Select(_) | SetExpr::Query(_) => {}
                SetExpr::SetOperation { left, right, .. } => {
                    bodies.push((left.as_ref(), depth + 1));
                    bodies.push((right.as_ref(), depth + 1));
                }
                _ => return ControlFlow::Break(Rejection::Statement),
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<Self::Break> {
        self.leave()
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        self.enter()?;
        if select.into.is_some() {
            return ControlFlow::Break(Rejection::SelectInto);
        }
        if !select.lateral_views.is_empty()
            || select.select_modifiers.is_some()
            || !select.optimizer_hints.is_empty()
        {
            return ControlFlow::Break(Rejection::QueryShape);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _: &Select) -> ControlFlow<Self::Break> {
        self.leave()
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if relation
            .0
            .iter()
            .all(|part| matches!(part, ObjectNamePart::Identifier(_)))
        {
            ControlFlow::Continue(())
        } else {
            ControlFlow::Break(Rejection::TableFunction)
        }
    }

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
        self.enter()?;
        match table {
            TableFactor::Table {
                args: None,
                with_hints,
                version: None,
                with_ordinality: false,
                index_directives,
                ..
            } if with_hints.is_empty() && index_directives.is_empty() => ControlFlow::Continue(()),
            TableFactor::Derived { .. }
            | TableFactor::NestedJoin { .. }
            | TableFactor::Pivot { .. }
            | TableFactor::Unpivot { .. } => ControlFlow::Continue(()),
            _ => ControlFlow::Break(Rejection::TableFunction),
        }
    }

    fn post_visit_table_factor(&mut self, _: &TableFactor) -> ControlFlow<Self::Break> {
        self.leave()
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        self.enter()?;
        match expression {
            Expr::Function(function) => {
                if function.uses_odbc_syntax
                    || !matches!(function.parameters, FunctionArguments::None)
                    || !allowed_function(&function.name)
                {
                    return ControlFlow::Break(Rejection::Function);
                }
            }
            Expr::BinaryOp {
                op:
                    BinaryOperator::Custom(_)
                    | BinaryOperator::PGCustomBinaryOperator(_)
                    | BinaryOperator::Assignment,
                ..
            }
            | Expr::Lambda(_) => return ControlFlow::Break(Rejection::Function),
            Expr::In {
                target: InTarget::Table { .. } | InTarget::Unnest(_),
                ..
            } => {
                return ControlFlow::Break(Rejection::TableFunction);
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, _: &Expr) -> ControlFlow<Self::Break> {
        self.leave()
    }

    fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<Self::Break> {
        if let Value::Placeholder(placeholder) = &value.value {
            let Some(name) = placeholder.strip_prefix(':') else {
                return ControlFlow::Break(Rejection::Placeholder);
            };
            if !valid_parameter_name(name) {
                return ControlFlow::Break(Rejection::Placeholder);
            }
            self.parameters.insert(name.to_string());
        }
        ControlFlow::Continue(())
    }
}

fn allowed_function(name: &ObjectName) -> bool {
    let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
        return false;
    };
    if identifier.quote_style.is_some() {
        return false;
    }
    // Expand intentionally with a contract test when another Redshift built-in
    // is needed. Never permit arbitrary names by prefix (including pg_ or st_).
    matches!(
        identifier.value.to_ascii_lowercase().as_str(),
        // Aggregates and analytic windows.
        "any_value" | "avg" | "bit_and" | "bit_or" | "bool_and" | "bool_or"
        | "count" | "cume_dist" | "dense_rank" | "first_value" | "grouping"
        | "lag" | "last_value" | "lead" | "listagg" | "max" | "median" | "min"
        | "nth_value" | "ntile" | "percent_rank" | "percentile_cont" | "percentile_disc"
        | "approximate_percentile_disc" | "rank" | "ratio_to_report" | "row_number"
        | "stddev" | "stddev_pop" | "stddev_samp" | "sum" | "var_pop" | "var_samp" | "variance"
        // Numeric expressions.
        | "abs" | "acos" | "asin" | "atan" | "atan2" | "cbrt" | "ceil" | "ceiling"
        | "cos" | "cot" | "degrees" | "dexp" | "dlog1" | "dlog10" | "exp"
        | "floor" | "ln" | "log" | "mod" | "pi" | "power" | "pow" | "radians"
        | "random" | "round" | "sign" | "sin" | "sqrt" | "tan" | "trunc"
        // Null handling and text transformations.
        | "coalesce" | "decode" | "greatest" | "least" | "nullif" | "nvl" | "nvl2"
        | "ascii" | "btrim" | "char_length" | "character_length" | "charindex" | "chr"
        | "concat" | "initcap" | "left" | "len" | "length" | "lower" | "lpad" | "ltrim"
        | "octet_length" | "position" | "quote_ident" | "quote_literal" | "regexp_count"
        | "regexp_instr" | "regexp_replace" | "regexp_substr" | "repeat" | "replace"
        | "reverse" | "right" | "rpad" | "rtrim" | "soundex" | "split_part" | "strpos"
        | "strtol" | "substr" | "substring" | "textlen" | "translate" | "trim" | "upper"
        | "crc32" | "fnv_hash" | "from_hex" | "md5" | "sha1" | "sha2" | "to_hex"
        | "from_varbyte" | "to_varbyte"
        // Date/time expressions and conversions.
        | "add_months" | "convert_timezone" | "current_date" | "current_time"
        | "current_timestamp" | "dateadd" | "datediff" | "date_part" | "date_trunc"
        | "extract" | "getdate" | "last_day" | "localtime" | "localtimestamp"
        | "months_between" | "next_day" | "sysdate" | "to_char" | "to_date"
        | "to_number" | "to_timestamp"
        // Semi-structured values.
        | "array" | "array_concat" | "array_flatten" | "can_json_parse" | "get_array_length"
        | "is_valid_json" | "is_valid_json_array" | "json_array_length"
        | "json_extract_array_element_text" | "json_extract_path_text" | "json_parse"
        | "json_serialize" | "json_serialize_to_varbyte" | "json_size" | "json_typeof"
        | "object" | "split_to_array" | "subarray"
        // Read-only session identity and explicit privilege introspection.
        | "current_database" | "current_schema" | "current_schemas" | "current_user"
        | "has_database_privilege" | "has_schema_privilege" | "has_table_privilege"
        | "session_user" | "user" | "version"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parameter(name: &str, value: &str) -> SqlParameter {
        SqlParameter {
            name: name.into(),
            value: value.into(),
        }
    }

    #[test]
    fn accepts_realistic_redshift_analytics_and_plain_explain() {
        for sql in [
            "SELECT 1",
            "SELECT lower(email), COUNT(*) FROM audience GROUP BY 1",
            "WITH recent AS (SELECT customer_id, updated_at FROM audience) SELECT customer_id FROM recent QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1",
            "SELECT DATEADD(day, -7, GETDATE()), COALESCE(SUM(amount), 0) FROM sales",
            "SELECT CAST(amount AS DECIMAL(18,2)), amount::varchar, EXTRACT(year FROM created_at) FROM sales",
            "SELECT CASE WHEN active THEN 'yes' ELSE 'no' END FROM audience WHERE email ILIKE '%example.test'",
            "SELECT customer_id FROM a EXCEPT SELECT customer_id FROM b",
            "SELECT a.id FROM a JOIN (SELECT id FROM b) AS b ON a.id = b.id",
            "SELECT JSON_EXTRACT_PATH_TEXT(payload, 'type'), GET_ARRAY_LENGTH(JSON_PARSE('[1,2]')) FROM events",
            "EXPLAIN SELECT COUNT(*) FROM audience",
        ] {
            assert!(validate(sql, &[]).is_ok(), "valid analytics SQL was rejected: {sql}");
        }
    }

    #[test]
    fn binds_named_parameters_through_ctes_and_preserves_double_colon_casts() {
        let sql =
            "WITH ids AS (SELECT :id::bigint AS id) SELECT id, :label FROM ids WHERE id = :id";
        let result = validate(sql, &[parameter("id", "42"), parameter("label", "hello")]).unwrap();
        assert_eq!(result.parameter_count, 2);
        assert_eq!(result.sql, sql);
    }

    #[test]
    fn ignores_parameter_shaped_text_in_literals_and_comments() {
        let sql = "SELECT ':literal', '::text', ':quoted''value' /* :comment */ -- :line\n";
        assert!(validate(sql, &[]).is_ok());
        assert!(validate(sql, &[parameter("comment", "not a binding")]).is_err());
    }

    #[test]
    fn rejects_top_level_writes_transactions_and_multiple_statements() {
        for sql in [
            "",
            "-- only a comment",
            "SELECT 1; SELECT 2",
            "DELETE FROM audience",
            "UPDATE audience SET active = false",
            "INSERT INTO audience VALUES (1)",
            "CREATE TABLE scratch (id integer)",
            "DROP TABLE scratch",
            "BEGIN",
            "COMMIT",
            "ROLLBACK",
            "SET statement_timeout = 0",
            "CALL custom_procedure()",
            "COPY audience FROM 's3://example/input'",
            "UNLOAD ('SELECT 1') TO 's3://example/output'",
            "VALUES (1)",
            "TABLE audience",
        ] {
            assert!(
                validate(sql, &[]).is_err(),
                "unsupported statement accepted: {sql}"
            );
        }
    }

    #[test]
    fn rejects_writes_and_locking_inside_queries() {
        for sql in [
            "SELECT 1 INTO scratch",
            "SELECT * FROM (SELECT 1 INTO scratch) AS s",
            "WITH changed AS (DELETE FROM audience RETURNING id) SELECT * FROM changed",
            "WITH changed AS (UPDATE audience SET active = false RETURNING id) SELECT * FROM changed",
            "WITH changed AS (INSERT INTO audience VALUES (1) RETURNING id) SELECT * FROM changed",
            "SELECT * FROM audience FOR UPDATE",
        ] {
            assert!(validate(sql, &[]).is_err(), "unsupported nested operation accepted: {sql}");
        }
    }

    #[test]
    fn rejects_executing_or_extended_explain_and_nonquery_targets() {
        for sql in [
            "EXPLAIN ANALYZE SELECT 1",
            "EXPLAIN (ANALYZE true) SELECT 1",
            "EXPLAIN (ANALYZE false) SELECT 1",
            "EXPLAIN VERBOSE SELECT 1",
            "EXPLAIN SELECT 1 INTO scratch",
            "EXPLAIN DELETE FROM audience",
        ] {
            assert!(
                validate(sql, &[]).is_err(),
                "unsupported EXPLAIN accepted: {sql}"
            );
        }
    }

    #[test]
    fn denies_unknown_and_qualified_function_calls_at_every_depth() {
        for sql in [
            "SELECT custom_function(1)",
            "SELECT custom.lower('hello')",
            "SELECT \"lower\"('hello')",
            "SELECT COALESCE(custom_function(1), 0)",
            "SELECT 1 WHERE custom_predicate()",
            "WITH q AS (SELECT custom_function(1)) SELECT * FROM q",
            "SELECT SUM(amount) OVER (ORDER BY custom_function(id)) FROM sales",
            "SELECT * FROM custom_table_function()",
            "SELECT * FROM TABLE(custom_function())",
        ] {
            assert!(
                validate(sql, &[]).is_err(),
                "unapproved function accepted: {sql}"
            );
        }
    }

    #[test]
    fn checks_missing_extra_duplicate_and_invalid_parameter_bindings() {
        assert!(validate("SELECT :id", &[]).is_err());
        assert!(validate("SELECT 1", &[parameter("id", "1")]).is_err());
        assert!(validate("SELECT :id", &[parameter("id", "1"), parameter("id", "2")]).is_err());
        assert!(validate("SELECT :id", &[parameter("id", "")]).is_err());
        assert!(validate("SELECT :id", &[parameter("id", "a\0b")]).is_err());
        for name in ["", ":id", "bad-name", "1id", "space name", "é"] {
            assert!(validate("SELECT 1", &[parameter(name, "value")]).is_err());
        }
        for sql in ["SELECT $1", "SELECT ?", "SELECT @id"] {
            assert!(validate(sql, &[parameter("id", "1")]).is_err());
        }
        assert!(validate("SELECT :id", &[parameter("id", " ")]).is_ok());
        assert!(validate("SELECT :Id", &[parameter("id", "1")]).is_err());
    }

    #[test]
    fn bounds_input_and_parameter_payloads() {
        assert!(validate(&format!("SELECT '{}'", "x".repeat(MAX_SQL_BYTES)), &[]).is_err());
        assert!(validate(
            "SELECT :id",
            &[parameter("id", &"x".repeat(MAX_PARAMETER_VALUE_BYTES + 1))]
        )
        .is_err());
        assert!(validate(
            "SELECT 1",
            &[parameter(&"x".repeat(MAX_PARAMETER_NAME_BYTES + 1), "1")]
        )
        .is_err());
        let parameters: Vec<_> = (0..=MAX_PARAMETERS)
            .map(|i| parameter(&format!("p{i}"), "x"))
            .collect();
        assert!(validate("SELECT 1", &parameters).is_err());
        let parameters: Vec<_> = (0..10)
            .map(|i| parameter(&format!("p{i}"), &"x".repeat(MAX_PARAMETER_VALUE_BYTES)))
            .collect();
        assert!(validate("SELECT 1", &parameters).is_err());
        let nested = format!("SELECT {}1{}", "(".repeat(100), ")".repeat(100));
        assert!(validate(&nested, &[]).is_err());
    }

    #[test]
    fn bounds_lexical_complexity_before_building_an_ast() {
        // SELECT, 512 literals and 511 commas consume exactly 1024 tokens.
        let projection = vec!["1"; MAX_SQL_TOKENS / 2].join(",");
        let at_limit = format!("SELECT {projection}");
        assert!(validate(&at_limit, &[]).is_ok());
        assert!(validate(&format!("{at_limit} /* ignored syntax */"), &[]).is_ok());
        let error = validate(&format!("{at_limit}, 2"), &[]).unwrap_err();
        assert_eq!(error.code, "sql_complexity_limit");
        // The lexical bound also runs before parsing malformed excess syntax.
        let error = validate(&format!("{at_limit},"), &[]).unwrap_err();
        assert_eq!(error.code, "sql_complexity_limit");
    }

    #[test]
    fn token_budget_does_not_count_literal_contents_or_comments_as_syntax() {
        let literal = "SELECT , + :ignored ".repeat(MAX_SQL_TOKENS);
        let comment = "SELECT , + :ignored ".repeat(MAX_SQL_TOKENS);
        let sql = format!("SELECT '{literal}' /* {comment} */");
        assert!(sql.len() < MAX_SQL_BYTES);
        assert!(validate(&sql, &[]).is_ok());
    }

    #[test]
    fn errors_do_not_echo_sql_parameter_names_or_values() {
        let secret = "sensitive_canary_4711";
        for result in [
            validate(&format!("SELECT {secret}(1)"), &[]),
            validate(&format!("SELECT '{secret}"), &[]),
            validate("SELECT :id", &[parameter(secret, secret)]),
            validate("SELECT :id", &[parameter("id", &format!("{secret}\0"))]),
        ] {
            let error = result.unwrap_err();
            assert!(!error.to_string().contains(secret));
            assert!(!format!("{error:?}").contains(secret));
        }
    }

    #[test]
    fn fingerprint_is_stable_for_trimmed_sql_without_parameter_data() {
        let first = validate(" SELECT :id ", &[parameter("id", "one")]).unwrap();
        let second = validate("SELECT :id", &[parameter("id", "two")]).unwrap();
        assert_eq!(first.fingerprint, second.fingerprint);
        assert_eq!(first.fingerprint.len(), 64);
        assert_ne!(
            first.fingerprint,
            validate("SELECT 1", &[]).unwrap().fingerprint
        );
    }
}
