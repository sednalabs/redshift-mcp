# SQL compatibility

PR1 accepts one `SELECT`/`WITH` query or plain `EXPLAIN` of a query. It parses
with the Redshift dialect and rejects syntax it cannot confidently classify.
This deliberately supports a narrower surface than all SQL accepted by Redshift.
`validate_sql` performs these checks without contacting AWS. It does not resolve
objects, inspect view definitions, validate permissions, or estimate execution
cost.

Joins, subqueries, common table expressions, set operations, aggregates, window
functions and Redshift `QUALIFY` are supported when all expressions pass the
policy. Ordinary casts and date/time expressions are also supported. Writes,
`SELECT INTO`, transaction commands, settings changes, locking clauses,
table functions, custom operators and execution-analysis options are rejected.
The server owns the transaction and statement-timeout wrapper.

## Named parameters

Use `:name` placeholders with a `parameters` array of `{ "name": "name",
"value": "text" }` bindings. Bind each distinct name once; repeated uses of a
placeholder share the binding. Names are case-sensitive ASCII identifiers,
starting with a letter or underscore. Their maximum length is 128 bytes.

Parameter values are nonempty strings, as required by the Data API. Use an
explicit SQL cast when a type matters, such as `CAST(:cutoff AS DATE)`. Empty
strings, NUL bytes, JSON null values and positional `$1` or `?` parameters are
not supported. A parameter substitutes a value, not a table or column name.

The maximum SQL length is 100,000 UTF-8 bytes. There are at most 100 distinct
parameters; each value is at most 16,384 bytes, and all parameter names and
values together are at most 100,000 bytes. The parser and visitor also limit
nesting and syntax complexity to protect local processing. Before constructing
an AST, the validator admits at most 1,024 lexical tokens, excluding whitespace
and comments. A quoted literal counts as one token regardless of its text
length, while the overall byte limit still applies. Queries with very large
generated projections or set-operation lists may need to be simplified.

## Function allowlist

Function calls must use an unquoted, unqualified name from the following list.
Names are matched case-insensitively. Unknown functions and schema-qualified
calls are rejected, including otherwise valid Redshift functions not yet
reviewed for this surface. These are syntactic allowances, not a promise that
every listed overload exists in your Redshift version.

Database grants remain the authorization boundary. Name checks do not inspect
functions hidden in views or prove which overloaded function the database will
resolve. Use a database principal that cannot invoke unwanted functions.

### Aggregates and analytic windows

`any_value`, `avg`, `bit_and`, `bit_or`, `bool_and`, `bool_or`, `count`, `cume_dist`, `dense_rank`, `first_value`, `grouping`, `lag`, `last_value`, `lead`, `listagg`, `max`, `median`, `min`, `nth_value`, `ntile`, `percent_rank`, `percentile_cont`, `percentile_disc`, `approximate_percentile_disc`, `rank`, `ratio_to_report`, `row_number`, `stddev`, `stddev_pop`, `stddev_samp`, `sum`, `var_pop`, `var_samp`, `variance`.

### Numeric expressions

`abs`, `acos`, `asin`, `atan`, `atan2`, `cbrt`, `ceil`, `ceiling`, `cos`, `cot`, `degrees`, `dexp`, `dlog1`, `dlog10`, `exp`, `floor`, `ln`, `log`, `mod`, `pi`, `power`, `pow`, `radians`, `random`, `round`, `sign`, `sin`, `sqrt`, `tan`, `trunc`.

### Null handling and text transformations

`coalesce`, `decode`, `greatest`, `least`, `nullif`, `nvl`, `nvl2`, `ascii`, `btrim`, `char_length`, `character_length`, `charindex`, `chr`, `concat`, `initcap`, `left`, `len`, `length`, `lower`, `lpad`, `ltrim`, `octet_length`, `position`, `quote_ident`, `quote_literal`, `regexp_count`, `regexp_instr`, `regexp_replace`, `regexp_substr`, `repeat`, `replace`, `reverse`, `right`, `rpad`, `rtrim`, `soundex`, `split_part`, `strpos`, `strtol`, `substr`, `substring`, `textlen`, `translate`, `trim`, `upper`, `crc32`, `fnv_hash`, `from_hex`, `md5`, `sha1`, `sha2`, `to_hex`, `from_varbyte`, `to_varbyte`.

### Date/time expressions and conversions

`add_months`, `convert_timezone`, `current_date`, `current_time`, `current_timestamp`, `dateadd`, `datediff`, `date_part`, `date_trunc`, `extract`, `getdate`, `last_day`, `localtime`, `localtimestamp`, `months_between`, `next_day`, `sysdate`, `to_char`, `to_date`, `to_number`, `to_timestamp`.

### Semi-structured values

`array`, `array_concat`, `array_flatten`, `can_json_parse`, `get_array_length`, `is_valid_json`, `is_valid_json_array`, `json_array_length`, `json_extract_array_element_text`, `json_extract_path_text`, `json_parse`, `json_serialize`, `json_serialize_to_varbyte`, `json_size`, `json_typeof`, `object`, `split_to_array`, `subarray`.

### Read-only session identity and explicit privilege introspection

`current_database`, `current_schema`, `current_schemas`, `current_user`, `has_database_privilege`, `has_schema_privilege`, `has_table_privilege`, `session_user`, `user`, `version`.

## Extending the surface

Add a needed built-in in `src/sql.rs` only after reviewing its Redshift semantics,
and add a representative accepted-query test plus any relevant rejected-shape
test. Update this list with the code. Do not replace the allowlist with a naming
prefix or permissive fallback when a query is rejected.
