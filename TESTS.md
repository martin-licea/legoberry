 # TESTS

 This document provides an overview of each pytest defined in `tests/unit/test_transformations.py`,
 summarizing the behavior and scenarios covered by each test function.

 ## tests/unit/test_transformations.py

 ### test_fields_in_field
 Verifies that `transformations.fix_nested_fields` explodes the `Custom Fields` column into separate
 columns based on the configured delimiter and key/value splitter, removes the original column, and
 populates the new fields with the expected values.

 ### test_create_new_fields
 Ensures that `transformations.create_new_fields` constructs a new field by combining source fields
 using a templated default value (`{first_name}.{last_name}`), applying alpha-only filtering by default,
 and honoring an `allow_special_char_list` when provided.

 ### test_rename_columns
 Checks that `transformations.rename_columns` correctly renames a column alias and drops the original
 column.

 ### test_replace_strings_literal
 Tests that `transformations.replace_strings` applies an in-memory list of literal replacement pairs
 to the values of a specified column.

 ### test_replace_strings_from_file
 Validates that `transformations.replace_strings` can load replacement pairs from a YAML file (via
 `replace_with_file`) and apply those replacements to the column values.

 ### test_fix_casing_variants
 Covers the `transformations.fix_casing` function for all supported casing modes: upper, lower, proper
 (title case), and name-style (handling Mc prefixes, apostrophes, and hyphens).

 ### test_validate_against_list
 Verifies that `transformations.validate_against_list` loads a valid value list from a YAML file and
 wraps any out-of-list values with `%%%%` markers while leaving valid values unchanged.

 ### test_select_columns
 Asserts that `transformations.select_columns` reorders the DataFrame according to `ordered_headers`
 and appends the drop-indicator (`legoberry_drop_field_indicator`) and reason
 (`legoberry_reason_for_drop`) columns.

 ### test_get_excel_formats
 Ensures that `transformations.get_excel_formats` extracts a mapping of `alias` to `excel_format` from
 the configuration only for fields where an alias is defined and `excel_format` is not `None`.

 ### test_drop_duplicates
 Checks that `transformations.drop_duplicates` removes duplicate rows based on the specified fields list.

 ### test_truncate_max_length
 Tests that `transformations.truncate_max_length` shortens string values exceeding `max_length` while
 leaving shorter strings intact.

 ### test_create_record_off_field
 Verifies that `transformations.create_record_off_field` expands a DataFrame row into multiple records
 by treating the `source_name` field as additional values for the `expand_on` key.

 ### test_drop_nulls_and_drop_if_length_less_than
 Covers two transformations in one test:
 - `transformations.drop_nulls`: marks rows with null or empty values as dropped with appropriate
   indicators and reasons.
 - `transformations.drop_if_length_less_than`: drops rows where the string length of a field is below
   the configured threshold, again setting indicators and reasons.

 ### test_format_fields_variants
 Exercises `transformations.format_fields` across all supported data types:
- **date**: parses and reformats dates (`empty strings become None`, invalid dates are flagged with `%%%%…%%%%`).
 - **integer**: casts numeric values to integer type (`Int64`) preserving nulls.
 - **number**: casts numeric values to float type (`Float64`) preserving nulls.
 - **string**: casts values to string (`Utf8`).
 - **boolean**: casts values to boolean (`Boolean`).
 - **phone number**: formats 10-digit phone numbers into `(XXX) XXX-XXXX`, marks invalid patterns with
   `%%%%…%%%%`.
 - **zip code**: normalizes ZIP codes to 5-digit strings (zero-padded), flags invalid codes with
   `%%%%…%%%%`.
 - **email**: validates email addresses, preserving valid ones, flagging invalid ones with `%%%%…%%%%`,
   and converting empty strings to `None`.