# coding: utf-8
import sys
from pathlib import Path
# Ensure project root is on PYTHONPATH so that transformations.py can be imported
sys.path.insert(0, str(Path(__file__).parents[2]))
import pytest
import polars as pl
import transformations
import copy
from icecream import ic
import yaml

def test_fields_in_field(monkeypatch):
    """
    Test that fix_nested_fields correctly explodes the 'Custom Fields' column
    into separate columns based on the given delimiter and key/value splitter.
    """
    # Prevent writing output file during test
    monkeypatch.setattr(transformations, 'create_output_file', lambda fname, df: None)
    # Locate the sample data file
    sample_file = Path(__file__).parents[1] / 'sample_files' / 'dummy_data_file_tab_delimited.txt'
    # Read the tab-delimited file
    df = pl.read_csv(
        sample_file,
        separator='\t',
        quote_char='"',
        ignore_errors=True,
        truncate_ragged_lines=True,
    )
    # Define config for fields_in_field feature
    config = {
        'fields_in_field': [
            {
                'field_name': 'Custom Fields',
                'delimiter': '\t',
                'key_value_splitter': ':',
                'ignore_junk_field_string': ['n'],
            }
        ]
    }
    # Apply the fix_nested_fields transformation
    result = transformations.fix_nested_fields(config, df, sample_file)
    # The original 'Custom Fields' column should be removed
    assert 'Custom Fields' not in result.columns
    # Expected new columns from the custom fields
    expected_fields = [
        'survey',
        'referral',
        'Grade Level',
        'Alternate E-Mail Address',
        'Date of Birth (Required by UCSD)',
        'Gender (Required by UCSD)',
    ]
    for field in expected_fields:
        assert field in result.columns, f"Missing exploded field column: {field}"
    # Verify values for the first row (row_number == '0')
    # Convert to dict for easy access
    rows = result.filter(pl.col('row_number') == '0').to_dicts()
    assert len(rows) == 1, "Expected exactly one record for row_number '0'"
    first = rows[0]
    # Check that values match the first sample record
    assert first['survey'] == 'Friend or Colleague'
    # 'referral' was empty in the sample
    assert first['referral'] == ''
    assert first['Grade Level'] == 'High School'
    assert first['Alternate E-Mail Address'] == 'gonzalezalex@hotmail.com'
    assert first['Date of Birth (Required by UCSD)'] == '04/07/1972'
    assert first['Gender (Required by UCSD)'] == 'Male'

@pytest.fixture
def sample_polars_df():
    return pl.DataFrame({
        "id": [1, 2, 3, 4],
        "first_name": ["Alice", "Bob", "Charlie", "Diana"],
        "last_name": ["Smith-Garcia", "O'Brian", "Krispy", "not&valid"],
        "age": [25, 30, 35, 40],
        "salary": [50000.0, 60000.0, 70000.0, 80000.0],
        "is_active": [True, False, True, False],
    })

def test_create_new_fields(sample_polars_df):
    df = sample_polars_df
    field = {
        "alias": "full_name",
        "default_value": "{first_name}.{last_name}",
        "source_fields_alpha_only": True
    }
    field_with_allow_chars = copy.deepcopy(field)
    field_with_allow_chars['allow_special_char_list'] = ["-", "&"]
    df_transformed = transformations.create_new_fields(df, field)
    df_allowed = transformations.create_new_fields(df, field_with_allow_chars)
    expected = ["Alice.SmithGarcia", "Bob.OBrian", "Charlie.Krispy", "Diana.notvalid"]
    expected_allow_chars = ["Alice.Smith-Garcia", "Bob.OBrian", "Charlie.Krispy", "Diana.not&valid"]

    assert df_transformed["full_name"].to_list() == expected
    assert df_allowed["full_name"].to_list() == expected_allow_chars

def test_rename_columns(sample_polars_df):
    df = sample_polars_df
    field = {"source_name": "first_name", "alias": "fname"}
    df2 = transformations.rename_columns(df, field)
    assert "fname" in df2.columns and "first_name" not in df2.columns

def test_replace_strings_literal(sample_polars_df):
    df = sample_polars_df
    field = {"source_name": "first_name", "replace": [{"from": "Alice", "to": "Alicia"}]}
    df2 = transformations.replace_strings(df, field)
    assert df2["first_name"].to_list()[0] == "Alicia"

def test_replace_strings_from_file(tmp_path, sample_polars_df):
    df = sample_polars_df
    replace_list = [{"from": "Bob", "to": "Bobby"}]
    file_path = tmp_path / "replace.yaml"
    file_path.write_text(yaml.safe_dump(replace_list))
    field = {"source_name": "first_name", "replace": [], "replace_with_file": str(file_path)}
    df2 = transformations.replace_strings(df, field)
    assert df2["first_name"].to_list()[1] == "Bobby"

def test_fix_casing_variants():
    df = pl.DataFrame({"name": ["alice smith", "bOB jOnes"]})
    # upper
    df_up = transformations.fix_casing(df, {"source_name": "name", "casing": "upper"})
    assert df_up["name"].to_list() == ["ALICE SMITH", "BOB JONES"]
    # lower
    df_low = transformations.fix_casing(df, {"source_name": "name", "casing": "lower"})
    assert df_low["name"].to_list() == ["alice smith", "bob jones"]
    # proper
    df_prop = transformations.fix_casing(df, {"source_name": "name", "casing": "proper"})
    assert df_prop["name"].to_list() == ["Alice Smith", "Bob Jones"]
    # name (Mc and apostrophes/hyphens)
    df2 = pl.DataFrame({"name": ["mcdonald", "o'brian", "anne-marie"]})
    df_name = transformations.fix_casing(df2, {"source_name": "name", "casing": "name"})
    assert df_name["name"].to_list() == ["McDonald", "O'Brian", "Anne-Marie"]

def test_validate_against_list(tmp_path, sample_polars_df):
    df = sample_polars_df
    valid = ["Alice", "Charlie"]
    list_file = tmp_path / "valid.yaml"
    list_file.write_text(yaml.safe_dump(valid))
    field = {"source_name": "first_name", "in_list": str(list_file)}
    df2 = transformations.validate_against_list(df, field)
    result = df2["first_name"].to_list()
    assert result[0] == "Alice"
    assert result[2] == "Charlie"
    assert result[1].startswith("%%%%") and result[1].endswith("Bob%%%%")
    assert result[3].startswith("%%%%") and result[3].endswith("Diana%%%%")

def test_select_columns(sample_polars_df):
    df = sample_polars_df
    field = {"ordered_headers": ["id", "last_name"]}
    df2 = transformations.select_columns(df, field)
    assert list(df2.columns) == ["id", "last_name", "legoberry_drop_field_indicator", "legoberry_reason_for_drop"]

def test_get_excel_formats():
    config = {"fields": [
        {"source_name": "a", "alias": "A", "excel_format": "0"},
        {"source_name": "b", "excel_format": None},
        {"source_name": "c", "alias": "C"}
    ]}
    fmt = transformations.get_excel_formats(config)
    assert fmt == {"A": "0"}

def test_drop_duplicates():
    df = pl.DataFrame({"a": [1, 2, 1], "b": [3, 4, 3]})
    cfg = {"fields_to_consider_duplicates": ["a", "b"]}
    df2 = transformations.drop_duplicates(df, cfg)
    assert df2.shape[0] == 2
    assert sorted(df2["a"].to_list()) == [1, 2]

def test_truncate_max_length():
    df = pl.DataFrame({"text": ["abcdef", "gh"]})
    field = {"source_name": "text", "max_length": 3}
    df2 = transformations.truncate_max_length(df, field)
    assert df2["text"].to_list() == ["abc", "gh"]

def test_create_record_off_field():
    df = pl.DataFrame({"val": [1], "expand": [2]})
    field = {"source_name": "expand", "expand_on": "val"}
    df2 = transformations.create_record_off_field(df, field)
    assert df2["val"].to_list() == [1, 2]

def test_drop_nulls_and_drop_if_length_less_than(sample_polars_df):
    df = pl.DataFrame({"col": [None, "x", ""]})
    # drop_nulls
    field_null = {"source_name": "col", "drop_full_row_if_empty": True}
    df_null = transformations.drop_nulls(df, field_null)
    indicator = df_null["legoberry_drop_field_indicator"].to_list()
    reason = df_null["legoberry_reason_for_drop"].to_list()
    assert indicator == [True, False, True]
    assert reason[0] == "col is Null"
    assert reason[2] == "col is Null"
    # drop_if_length_less_than
    field_len = {"source_name": "col", "alias": None, "drop_if_length_less_than": 1}
    df_len = transformations.drop_if_length_less_than(df, field_len)
    ind_len = df_len["legoberry_drop_field_indicator"].to_list()
    reason_len = df_len["legoberry_reason_for_drop"].to_list()
    assert ind_len == [False, False, True]
    assert "is less than 1" in reason_len[-1]

def test_format_fields_variants():
    # date with reformat and invalid dates flagged
    df = pl.DataFrame({"d": ["2020-01-02", "bad-date", ""]})
    field_date = {"source_name": "d", "data_type": "date", "data_format": "%Y-%m-%d", "reformat_to": "%d/%m/%Y"}
    df_date = transformations.format_fields(df, field_date)
    assert df_date["d"].to_list() == ["02/01/2020", "%%%%bad-date%%%%", None]

    # integer with numeric input
    df2 = pl.DataFrame({"i": [1, None]})
    field_int = {"source_name": "i", "data_type": "integer"}
    df2_i = transformations.format_fields(df2, field_int)
    assert df2_i["i"].dtype == pl.Int64 and df2_i["i"].to_list() == [1, None]

    # number with numeric input
    df3 = pl.DataFrame({"n": [1.5, None]})
    field_num = {"source_name": "n", "data_type": "number"}
    df3_n = transformations.format_fields(df3, field_num)
    assert df3_n["n"].dtype == pl.Float64 and df3_n["n"].to_list() == [1.5, None]

    # string conversion
    df4 = pl.DataFrame({"s": [1, None]})
    field_str = {"source_name": "s", "data_type": "string"}
    df4_s = transformations.format_fields(df4, field_str)
    assert df4_s["s"].dtype == pl.Utf8

    # boolean conversion
    df5 = pl.DataFrame({"b": [True, False]})
    field_bool = {"source_name": "b", "data_type": "boolean"}
    df5_b = transformations.format_fields(df5, field_bool)
    assert df5_b["b"].dtype == pl.Boolean

    # phone number formatting
    df6 = pl.DataFrame({"p": ["123-456-7890", "11234567890", "000"]})
    field_phone = {"source_name": "p", "data_type": "phone number"}
    df6_p = transformations.format_fields(df6, field_phone)
    assert df6_p["p"].to_list() == ["(123) 456-7890", "(123) 456-7890", "%%%%000%%%%"]

    # zip code formatting
    df7 = pl.DataFrame({"z": ["12345-6789", "2345", "12", ""]})
    field_zip = {"source_name": "z", "data_type": "zip code"}
    df7_z = transformations.format_fields(df7, field_zip)
    assert df7_z["z"].to_list() == ["12345", "02345", "%%%%12%%%%", "%%%%%%%%"]

    # email validation/formatting
    df8 = pl.DataFrame({"e": ["test@example.com", "bad-email", ""]})
    field_email = {"source_name": "e", "data_type": "email"}
    df8_e = transformations.format_fields(df8, field_email)
    assert df8_e["e"].to_list() == ["test@example.com", "%%%%bad-email%%%%", None]


def test_drop_if_length_less_than_with_non_ascii():
    df = pl.DataFrame({"col": ["ñ", "éé", "", None, "abc"]})
    field_len = {"source_name": "col", "alias": None, "drop_if_length_less_than": 2}
    df_len = transformations.drop_if_length_less_than(df, field_len)
    ind_len = df_len["legoberry_drop_field_indicator"].to_list()
    reason_len = df_len["legoberry_reason_for_drop"].to_list()
    # Note: str.lengths() measures byte length, so multibyte chars count accordingly
    # Expected: only empty string ("" -> length 0) is flagged for drop (length < 2)
    assert ind_len == [False, False, True, False, False]
    assert reason_len[2] == "Length of col is less than 2"
    for idx in [0, 1, 3, 4]:
        assert reason_len[idx] is None

def test_fix_casing_name_initial_letter_removal_non_ascii_and_short():
    df = pl.DataFrame({"name": ["j doe", "É Dupont", "X "]})
    df_name = transformations.fix_casing(df, {"source_name": "name", "casing": "name"})
    # "j doe" -> "Doe"; "É Dupont" -> "Dupont"; "X " length <=2 remains unchanged
    assert df_name["name"].to_list() == ["Doe", "Dupont", "X "]