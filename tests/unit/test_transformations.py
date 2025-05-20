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
        "address": [],
        "zip": [],
        "city": [],
        "state": [],
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