# tests/unit/test_transformations.py

import pytest
import polars as pl
import transformations as tf
import yaml

@pytest.fixture
def test_df():
    # Replace 'sample_files/Dummy_Data_File.xlsx' with the path to your sample file
    return pl.read_csv('tests/sample_files/dummy_data_file_tab_delimited.txt', separator="\t", truncate_ragged_lines=True, ignore_errors=True)

@pytest.fixture
def test_config():
    with open('tests/configs/config_field_in_fields.yaml', 'r') as f:
        data = yaml.safe_load(f)
    return data

def test_fix_nested_fields(test_df, test_config):
    source_df = tf.fix_nested_fields(test_config, test_df)
    #assert len(source_df.columns) == 14
    print(source_df.columns)
    assert source_df.columns == [
        'Order Number', 
        'First', 
        'Last', 
        'Title', 
        'Address', 
        'Address 2', 
        'City', 
        'State', 
        'Zip', 
        'Phone', 
        'Email', 
        'Date', 
        'row_number', 
        'survey',
        'referral'
    ]
    #assert source_df.shape == (30, 14)
    #assert source_df.select('row_number')
    print(source_df)