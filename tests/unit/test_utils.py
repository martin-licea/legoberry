# coding: utf-8
import sys
from pathlib import Path
# Ensure project root is on PYTHONPATH so that utils.py can be imported
sys.path.insert(0, str(Path(__file__).parents[2]))

import os

import yaml
import polars as pl
import pytest

import utils


def test_delimiter_mapper():
    assert utils.delimiter_mapper("comma") == ","
    assert utils.delimiter_mapper("pipe") == "|"
    assert utils.delimiter_mapper("tab") == "\t"
    assert utils.delimiter_mapper("space") == " "
    # Unknown delimiters are returned unchanged
    assert utils.delimiter_mapper(";") == ";"


def test_get_output_file_name_keep_source(tmp_path):
    config = {"source_files": [{"name": "a.csv"}, {"name": "b.csv"}]}
    output = {
        "keep_source_file_name": True,
        "output_file_suffix": "_suf",
        "output_file_extension": ".ext",
    }
    name = utils.get_output_file_name(config, output)
    # 'b.csv' is lexically highest when sorted reverse, so 'b' is used
    assert name == "b_suf.ext"


def test_get_output_file_name_with_newest_file():
    config = {}
    output = {
        "keep_source_file_name": True,
        "output_file_suffix": "_suf",
        "output_file_extension": ".ext",
    }
    name = utils.get_output_file_name(config, output, newest_file="xyz.csv")
    assert name == "xyz_suf.ext"


def test_get_output_file_name_direct_name():
    config = {}
    output = {"output_file_name": "myfile.out"}
    assert utils.get_output_file_name(config, output) == "myfile.out"


def test_find_data_files_source_files(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    # When source file is missing, expect FileNotFoundError
    cfg_missing = {"source_files": [{"name": "no_exist.csv"}]}
    with pytest.raises(FileNotFoundError):
        utils.find_data_files(cfg_missing)

    # When source file exists, return the path and delimiter if provided
    sample = tmp_path / "sample.csv"
    sample.write_text("col1,col2\nx,y")
    cfg = {"source_files": [{"name": str(sample), "delimiter": ";"}]}
    result = utils.find_data_files(cfg)
    assert result == [{"name": Path(str(sample)), "delimiter": ";"}]


def test_get_config(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    data = {"key": "value"}
    # config.yml takes precedence if present
    cfgyml = tmp_path / "config.yml"
    cfgyml.write_text(yaml.safe_dump(data))
    assert utils.get_config() == data
    # if config.yml is removed but config.yaml exists, read config.yaml
    cfgyml.unlink()
    cfgyaml = tmp_path / "config.yaml"
    cfgyaml.write_text(yaml.safe_dump(data))
    assert utils.get_config() == data


def test_get_dropped_fields_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    drop_file, action = utils.get_dropped_fields_file("_t")
    assert drop_file.name == "dropped_fields_t.csv"
    assert action == "created"
    assert drop_file.exists()
    # Second call should not recreate the file
    _, action2 = utils.get_dropped_fields_file("_t")
    assert action2 is None


def test_clean_up_drop_fields(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    df = pl.DataFrame({
        "legoberry_drop_field_indicator": [True, False, True],
        "value": [1, 2, 3],
    })
    cleaned = utils.clean_up_drop_fields(df, "_t")
    # Only rows with indicator False are retained
    assert cleaned["value"].to_list() == [2]
    # The dropped file should contain only the dropped rows
    drop_file = Path("dropped_fields_t.csv")
    dropped_df = pl.read_csv(drop_file)
    assert dropped_df["value"].to_list() == [1, 3]


def test_create_output_file_csv(tmp_path):
    out_path = tmp_path / "out.csv"
    df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    utils.create_output_file(str(out_path), df)
    # Read back and compare
    result = pl.read_csv(out_path)
    assert result.equals(df)


def test_read_data_files(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    # Create two CSV files with identical schema
    df1 = pl.DataFrame({"x": [1], "y": [2]})
    df2 = pl.DataFrame({"x": [3], "y": [4]})
    df1.write_csv("file1.csv")
    df2.write_csv("file2.csv")
    cfg = {"input_file_extension": ".csv", "delimiter": ",", "output_file_configs": []}
    combined, sources = utils.read_data_files(cfg)
    # Combined DataFrame should include rows from both files
    assert set(combined["x"].to_list()) == {1, 3}
    # Source list should reflect the two files
    assert len(sources) == 2