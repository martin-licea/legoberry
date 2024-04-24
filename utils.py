from icecream import ic
from pathlib import Path
import yaml
import polars as pl

def find_data_files(config: dict) -> list:
    source_files = config.get("source_files", [])
    pathed_files = []
    for file in source_files:
        if not Path(file.get("name")).exists():
            raise FileNotFoundError(f"{file.get('name')} not found.")
        else:
            pathed_file = Path(file.get("name"))
            delimiter = file.get("delimiter", None)
            file_object = {
                "name": pathed_file
            }
            if delimiter:
                file_object["delimiter"] = delimiter
            pathed_files.append(file_object)
    
    if pathed_files:
        return pathed_files
    
        
    if source_files:
        for file in source_files:
            filename = file.get("name")
            if not Path(filename).exists():
                raise FileNotFoundError(f"{filename} not found.")
        return source_files

    files = list(Path().glob("*.xlsx"))
    files += list(Path().glob("*.csv"))
    files += list(Path().glob("*.dat"))
    files += list(Path().glob("*.txt"))
    target_file = config.get("target_file", None)
    ic(target_file)
    if target_file:
        files_without_target = [file for file in files if file.name != target_file]
        return files_without_target
    
    raise ValueError("No source files found in the config file.")

def get_config() -> dict:
    config_file = "config.yml" if Path("config.yml").exists() else "config.yaml"

    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config

def delimiter_mapper(delimiter: str) -> str:
    delim_mapper = {
        "comma": ",",
        "pipe": "|",
        "tab": "\t",
        "space": " "
    }
    return delim_mapper.get(delimiter, delimiter)

def _read_data_file(file: Path, config: dict) -> pl.DataFrame:
    file_suffix=file.get("name").suffix
    file_name = file.get("name").name
    delimiter = delimiter_mapper(file.get("delimiter", "|"))
    if file_suffix == ".xlsx":
        return pl.read_excel(file_name)
    elif file_suffix == ".csv":
        return pl.read_csv(file_name)
    elif file_suffix in (".dat", ".txt"):
        ic(file_name)
        ic(delimiter)
        df = pl.read_csv(file_name, separator=delimiter, truncate_ragged_lines=True, ignore_errors=True)
        return df

def create_output_file(create_output_file: dict, df: pl.DataFrame, column_formats: dict = None):
    file = Path(create_output_file)
    suffix = file.suffix
    if suffix == ".csv":
        df.write_csv(create_output_file)
    elif suffix == ".xlsx":
        df.write_excel(create_output_file, include_header=True, column_formats=column_formats)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def get_output_file_name(config: dict, output: dict) -> str:
    if output.get("keep_source_file_name"):
        source_files = config.get("source_files")
        file_names = [file.get("name") for file in source_files]
        sorted_file_names = sorted(file_names, reverse=True)
        newest_file = sorted_file_names[0]
        root_file_name = Path(newest_file).stem
        if output.get("output_file_suffix"):
            root_file_name += output.get("output_file_suffix")
        if output.get("output_file_extension"):
            root_file_name += output.get("output_file_extension")
        return root_file_name
    if output.get("output_file_name"):
        return output.get("output_file_name")
    
def read_data_files(config: dict) -> pl.DataFrame:
    source_files = find_data_files(config)
    for file in source_files:
        #check if df exists
        if not "df" in locals():
            df = _read_data_file(file, config)
            ic(df)
        else:
            df_new = _read_data_file(file, config)
            for column in df.columns:
                if df[column].dtype == pl.datatypes.Utf8 or df_new[column].dtype == pl.datatypes.Utf8:
                    df = df.with_columns(df[column].cast(pl.datatypes.Utf8).alias(column))
                    df_new = df_new.with_columns(df_new[column].cast(pl.datatypes.Utf8).alias(column))
            ic(df_new)
            df = pl.concat([df_new, df], how='vertical')
    ic(df)
    return df