from icecream import ic
from pathlib import Path
import yaml
import polars as pl
import logging 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
logger = logging.getLogger(__name__)
def find_data_files(config: dict) -> list:
    #find_source_file_type = config.get("find_source_file_type", None)
    input_file_extension = config.get("input_file_extension", None)
    input_delimiter = config.get("delimiter", None)
    source_files = config.get("source_files", [])
    pathed_files = []
    if source_files:
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


    files = list(Path().glob(f"*{input_file_extension}"))
    target_file_suffix = [x.get('output_file_suffix') + x.get('output_file_extension') for x in config.get("output_file_configs", [])]
    logger.info(target_file_suffix)
    #remove file from files if ends with any one of target_file_suffix list
    files_without_target = [path for path in files if not path.name.endswith(tuple(target_file_suffix))]

    logger.info(files_without_target)
    pathed_files = [ {"name": file, "delimiter": input_delimiter} for file in files_without_target]
    if pathed_files:
        return pathed_files
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
        logger.info(file_name)
        logger.info(delimiter)
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


def get_output_file_name(config: dict, output: dict, newest_file: str = None) -> str:
    if output.get("keep_source_file_name"):
        if not newest_file:
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
            logger.info(df)
        else:
            df_new = _read_data_file(file, config)
            for column in df.columns:
                if df[column].dtype == pl.datatypes.Utf8 or df_new[column].dtype == pl.datatypes.Utf8:
                    df = df.with_columns(df[column].cast(pl.datatypes.Utf8).alias(column))
                    df_new = df_new.with_columns(df_new[column].cast(pl.datatypes.Utf8).alias(column))
            logger.info(df_new)
            df = pl.concat([df_new, df], how='vertical')
    logger.info(df)
    return df, source_files

def get_dropped_fields_file(file_suffix):
    file_name = Path(f"dropped_fields{file_suffix}.csv")
    if not file_name.exists():
        logger.info(f"will create {file_name}")
        file_name.touch()
        action = "created"
    else:
        action = None
    return file_name, action

def clean_up_drop_fields(df, output_file_suffix):
    drop_file, action = get_dropped_fields_file(output_file_suffix)
    if action == "created":
        #write out only if field drop_indicator is set to true
        df.filter(pl.col("legoberry_drop_field_indicator") == True).write_csv(drop_file)
    return df.filter(pl.col("legoberry_drop_field_indicator") == False)
    