import polars as pl
from icecream import ic
from utils import find_data_files, get_config, create_output_file, get_output_file_name, read_data_files
import transformations as tf

def main():
    config = get_config()
    df = read_data_files(config)
    df = tf.fix_nested_fields(config, df)
    output_file_configs = config.get("output_file_configs", [])
    if not output_file_configs:
        raise ValueError("No output file configurations found in the config file.")
    for output in output_file_configs:
        for field in output.get('fields'):
            df = tf.rename_columns(df, field)
            df = tf.create_new_fields(df, field)
            df = tf.replace_strings(df, field)
            df = tf.fix_casing(df, field)
            df = tf.validate_against_list(df, field)
        df = tf.select_columns(df, output)
        if output.get('keep_source_file_name'):
            output_file_name = get_output_file_name(config, output)
            create_output_file(output_file_name, df)


if __name__ == "__main__":
    main()