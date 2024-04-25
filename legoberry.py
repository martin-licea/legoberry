import polars as pl
from icecream import ic
from utils import find_data_files, get_config, create_output_file, get_output_file_name, read_data_files
import transformations as tf
import os
import sys
def main():
    config = get_config()
    source_df= read_data_files(config)
    source_df = tf.fix_nested_fields(config, source_df)
    output_file_configs = config.get("output_file_configs", [])
    if not output_file_configs:
        raise ValueError("No output file configurations found in the config file.")
    for output in output_file_configs:
        df = source_df.clone()
        for field in output.get('fields'):
            df = tf.drop_nulls(df, field)
            df = tf.drop_if_length_less_than(df, field)
            df = tf.rename_columns(df, field)
            df = tf.create_new_fields(df, field)
            df = tf.replace_strings(df, field)
            df = tf.fix_casing(df, field)
            df = tf.validate_against_list(df, field)
            df = tf.truncate_max_length(df, field)
            df = tf.format_fields(df, field)
            df = tf.create_record_off_field(df, field)
        df = tf.select_columns(df, output)
        df = tf.drop_duplicates(df, output)
        output_file_name = get_output_file_name(config, output,)
        column_formats = tf.get_excel_formats(output)
        create_output_file(output_file_name, df, column_formats)


if __name__ == "__main__":
    if getattr(sys, 'frozen', False):
        application_path = os.path.dirname(sys.executable)
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(application_path)
    cwd = os.getcwd()

    main()