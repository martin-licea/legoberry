import polars as pl
from icecream import ic
from utils import find_data_files, get_config, create_output_file, get_output_file_name, read_data_files
import transformations as tf
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
logger = logging.getLogger(__name__)


def main():
    config = get_config()
    source_df, source_files= read_data_files(config)
    logger.info(source_files)
    temp_target_file = sorted([x.get('name').name for x in source_files], reverse=True)[0]
    logger.info(temp_target_file)
    source_df = tf.fix_nested_fields(config, source_df, temp_target_file)
    output_file_configs = config.get("output_file_configs", [])
    if not output_file_configs:
        raise ValueError("No output file configurations found in the config file.")
    for output in output_file_configs:
        df = source_df.clone()
        for field in output.get('fields'):
            df = tf.rename_columns(df, field)
            df = tf.create_new_fields(df, field)
            # df = tf.replace_strings_with_file(df, field, replace_with_file)
            df = tf.replace_strings(df, field)
            df = tf.fix_casing(df, field)
            df = tf.validate_against_list(df, field)
            df = tf.truncate_max_length(df, field)
            df = tf.format_fields(df, field)
            df = tf.create_record_off_field(df, field)
            df = tf.drop_nulls(df, field)
            df = tf.drop_if_length_less_than(df, field)
        df = tf.select_columns(df, output)
        df = tf.drop_duplicates(df, output)
        output_file_name = get_output_file_name(config, output, temp_target_file)
        column_formats = tf.get_excel_formats(output)
        df_drop = df.filter(pl.col("legoberry_drop_field_indicator") == True)
        #logger.info(df_drop)
        df = df.filter((pl.col("legoberry_drop_field_indicator") == False) | pl.col("legoberry_drop_field_indicator").is_null())
        #logger.info(df)
        #create df where drop_indicator is false
        select_columns = [x for x in df.columns if x.startswith("legoberry") == False]
        df = df.select(select_columns)
        create_output_file(output_file_name, df, column_formats)
        #delete dropped fields file if exists
        dropped_fields_file = f"dropped_fields{output.get('output_file_suffix')}.csv"
        if os.path.exists(dropped_fields_file):
            os.remove(dropped_fields_file)
        if df_drop.shape[0] > 0:
            df_drop.write_csv(dropped_fields_file)
def finalizer():
    while True:
            will_exit= input("Press e to exit:")
            if will_exit.lower() == 'e':
                sys.exit(0)

if __name__ == "__main__":
    if getattr(sys, 'frozen', False):
        application_path = os.path.dirname(sys.executable)
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(application_path)
    cwd = os.getcwd()

    try:
        main()
    except Exception as e:
        logger.error(e)
        logger.info('Something is likely wrong in the source file. Check the log a few lines above this to see if there is an indication of the issue.')
        finalizer()
    finally:
        finalizer()