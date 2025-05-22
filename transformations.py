import polars as pl
from icecream import ic
import yaml
import re
from utils import clean_up_drop_fields, create_output_file
import logging 
from pathlib import Path
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s')
logger = logging.getLogger(__name__)
from pathlib import Path
ic.configureOutput(contextAbsPath=True)

def do_explore(conifg, df):
    pass

def fix_nested_fields(config: dict, df: pl.DataFrame, target_file) -> pl.DataFrame:
    target_file = Path(target_file)
    csv_file_name = target_file.stem + ".csv"
    fields_in_field = config.get("fields_in_field", [])
    for explode in fields_in_field:
            delimiter = explode.get("delimiter", "|")
            splitter = explode.get("key_value_splitter", ":")
            field_name = explode.get("field_name", "Custom Fields")
            ignore_junk_field_string = explode.get("ignore_junk_field_string", ["n"])
            df = explode_custom_fields(df, field_name, delimiter, splitter, ignore_junk_field_string)
    if len(fields_in_field) > 0:
        create_output_file(csv_file_name, df)
    return df


def explode_custom_fields(df: pl.DataFrame, on_field: str, delimiter: str, splitter: str, ignore_junk_fields: list = ['n']) -> pl.DataFrame:
    df = df.with_columns(pl.arange(0, df.height).cast(pl.datatypes.Utf8).alias("row_number"))
    if on_field in df.columns:
        # Split the "Custom Fields" column by '|'
        custom_fields = df[on_field].map_elements(lambda x: x.split(delimiter) if isinstance(x, str) else x, return_dtype=pl.datatypes.List)
        #remove field if field is 'n'
        for ignore in ignore_junk_fields:
            custom_fields = custom_fields.map_elements(lambda x: [field for field in x if field != ignore], return_dtype=pl.datatypes.List)
        logger.info(custom_fields)
        # For each split column
        row_number = 0
        for field in custom_fields:
            # Split the column by ':'
            split_column = field.map_elements(lambda x: x.split(splitter), return_dtype=pl.datatypes.List)
            field_name = split_column.map_elements(lambda x: x[0].strip() if len(x) > 1 else "", return_dtype=pl.datatypes.Utf8)
            field_value = split_column.map_elements(lambda x: x[1].strip() if len(x) > 1 else "", return_dtype=pl.datatypes.Utf8)
            field_name = field_name.append(pl.Series(["row_number"]))
            field_value = field_value.append(pl.Series([str(row_number)]))
            if "df_exploded_fields" not in locals():
                df_exploded_fields = pl.DataFrame({field_name[i]: [field_value[i]] for i in range(len(field_name))})
            else:
                tmp = pl.DataFrame({field_name[i]: [field_value[i]] for i in range(len(field_name))})
                df_exploded_fields = pl.concat([df_exploded_fields, tmp], how='align')
            row_number += 1
        # Merge the exploded fields with the original DataFrame on the row number
        df = df.join(df_exploded_fields, on="row_number", how="inner")
        # Drop the "Custom Fields" column
        df = df.drop(on_field)
        # order by row number asc and drop row number
        df = df.sort(pl.col("row_number").cast(pl.Int64))#.drop("row_number")
        # write_path = "output.csv"
        # df.write_csv(write_path)
        #show the last 5 rows
        logger.info("printing with row_number")
        
    else: 
        raise ValueError(f"{field_name} not found in the DataFrame.")
    #
    return df

def rename_columns(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    alias = field.get("alias")
    source = field.get("source_name")

    if not alias or not source:
        return df
    if source not in df.columns:
        logger.debug(f"{source} not found in the DataFrame. Will not rename")
        return df
    
    #rename source field to alias
    logger.info(f"will rename {source} to {alias}")
    df = df.rename({source: alias})
    
    return df

# def replace_strings_with_file(df: pl.DataFrame, field: dict) -> pl.DataFrame:
#     with open(filename) as f:
#         data = yaml.safe_load(f)
#     field["replace"] = data
#     return replace_strings(df, field)

def replace_strings(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    replace = field.get("replace", [])
    replace_filename = field.get("replace_with_file")
    
    if (not source and not alias) or not (replace or replace_filename):
        logger.debug(field)
        logger.debug("source or replace not found in the field.")
        return df
    if alias and alias not in df.columns:
        logger.debug(f"{alias} not found in the DataFrame.")
        return df
    
    if not alias and source not in df.columns:
        logger.info(f"{source} not found in the DataFrame.")
        return df
    if replace_filename:
        with open(replace_filename) as f:
            data = yaml.safe_load(f)
        replace = replace + data
    if alias:
        field_name = alias
    else:
        field_name = source 

    for item in replace:
        from_string=item.get("from")
        to_string=item.get("to")
        is_expression = item.get("is_expression")
        is_literal = False if is_expression == True else True #flip it for ease of use in the config
        logger.info(f"setting is_literal to {is_literal} for {from_string} to {to_string} in {field_name}")
        logger.info(rf"will replace {from_string} with {to_string} in {field_name}")
        df = df.with_columns(pl.col(field_name).str.replace_all(from_string, to_string, literal=is_literal))
        #df = df.with_columns(pl.col(field_name).str.replace_all("[Dd]r($| )", "Dr.$1", literal=False))
        
    return df

def fix_casing(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    casing = field.get("casing")
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    if not casing:
        logger.debug(f"casing not required for {field_name}.")
        return df
    
    if casing == "upper":
        logger.info(f"will convert {field_name} to uppercase.")
        df = df.with_columns(pl.col(field_name).str.to_uppercase())
    elif casing == "lower":
        df = df.with_columns(pl.col(field_name).str.to_lowercase())
    elif casing == "proper":
        df = df.with_columns(pl.col(field_name).str.to_titlecase())
    elif casing == "name":
        #ex) john doe -> John Doe
        df = df.with_columns(pl.col(field_name).str.to_titlecase())
        #find Mc or Mac and capitalize the next letter
        df = df.with_columns(pl.col(field_name).apply(lambda value: re.sub(r"(Mc)(\w)", _name_replacement, value)))
        #capitalize letter after ' and -
        df = df.with_columns(pl.col(field_name).apply(lambda value: re.sub(r"('|\-)(\w)", _name_replacement, value)))
        #if the field starts with a single letter followed by a space, and the length of the field is more than 2, remove the first letter and the space
        df = df.with_columns(pl.when(pl.col(field_name).str.len_chars() > 2).then(
            pl.col(field_name).str.replace_all(r"(^\w\s)", "$2")
        ).otherwise(
            pl.col(field_name)
        ))
    else:    
        logger.info(f"{casing} not supported.")
    
    return df
def _name_replacement(match):
    return match.group(1) + match.group(2).capitalize()

def validate_against_list(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    in_list = field.get("in_list")

    field_name = alias if alias else source

    if not in_list:
        logger.debug(f"in_list not found in the {field_name}. will skip validation.")
        return df
    with open(in_list) as f:
        list_yaml = yaml.safe_load(f)

    logger.info(f"will validate {field_name} against {in_list}")
    df = df.with_columns(pl.col(field_name).apply(lambda x: f'%%%%{x}%%%%' if x not in list_yaml else x))
    
    return df
def remove_duplicates_from_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    """
    Remove duplicate substrings from a field based on values in other fields.
    If any duplicates are removed, wrap the result with %%%% markers.
    """
    remove_fields = field.get("remove_duplicates_from_fields", [])
    if not remove_fields:
        return df
    alias = field.get("alias")
    source = field.get("source_name")
    field_name = alias if alias else source
    if field_name not in df.columns:
        return df
    valid_remove_fields = [f for f in remove_fields if f in df.columns]
    if not valid_remove_fields:
        return df
    def _remove_duplicates(row):
        addr = row[field_name]
        if not isinstance(addr, str):
            return addr
        modified = False
        # Remove each duplicate value along with surrounding commas/spaces
        for dup in valid_remove_fields:
            val = row[dup]
            if val and isinstance(val, str) and re.search(re.escape(val), addr, flags=re.IGNORECASE):
                pattern = rf"[\s,\.]*(?:\b{re.escape(val)}\b)[\s,\.]*"
                new_addr, count = re.subn(pattern, " ", addr, flags=re.IGNORECASE)
                if count and count > 0:
                    addr = new_addr
                    modified = True
        if modified:
            addr = re.sub(r"\s+", " ", addr).strip(", .")
            return f"%%%%{addr}%%%%"
        return addr
    df = df.with_columns(
        pl.struct([field_name] + valid_remove_fields)
          .apply(_remove_duplicates)
          .alias(field_name)
    )
    return df

def select_columns(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    select_fields = field.get("ordered_headers")
    select_fields.extend(["legoberry_drop_field_indicator", "legoberry_reason_for_drop"])
    logger.info(select_fields)
    if not select_fields:
        logger.info(f"{select_fields} not found. Will pass all fields")
        return df
    logger.info(f"will select {select_fields} from the DataFrame.")
    #create field if not found
    for field in select_fields:
        if field not in df.columns:
            df = df.with_columns(pl.Series([None]).alias(field))
    df = df.select(select_fields)
    
    return df

def create_new_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    alias = field.get("alias")
    field_name = alias if alias else field.get("source_name")
    default_value = field.get("default_value")
    source_fields_alpha_only = field.get("source_fields_alpha_only")
    allow_special_char_list = field.get("allow_special_char_list", [])
    if not alias:
        logger.info(f"alias not found for {field_name}. Will skip creating new field.")
        return df
    if alias in df.columns:
        logger.info(f"{alias} already exists in the DataFrame. Will not alias a new created field")
        return df
    if not default_value:
        logger.debug(f"{default_value} not found in the field.")
        return df

    logger.info(f"will create {alias} with default value {default_value}")
    if  "{" in default_value and "}" in default_value: 
        split_values = re.split(r'[{}]', default_value)
        allowed_chars = ''.join(re.escape(char) for char in allow_special_char_list)
        if source_fields_alpha_only:
            replace = rf'[^a-zA-Z{allowed_chars}]'
            #if p.col add replace_all function
            split_values = [f"pl.col('{value.strip()}').str.replace_all('{replace}', '')" if i % 2 == 1 else f"'{value}'" for i, value in enumerate(split_values) if value.strip()]        # Join the list back into a string
        else:
            split_values = [f"pl.col('{value.strip()}')" if i % 2 == 1 else f"'{value}'" for i, value in enumerate(split_values) if value.strip()]        # Join the list back into a string
        default_value = "+".join(split_values)
        logger.info(default_value)
        df = df.with_columns(eval(default_value).alias(alias))
        return df
    df = df.with_columns(pl.Series([default_value]).alias(alias))
    return df

def format_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    data_type = field.get("data_type")
    data_format = field.get("data_format")
    reformat_to = field.get("reformat_to")
    if not data_type:
        logger.info(f'data_type not found in the "{field_name}" field. Will skip formatting.')
        return df
    failed_records = pl.DataFrame()
    data_type = data_type.lower()

    if data_type == "date":
        if not data_format:
            logger.info(f'date not found in the "{field_name}" field. Will skip formatting to date.')
            return df
        parsed_date = pl.col(field_name).str.to_date(data_format, strict=False)
        if reformat_to:
            parsed = parsed_date.dt.strftime(reformat_to)
        else:
            parsed = parsed_date
        df = df.with_columns(
            pl.when(pl.col(field_name) == "")
            .then(None)
            .when(parsed.is_not_null())
            .then(parsed)
            .otherwise("%%%%" + pl.col(field_name) + "%%%%")
            .alias(field_name)
        )
    elif data_type == "integer":
        logger.info(f"will convert {field_name} to integer.")
        #try to convert to integer, if fails, add *** to the field and keep it as string
        df = df.with_columns(
            pl.when(pl.col(field_name).str.contains(r'^[0-9]+$'))
            .then(pl.col(field_name))
            .otherwise(pl.lit(None))
            .alias(field_name)
        )

        df = df.with_columns(pl.col(field_name).cast(pl.Int64))
        
    elif data_type == "number":
        logger.info(f"will convert {field_name} to number.")
        df = df.with_columns(pl.col(field_name).cast(pl.Float64))
    elif data_type == "string":
        logger.info(f"will convert {field_name} to string.")
        df = df.with_columns(pl.col(field_name).cast(pl.Utf8))
    elif data_type == "boolean":
        logger.info(f"will convert {field_name} to boolean.")
        df = df.with_columns(pl.col(field_name).cast(pl.Boolean))
    elif data_type == "phone number":
        logger.info(f"will convert {field_name} to phone number.")
        #ex) (123) 456-7890 or 123-456-7890 or 123.456.7890 or 1234567890 -> (123) 456-7890
        df = df.with_columns(pl.col(field_name).str.replace_all(r"[^\d]", ""))
        # df = df.with_columns(
        #     "(" + pl.col(field_name).str.slice(0, 3) + ") " 
        #     + pl.col(field_name).str.slice(3, 3) 
        #     + "-" + pl.col(field_name).str.slice(6, 4)
        #     .alias(field_name))
        df = _format_phone_numbers(df, field_name)

    elif data_type == "zip code":
        logger.info(f"will convert {field_name} to zip code.")
        #ex) 12345-6789 or 12345 -> 12345
        #ex) 2345 -> 02345
        df = df.with_columns(pl.col(field_name).cast(pl.Utf8))
        condition = pl.col(field_name).str.len_chars() == 4
        warning_condition = pl.col(field_name).str.len_chars() < 4
        formatted = pl.when(condition).then(
            pl.col(field_name).str.zfill(5)
        ).when(warning_condition).then(
            "%%%%" + pl.col(field_name) + "%%%%"
        ).otherwise(
            pl.col(field_name).str.slice(0, 5)
        )
        df = df.with_columns(formatted.alias(field_name))

                             
    elif data_type == "email":
        logger.info(f"will convert {field_name} to email.")
        regex_valid_email = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        df = df.with_columns(
            pl.when(pl.col(field_name).str.contains(regex_valid_email, literal=False) & pl.col(field_name).is_not_null())
            .then(pl.col(field_name)).when(pl.col(field_name) == "").then(None)
            .otherwise("%%%%" + pl.col(field_name) + "%%%%")
        )
    else:
        logger.info(f"{data_type} not supported.")
    
    return df

def _format_phone_numbers(df, field_name):
    old_field_name = f"{field_name}_old"
    df = df.rename({field_name: old_field_name})
    # Apply formatting if the condition is met
    condition = pl.col(old_field_name).str.len_chars() == 10
    # if phone number is 11 digits long and starts with 1, remove 1
    eleven_digits = pl.col(old_field_name).str.len_chars() == 11
    formatted = pl.when(condition).then(
        "(" + pl.col(old_field_name).str.slice(0, 3) + 
        ") " + pl.col(old_field_name).str.slice(3, 3) + 
        "-" + pl.col(old_field_name).str.slice(6, 4)
    ).when(eleven_digits).then(
        "(" + pl.col(old_field_name).str.slice(1, 3) + 
        ") " + pl.col(old_field_name).str.slice(4, 3) + 
        "-" + pl.col(old_field_name).str.slice(7, 4)
    ).otherwise(
        "%%%%" + pl.col(old_field_name) + "%%%%" # Return unchanged if not exactly 10 digits
    )
    logger.info(field_name)
    return df.with_columns(formatted.alias(field_name))
    
def get_excel_formats(output_config: dict) -> dict:
    column_formats = {}
    for field in output_config.get("fields"):
        alias = field.get("alias")
        source = field.get("source_name")
        field_name = alias if alias else source
        format = field.get("excel_format")
        if format:
            column_formats[field_name] = format
    return column_formats

def drop_duplicates(df: pl.DataFrame, config: dict) -> pl.DataFrame:
    fields_list = config.get("fields_to_consider_duplicates")
    if not fields_list:
        logger.info(f"{fields_list} not found in the config. No fields to consider for duplicates.")
        return df
    logger.info(f"will drop duplicates based on {fields_list}")
    df = df.unique(subset=fields_list)
    return df

def truncate_max_length(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    max_length = field.get("max_length")

    if not max_length:
        logger.debug(f"Will not format truncate {field_name}.")
        return df
    logger.info(f"will truncate {field_name} to {max_length}")
    #chceck data type of the field
    if df[field_name].dtype == pl.datatypes.Utf8:
        df = df.with_columns(pl.col(field_name).str.slice(0, max_length))
    else:
        logger.info(f"{field_name} is not of type Utf8.")
        data_type = df[field_name].dtype
        df = df.with_columns(pl.col(field_name).cast(pl.datatypes.Utf8).str.slice(0, max_length))
        df = df.with_columns(pl.col(field_name).cast(data_type))
    
    return df

def create_record_off_field(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    field_name = field.get("source_name")
    expand_on = field.get("expand_on")

    if not expand_on:
        logger.debug(f"Will not format expand on {field_name or field.get('alias')}.")
        return df
    logger.info(f"will create record off {field_name} on {expand_on}")
    df_alt = df.clone()
    #remove row if field is empty or null
    df_alt = df_alt.filter(pl.col(field_name).is_not_null())
    df_alt = df_alt.drop([expand_on])
    df_alt = df_alt.with_columns(pl.col(field_name).alias(expand_on))
    df = pl.concat([df, df_alt], how='align')

    return df

def drop_nulls(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    drop_full_row_if_empty = field.get("drop_full_row_if_empty")
    if not drop_full_row_if_empty:
        logger.debug(f"Will not format drop full row if {field.get('source_name') or field.get('alias')} is null.")
        return df
    source = field.get("source_name")
    alias = field.get("expand_on") or field.get("alias")
    field_name = alias if alias else source
    logger.info(f"will drop nulls from {field_name}")
    #set legoberry_drop_field_indicator to true if field is null
    if "legoberry_drop_field_indicator" in df.columns:
        #add to existing drop field indicator if exists
        df = df.with_columns(pl.when((pl.col(field_name).is_null()) | (pl.col(field_name) == "")).then(
            True
        ).otherwise(
            pl.col("legoberry_drop_field_indicator")
        ).alias("legoberry_drop_field_indicator"))
    else:
        df = df.with_columns(pl.when((pl.col(field_name).is_null()) | (pl.col(field_name) == "")).then(
            True
        ).otherwise(False).alias("legoberry_drop_field_indicator"))

    if "legoberry_reason_for_drop" in df.columns:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True & pl.col("legoberry_reason_for_drop").is_null()).then(
            pl.lit(f"{source} is Null")
        ).otherwise(
            pl.col("legoberry_reason_for_drop")
        ).alias("legoberry_reason_for_drop"))
    else:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True).then(
            pl.lit(f"{source} is Null")
        ).alias("legoberry_reason_for_drop"))


    #df = df.filter(pl.col(field_name).is_not_null())
    #df = df.filter(pl.col(field_name).str.contains("%%%%%%%%", literal=True).is_not())
    return df

def drop_if_length_less_than(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    drop_if_length_less_than=field.get("drop_if_length_less_than")
    if not drop_if_length_less_than:
        logger.debug(f"Will not format check for length of {field.get('source_name')}.")
        return df
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    logger.info(f"will drop if length of {field_name} is less than {drop_if_length_less_than}")
    if "legoberry_drop_field_indicator" in df.columns:
        df = df.with_columns(
            pl.when(pl.col(field_name).str.len_chars() < drop_if_length_less_than).then(
                True
            ).otherwise(
                pl.col("legoberry_drop_field_indicator")
            ).alias("legoberry_drop_field_indicator")
        )
    else:
        df = df.with_columns(
            pl.when(pl.col(field_name).str.len_chars() < drop_if_length_less_than).then(
                True
            ).otherwise(False).alias("legoberry_drop_field_indicator")
        )

    if "legoberry_reason_for_drop" in df.columns:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True & pl.col("legoberry_reason_for_drop").is_null()).then(
            pl.lit(f"Length of {field_name} is less than {drop_if_length_less_than}")
        ).otherwise(
            pl.col("legoberry_reason_for_drop")
        ).alias("legoberry_reason_for_drop"))
    else:
        df = df.with_columns(pl.when(pl.col("legoberry_drop_field_indicator") == True).then(
            pl.lit(f"Length of {field_name} is less than {drop_if_length_less_than}")
        ).alias("legoberry_reason_for_drop"))
    
    return df