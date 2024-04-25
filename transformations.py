import polars as pl
from icecream import ic
import yaml
import re

ic.configureOutput(contextAbsPath=True)

def do_explore(conifg, df):
    pass

def fix_nested_fields(config: dict, df: pl.DataFrame) -> pl.DataFrame:
    for explode in config.get("fields_in_field", []):
            delimiter = explode.get("delimiter", "|")
            splitter = explode.get("key_value_splitter", ":")
            field_name = explode.get("field_name", "Custom Fields")
            ignore_junk_field_string = explode.get("ignore_junk_field_string", ["n"])
            df = explode_custom_fields(df, field_name, delimiter, splitter, ignore_junk_field_string)
    return df


def explode_custom_fields(df: pl.DataFrame, on_field: str, delimiter: str, splitter: str, ignore_junk_fields: list = ['n']) -> pl.DataFrame:
    df = df.with_columns(pl.arange(0, df.height).cast(pl.datatypes.Utf8).alias("row_number"))
    if on_field in df.columns:
        # Split the "Custom Fields" column by '|'
        custom_fields = df[on_field].apply(lambda x: x.split(delimiter) if isinstance(x, str) else x, return_dtype=pl.datatypes.List)
        #remove field if field is 'n'
        for ignore in ignore_junk_fields:
            custom_fields = custom_fields.apply(lambda x: [field for field in x if field != ignore], return_dtype=pl.datatypes.List)
        ic(custom_fields)
        # For each split column
        row_number = 0
        for field in custom_fields:
            # Split the column by ':'
            split_column = field.apply(lambda x: x.split(splitter), return_dtype=pl.datatypes.List)
            field_name = split_column.apply(lambda x: x[0].strip() if len(x) > 1 else "", return_dtype=pl.datatypes.Utf8)
            field_value = split_column.apply(lambda x: x[1].strip() if len(x) > 1 else "", return_dtype=pl.datatypes.Utf8)
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
        ic("printing with row_number")
        
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
        ic(f"{source} not found in the DataFrame.")
        return df
    
    #rename source field to alias
    ic(f"will rename {source} to {alias}")
    df = df.rename({source: alias})
    
    return df

def replace_strings(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    replace = field.get("replace")

    if (not source and not alias) or not replace:
        ic(field)
        ic("source or replace not found in the field.")
        return df
    if alias and alias not in df.columns:
        ic(f"{alias} not found in the DataFrame.")
        return df
    
    if not alias and source not in df.columns:
        ic(f"{source} not found in the DataFrame.")
        return df
    if alias:
        field_name = alias
    else:
        field_name = source 

    for item in replace:
        from_string=item.get("from")
        to_string=item.get("to")
        is_expression = item.get("is_expression")
        ic(is_expression)
        is_literal = False if is_expression == True else True #flip it for ease of use in the config
        ic(is_literal)
        ic(rf"will replace {from_string} with {to_string} in {field_name}")
        df = df.with_columns(pl.col(field_name).str.replace_all(from_string, to_string, literal=is_literal))
        #df = df.with_columns(pl.col(field_name).str.replace_all("[Dd]r($| )", "Dr.$1", literal=False))
        
    return df

def fix_casing(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    casing = field.get("casing")
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    if not casing:
        ic(f"{casing} not required for {field_name}.")
        return df
    
    if casing == "upper":
        ic(f"will convert {field_name} to uppercase.")
        df = df.with_columns(pl.col(field_name).str.to_uppercase())
    elif casing == "lower":
        df = df.with_columns(pl.col(field_name).str.to_lowercase())
    elif casing == "proper":
        df = df.with_columns(pl.col(field_name).str.to_titlecase())
    elif casing == "name":
        #ex) john doe -> John Doe
        df = df.with_columns(pl.col(field_name).str.to_titlecase())
        #find Mc or Mac and capitalize the next letter
        df = df.with_columns(pl.col(field_name).apply(lambda value: re.sub(r"(Mc|Mac)(\w)", _name_replacement, value)))
        #capitalize letter after ' and -
        df = df.with_columns(pl.col(field_name).apply(lambda value: re.sub(r"('|\-)(\w)", _name_replacement, value)))
    else:
        ic(f"{casing} not supported.")
    
    return df
def _name_replacement(match):
    return match.group(1) + match.group(2).capitalize()

def validate_against_list(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    in_list = field.get("in_list")

    field_name = alias if alias else source

    if not in_list:
        ic(f"{field_name} not found in the field.")
        return df
    with open(in_list) as f:
        list_yaml = yaml.safe_load(f)

    ic(f"will validate {field_name} against {in_list}")
    df = df.with_columns(pl.col(field_name).apply(lambda x: f'%%%%{x}%%%%' if x not in list_yaml else x))
    
    return df

def select_columns(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    select_fields = field.get("ordered_headers")

    if not select_fields:
        ic(f"{select_fields} not found. Will pass all fields")
        return df
    ic(f"will select {select_fields} from the DataFrame.")
    #create field if not found
    for field in select_fields:
        if field not in df.columns:
            df = df.with_columns(pl.Series([None]).alias(field))
    df = df.select(select_fields)
    
    return df

def create_new_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    alias = field.get("alias")
    default_value = field.get("default_value")
    source_fields_alpha_only = field.get("source_fields_alpha_only")
    if not alias:
        ic(f"{alias} not found in the field.")
        return df
    if alias in df.columns:
        ic(f"{alias} already exists in the DataFrame.")
        return df
    if not default_value:
        ic(f"{default_value} not found in the field.")
        return df

    ic(f"will create {alias} with default value {default_value}")
    if  "{" in default_value and "}" in default_value: 
        split_values = re.split(r'[{}]', default_value)
        if source_fields_alpha_only:
            replace = r'[^a-zA-Z]'
            #if p.col add replace_all function
            split_values = [f"pl.col('{value.strip()}').str.replace_all('{replace}', '')" if i % 2 == 1 else f"'{value}'" for i, value in enumerate(split_values) if value.strip()]        # Join the list back into a string
        else:
            split_values = [f"pl.col('{value.strip()}')" if i % 2 == 1 else f"'{value}'" for i, value in enumerate(split_values) if value.strip()]        # Join the list back into a string
        default_value = "+".join(split_values)
        ic(default_value)
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
        ic(f"{data_type} not found in the field.")
        return df
    
    data_type = data_type.lower()

    if data_type == "date":
        if not data_format:
            ic(f"{data_format} not found in the field.")
            return df
        ic(f"will format {field_name} as date with format {data_format}")
        #if date conversion fails, add *** to the field and keep it as string
        df = df.with_columns(pl.col(field_name).str.to_date(data_format))
        #df = df.with_columns(pl.col(field_name).apply(lambda x: x if not x else f'***{x}'))
        if reformat_to:
            ic(f"will reformat {field_name} to {reformat_to}")
            df = df.with_columns(pl.col(field_name).dt.strftime(reformat_to))
        else:
            ic(f"reformat_to not found in the field.")
    elif data_type == "integer":
        ic(f"will convert {field_name} to integer.")
        df = df.with_columns(pl.col(field_name).cast(pl.Int64))
    elif data_type == "number":
        ic(f"will convert {field_name} to number.")
        df = df.with_columns(pl.col(field_name).cast(pl.Float64))
    elif data_type == "string":
        ic(f"will convert {field_name} to string.")
        df = df.with_columns(pl.col(field_name).cast(pl.Utf8))
    elif data_type == "boolean":
        ic(f"will convert {field_name} to boolean.")
        df = df.with_columns(pl.col(field_name).cast(pl.Boolean))
    elif data_type == "phone number":
        ic(f"will convert {field_name} to phone number.")
        #ex) (123) 456-7890 or 123-456-7890 or 123.456.7890 or 1234567890 -> (123) 456-7890
        df = df.with_columns(pl.col(field_name).str.replace_all(r"[^\d]", ""))
        # df = df.with_columns(
        #     "(" + pl.col(field_name).str.slice(0, 3) + ") " 
        #     + pl.col(field_name).str.slice(3, 3) 
        #     + "-" + pl.col(field_name).str.slice(6, 4)
        #     .alias(field_name))
        df = _format_phone_numbers(df, field_name)

    elif data_type == "zip code":
        ic(f"will convert {field_name} to zip code.")
        #ex) 12345-6789 or 12345 -> 12345
        #ex) 2345 -> 02345
        df = df.with_columns(pl.col(field_name).cast(pl.Utf8))
        condition = pl.col(field_name).str.lengths() == 4
        warning_condition = pl.col(field_name).str.lengths() < 4
        formatted = pl.when(condition).then(
            pl.col(field_name).str.zfill(5)
        ).when(warning_condition).then(
            "%%%%" + pl.col(field_name) + "%%%%"
        ).otherwise(
            pl.col(field_name).str.slice(0, 5)
        )
        df = df.with_columns(formatted.alias(field_name))

                             
    elif data_type == "email":
        ic(f"will convert {field_name} to email.")
        regex_valid_email = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        df = df.with_columns(
            pl.when(pl.col(field_name).str.contains(regex_valid_email, literal=False))
            .then(pl.col(field_name))
            .otherwise("%%%%" + pl.col(field_name) + "%%%%")
        )
    else:
        ic(f"{data_type} not supported.")
    
    return df

def _format_phone_numbers(df, field_name):
    old_field_name = f"{field_name}_old"
    df = df.rename({field_name: old_field_name})
    # Apply formatting if the condition is met
    condition = pl.col(old_field_name).str.lengths() == 10
    #if phone number is 11 digits long and starts with 1, remove 1
    eleven_digits = pl.col(old_field_name).str.lengths() == 11
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
    ic(field_name)
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
        ic(f"{fields_list} not found in the config. No fields to consider for duplicates.")
        return df
    ic(f"will drop duplicates based on {fields_list}")
    df = df.unique(subset=fields_list)
    return df

def truncate_max_length(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    max_length = field.get("max_length")

    if not max_length:
        ic(f"{max_length} not found in the field.")
        return df
    ic(f"will truncate {field_name} to {max_length}")
    #chceck data type of the field
    if df[field_name].dtype == pl.datatypes.Utf8:
        df = df.with_columns(pl.col(field_name).str.slice(0, max_length))
    else:
        ic(f"{field_name} is not of type Utf8.")
        data_type = df[field_name].dtype
        df = df.with_columns(pl.col(field_name).cast(pl.datatypes.Utf8).str.slice(0, max_length))
        df = df.with_columns(pl.col(field_name).cast(data_type))
    
    return df

def create_record_off_field(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    field_name = field.get("source_name")
    expand_on = field.get("expand_on")

    if not expand_on:
        ic(f"{expand_on} not found in the field. nothing to expand on")
        return df
    ic(f"will create record off {field_name} on {expand_on}")
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
        ic(f"{drop_full_row_if_empty} not found in the field.")
        return df
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    ic(f"will drop nulls from {field_name}")

    df = df.filter(pl.col(field_name).is_not_null())
    df = df.filter(pl.col(field_name).str.contains("%%%%%%%%", literal=True).is_not())
    return df

def drop_if_length_less_than(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    drop_if_length_less_than=field.get("drop_if_length_less_than")
    if not drop_if_length_less_than:
        ic(f"{drop_if_length_less_than} not found in the field.")
        return df
    source = field.get("source_name")
    alias = field.get("alias")
    field_name = alias if alias else source
    
    ic(f"will drop if length of {field_name} is less than {drop_if_length_less_than}")
    df = df.filter(pl.col(field_name).str.lengths() >= drop_if_length_less_than)
    return df