import polars as pl
from icecream import ic
import yaml
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
        df = df.sort("row_number", descending=False).drop("row_number")
        write_path = "output.csv"
        df.write_csv(write_path)
        #show the last 5 rows
        ic(df)
    else: 
        raise ValueError(f"{field_name} not found in the DataFrame.")
    #ic(df)
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
    ic(df)
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
        ic(f"will replace {from_string} with {to_string} in {field_name}")
        df = df.with_columns(pl.col(field_name).str.replace_all(from_string, to_string))
        ic(df)
    return df

def fix_casing(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    source = field.get("source_name")
    alias = field.get("alias")
    casing = field.get("casing")

    field_name = alias if alias else source

    if casing == "upper":
        ic(f"will convert {field_name} to uppercase.")
        df = df.with_columns(pl.col(field_name).str.to_uppercase())
    elif casing == "lower":
        df = df.with_columns(pl.col(field_name).str.to_lowercase())
    elif casing == "proper":
        df = df.with_columns(pl.col(field_name).str.to_titlecase())
    else:
        ic(f"{casing} not supported.")
    ic(df)
    return df

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
    df = df.with_columns(pl.col(field_name).apply(lambda x: f'****{x}****' if x not in list_yaml else x))
    ic(df)
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
    ic(df)
    return df

def create_new_fields(df: pl.DataFrame, field: dict) -> pl.DataFrame:
    alias = field.get("alias")
    default_value = field.get("default_value")


    if not alias:
        ic(f"{alias} not found in the field.")
        return df
    if alias in df.columns:
        ic(f"{alias} already exists in the DataFrame.")
        return df
    ic(f"will create {alias} with default value {default_value}")
    df = df.with_columns(pl.Series([default_value]).alias(alias))
    return df