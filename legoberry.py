import os
import sys
import glob
import pandas as pd
import logging
from nested_lookup import nested_lookup
import yaml
from pathlib import Path

def main():
    #os.chdir('..')
    #set the current working directory to the parent directory of the script
    #os.chdir(os.path.dirname(os.path.realpath(__file__)))
    if getattr(sys, 'frozen', False):
    # If the application is run as a bundle, the PyInstaller bootloader
    # extends the sys module by a flag frozen=True and sets the app 
    # path into variable _MEIPASS'.
        application_path = os.path.dirname(sys.executable)
    else:
        application_path = os.path.dirname(os.path.abspath(__file__))
    os.chdir(application_path)
    cwd = os.getcwd()
    print(cwd)
    conf = get_configs()
    #print(conf)
    clean_previous_runs(conf['target_file'], conf['log'])
    #csv or xlsx glob files
    legos = glob.glob('*.csv')
    legos += glob.glob('*.xlsx')
    school_type = conf.get('type')
    ordered_headers = conf['ordered_headers']
    target_headers = nested_lookup('target_name', conf)
    #target_headers = list(target_headers)
    target_headers = list(set(target_headers)) # remove dupes
    lego_tower = pd.DataFrame(columns=ordered_headers)
    for lego in legos:
        if Path(lego).suffix == '.csv':
            lego_data = pd.read_csv(lego)
        elif Path(lego).suffix == '.xlsx':
            lego_data = pd.read_excel(lego)
        lego_data['filename'] = Path(lego).stem
        #find transformations
        lego_data = find_transformations(conf, lego_data)
        lego_tower = pd.concat([lego_tower,lego_data], axis=0, ignore_index=True)

    #print(lego_tower)
    lego_tower_size = len(lego_tower)
    i = 1

    while lego_tower_size > 0:
        name, extension = conf['target_file'].split('.')
        file_name = f'{name}_{i}.{extension}'
        keep_size = conf['max_target_file_size'] if lego_tower_size > conf['max_target_file_size'] else lego_tower_size
        mini_lego_tower = lego_tower[0:keep_size]
        lego_tower = lego_tower[keep_size:]
        #print(mini_lego_tower)
        df = mini_lego_tower[ordered_headers].copy()
        if conf.get('remove_duplicates', None):
            if conf.get('fields_to_consider_duplicates', None):
                df = df.drop_duplicates(subset=conf['fields_to_consider_duplicates'])
            else:
                df = df.drop_duplicates()
        #if school_type != "ES":
            #df = df.assign(df['grade level']='MS')
            #df.loc[:, 'grade level'] = school_type
        if extension == 'xlsx':
            df.to_excel(file_name, index=False)
        if extension == 'csv':
            df.to_csv(file_name, index=False)
        lego_tower_size -= conf['max_target_file_size']
        i += 1

    with open(conf['log'], 'w+') as f: #TODO: write a logger and send to output file
        f.write(f'legoberry found a total of {len(legos)} files to aggregate')
        f.write(f"\nwriting a total of {i} file(s) at a max {conf['max_target_file_size']} data rows per")

def clean_previous_runs(target_file, log_file):
    name, extension = target_file.split('.')
    master_files = glob.glob(f'{name}*.{extension}')
    for master in master_files:
        os.remove(master)
    try:
        os.remove(log_file)
    except:
        print(f'will not remove {log_file}. it does not exist')


def find_transformations(conf: dict, lego_data):
    fields = conf['fields']
    rename_mapper = {}
    for field in fields:
        rename_mapper = {}
        if field.get('target_name', None) is None:
            field['target_name'] = field['source_name']
        split_fields = []
        if field.get('source_name', None) is None:
            field_name = "Unnamed: " + str(field.get('field_number'))
            field['source_name'] = field_name
        if field.get('split', None):
            new = lego_data[field['source_name']].str.split(field['split_by'], expand = True)
            for item in field['split']:
                index = item['index'] - 1
                lego_data[item['target_name']] = new[index]
                split_fields.append(item['target_name'])
        if field.get('replace', None):
            from_these = []
            to_these = []
            for item in field['replace']:
                from_these.append(item['from'])
                to_these.append(item['to'])
            if split_fields:
                for split_field in split_fields:
                    lego_data[split_field] = lego_data[split_field].replace(from_these, to_these)
            else:
                lego_data[field['source_name']] = lego_data[field['source_name']].replace(from_these, to_these)
        if field.get('expand_on') or field.get('split'):
            if field.get('split'):
                for split_field in field.get('split', []):
                    target_name = split_field.get('target_name')
                    expand_on = split_field.get('expand_on', None)
                    if expand_on:
                        lego_data = expand_on_df(lego_data, target_name, expand_on)
            else:
                expand_on_df(lego_data, field['target_name'], field['expand_on'])
                
        if field.get('default_value', None):
            lego_data[field['target_name']] = field['default_value']
        if field.get('target_name', None):
            if field['source_name'] in lego_data:
                rename_mapper[field['source_name']] = field['target_name']
            elif field.get('secondary_source_name', None) in lego_data:
                rename_mapper[field['secondary_source_name']] = field['target_name']
        
        if field.get('casing'):
            casing = field['casing']
            target_name = field.get('target_name')
            source_name = field.get('source_name')
            if split_fields:
                for split_field in split_fields:
                    target_name = split_field
                    lego_data = format_casing(lego_data, target_name, casing)
            else:
                lego_data = format_casing(lego_data, target_name, casing, source_name)
        if field.get('in_list', None):
            with open(field['in_list']) as f:
                in_list = yaml.safe_load(f)
            # for each cell in the column, get the location of the cell in the list
            field_name = field.get('target_name', field['source_name'])
            lego_data[field_name] = lego_data[field_name].apply(lambda x: f'****{x}****' if x not in in_list else x)
            
    lego_data = lego_data.rename(columns = rename_mapper)

    return lego_data


def format_casing(lego_data, target_name, casing, source_name=None):
    source_name = source_name if source_name else target_name
    if casing == 'upper':
        lego_data[target_name] = lego_data[source_name].str.upper()
    elif casing == 'lower':
        lego_data[target_name] = lego_data[source_name].str.lower()
    elif casing == "proper":
        lego_data[target_name] = lego_data[source_name].str.title()
    return lego_data

def expand_on_df(lego_data, target_name, expand_on):
    mask = lego_data[target_name].notnull()
    #remove target_name values in mask that are empty
    mask = mask & (lego_data[target_name] != '')
    #print(f"mask is {mask}")
    new_rows = lego_data[mask].copy()
    new_rows[expand_on] = lego_data[target_name]
    #print(new_rows.iloc[0])
    lego_data = pd.concat([lego_data, new_rows], axis=0, ignore_index=True)
    return lego_data

def get_configs():
    if glob.glob('./config.yml'):
        with open('./config.yml') as file:
            return yaml.full_load(file)

if __name__ == '__main__':
    main()