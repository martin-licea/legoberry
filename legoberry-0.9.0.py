import os
import glob
import pandas as pd
import logging
from nested_lookup import nested_lookup
import yaml

def main():
    os.chdir('..')
    conf = get_configs()
    clean_previous_runs([conf['target_file'], conf['log']])
    legos = glob.glob('*.csv')
    school_type = conf['type']
    target_headers = nested_lookup('target_name', conf)
    target_headers = list(set(target_headers)) # remove dupes
    lego_tower = pd.DataFrame(columns=target_headers)

    for lego in legos:
        lego_data = pd.read_csv(lego)
        lego_data['filename'] = lego.split('.')[0]
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
        mini_lego_tower[target_headers].to_csv(file_name, index=False)
        lego_tower_size -= conf['max_target_file_size']
        i += 1

    with open(conf['log'], 'w+') as f: #TODO: write a logger and send to output file
        f.write(f'legoberry found a total of {len(legos)} files to aggregate')
        f.write(f"\nwriting a total of {i} file(s) at a max {conf['max_target_file_size']} data rows per")

def clean_previous_runs(files_to_clean: list):
    for file in files_to_clean:
        name, extension = file.split('.')
        if name == 'master':
            master_files = glob.glob(f'{name}*.{extension}')
            if master_files:
                for master in master_files:
                    os.remove(master)
            else: 
                print(f'{file} does not exist in this directory. Will not remove anything')
        else:
            os.remove(file) if glob.glob(file) else print(f'{file} does not exist in this directory. Will not remove anything')

def find_transformations(conf: dict, lego_data):
    fields = conf['fields']
    rename_mapper = {}
    for field in fields:
        if field.get('split', None):
            new = lego_data[field['source_name']].str.split(field['split_by'], expand = True)
            for item in field['split']:
                index = item['index'] - 1
                lego_data[item['target_name']] = new[index]
        if field.get('replace', None):
            from_these = []
            to_these = []
            for item in field['replace']:
                from_these.append(item['from'])
                to_these.append(item['to'])
            lego_data[field['source_name']] = lego_data[field['source_name']].replace(from_these, to_these)
        if field.get('default_value', None):
            lego_data[field['target_name']] = field['default_value']
        elif field.get('target_name', None):
            print(field)
            if field['source_name'] in lego_data:
                rename_mapper[field['source_name']] = field['target_name']
            elif field.get('secondary_source_name', None) in lego_data:
                print('got to 2nd')
                rename_mapper[field['secondary_source_name']] = field['target_name']
    lego_data = lego_data.rename(columns = rename_mapper)
    #print(lego_data)
    return lego_data
        

def get_configs():
    if glob.glob('./config.yml'):
        with open('./config.yml') as file:
            return yaml.full_load(file)

if __name__ == '__main__':
    main()