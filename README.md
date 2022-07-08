# What is legoberry?

legoberry is a utility to merge multiple csv files into 1 or more files. It takes in a schema and does simple transformations stated below. 
# How to run legoberry

## Config Options
This file must be named config.yml. Sometimes windows file explorer doesn't show  
| parameter | description | example |
| ------ | ------ | ------ |
| type | school level  like HS or MS| ES |
| target_file | name of file where source csvs will be merged | master.csv |
| ordered_headers | a list of headers to be ordered  |``` ordered_headers:
  - 'first'
  - 'last'
  - 'e-mail - work' 
  - 'grade level'
  - 'tag'
  - 'lead'
  - 'school'
``` |
| OneDrive | [plugins/onedrive/README.md][PlOd] | |
| Medium | [plugins/medium/README.md][PlMe] | |
| Google Analytics | [plugins/googleanalytics/README.md][PlGa] | |
