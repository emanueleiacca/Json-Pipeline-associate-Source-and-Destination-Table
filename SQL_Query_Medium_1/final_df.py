import re
from print_query import extract_select_statements
import json
from local_parameters_return import load_local_parameters
from print_query import extract_select_statements
from placeholders import substitute_parameters, substitute_parameters_PROJDEF
import pandas as pd

# call file
file_path = 'json_file/medium.json'
# extract sql query
query = extract_select_statements(file_path)
# Start the select column process from select.py
selected_columns = re.findall(r'SELECT\s+(.*?)\s+FROM', query, re.DOTALL)[0].strip().split('\n')
# we start by splitting the column between column name and alias when avaible, if not we just copy the column name
columns = []
for column in selected_columns:
    column = column.strip()
    if 'AS' in column:
        parts = re.split(r'\s+AS', column)
        column_name = parts[0].strip()
        alias = parts[1].strip()
        columns.append((column_name, alias))
    else:
        columns.append((column, column))
# Since some of select statement contain function, to facilitate the extraction process we search for the most inner parenthesis where the column name is contained
# This process is followed by some cleaning where we delete the data inside the parenthesis that we don't need, like the parameters of the function
updated_columns = []
for column in columns:
    text = column[0]
    while True:
        innermost_texts = re.findall(r'\(([^()]*)\)', text)
        if not innermost_texts:
            break
        names = [item.split(',')[0].strip().replace("'", '').replace('$', '').split(' as ')[0] for item in innermost_texts]
        unique_names = [name for name in set(names) if not name.startswith('%')]
        for innermost_text in innermost_texts:
            text = re.sub(r'.*\(([^()]*)\).*', ', '.join(unique_names), text)
    parameters = load_local_parameters(file_path)
            #print(parameters)
    parameter_dict = {param['Name']: param for param in parameters}
    #print(parameter_dict)
    final_column_name = substitute_parameters(text, parameter_dict)
    updated_columns.append((final_column_name, column[1]))
    #print(updated_columns)
    #print(final_column_name)

query = extract_select_statements(file_path)  
with open(file_path, 'r') as file:
    data = json.load(file)
# Extract the FROM clause to obtain Table and respective alias info, procedure used on table.py
from_clause_search = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', query, re.DOTALL)
if from_clause_search:
    from_clause = from_clause_search.group(1)

    table_alias_search = re.search(r'(.*)\s+AS\s+(.*)', from_clause)
    if table_alias_search:
        table_name, alias = table_alias_search.groups()
        table_name = table_name.strip()
        #print(table_name)
        alias = alias.strip()
# After extracting it we substitute the placeholder with its value
        parameters = data['parameters']['local_parameters']
        #print(parameters)
        parameter_dict = {param['name'] for param in parameters}
        #rint(parameter_dict)
        default_value_dict = {param['value'] for param in parameters}
        #print(default_value_dict)
        #table_name = substitute_parameters(table_name, parameter_dict) 
# When the parameter inside the job (value) is equal to PROJDEF we go to a separate file where for each placeholder name is stored the corresponding value
        if "PROJDEF" in default_value_dict:
            #print("yes") 

            file_path_PROJDEF = 'json_file\PROJDEF.json'
            with open(file_path_PROJDEF, 'r') as file:
                data = json.load(file)

            parameters_projdef = data['entity']['parameter_set']['parameters']
            projdef_dict = {param['name']: param['value'] for param in parameters_projdef}
            #print(projdef_dict)
# For each name contained into the file we extract the corresponding value and save it on a list
            filtered_dict = {}
            for key, value in list(projdef_dict.items()):
                if isinstance(value, str):
                    #print(key)
                    if key in parameter_dict:
                        filtered_dict[key] = value
#print(filtered_dict)
#df = pd.DataFrame(list(filtered_dict.items()), columns=['Parameter', 'Value'])
#print(df)

# Now we substitute initially each placeholder with its associated value on the Json file, then we substitute it with its corresponding value from PROJDEF file
#print(alias)
updated2_columns = []
for text, column_name in updated_columns:
    text = text.replace(alias + '.', table_name + '.')
    #print(text)
    column_name = column_name.replace(alias + '.', table_name + '.')
    parameters = load_local_parameters(file_path)
    #print(parameters)
    parameter_dict = {param['Name']: param for param in parameters}
    #print(parameter_dict)
    text = substitute_parameters_PROJDEF(text, filtered_dict)
    column_name = substitute_parameters_PROJDEF(column_name, filtered_dict)
    #print(final_column_name)
    #print(final_column2_name)
    updated2_columns.append((text, column_name))
    #print(updated2_columns)

# Now we analyze the last part of the query regarding the Join and unnest part: When merging tables we 
transformations = re.search(r'CROSS JOIN.*?(?=;)', query, re.DOTALL).group(0)
aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)
columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)
#print(columns, aliases)
    #print(unique_values)
with open(file_path, 'r') as file:
    data = json.load(file)

    query = extract_select_statements(file_path)  

 # Extract the FROM clause
    from_clause_search = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', query, re.DOTALL)
    if from_clause_search:
        from_clause = from_clause_search.group(1)

# Extract table name and alias
        table_alias_search = re.search(r'(.*)\s+AS\s+(.*)', from_clause)
        if table_alias_search:
            table_name, alias = table_alias_search.groups()
            table_name = table_name.strip()
            alias = alias.strip()

            parameters = load_local_parameters(file_path)
            #print(parameters)
            parameter_dict = {param['Name']: param for param in parameters}
            #print(parameter_dict)
            default_value_dict = {param['Default Value'] for param in parameters}
            #print(default_value_dict)
            if "PROJDEF" in default_value_dict:
                #print("yes") 

                file_path_PROJDEF = 'json_file\PROJDEF.json'
                with open(file_path_PROJDEF, 'r') as file:
                    data = json.load(file)

                parameters_projdef = data['entity']['parameter_set']['parameters']
                projdef_dict = {param['name']: param['value'] for param in parameters_projdef}
                #print(projdef_dict)
    # For each name contained into the file we extract the corresponding value and save it on a list
                filtered_dict = {}
                for key, value in list(projdef_dict.items()):
                    if isinstance(value, str):
                        #print(key)
                        if key in parameter_dict:
                            filtered_dict[key] = value
            #print(filtered_dict)                
    # Substitute parameters in the table name
            table_name = substitute_parameters_PROJDEF(table_name, filtered_dict)
            #print(table_name)

for alias, column in zip(aliases, columns):
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    unique_values = list(set(unique_values))
    print(unique_values)
    updated_values = [value.replace(alias + '.', table_name + '.') if value.startswith(alias + '.') else value for value in unique_values]
    #print(alias)
    #print(updated_values)
# We substitute alias with the corresponding table name and from its placeholders we obtain the real path
    for updated_value in updated_values:
        parameters = load_local_parameters(file_path)
                #print(parameters)
        parameter_dict = {param['Name']: param for param in parameters}
                    #print(parameter_dict)
        final_column_name = substitute_parameters(updated_values[0], parameter_dict)

    for i, (text, column_name) in enumerate(updated2_columns):
        #print(alias)
        #print(column_name)
        #print(text)
        #print(aliases)
        if alias == column_name.strip(','):
        #for clean_cn in aliases:
            #print(clean_cn)
            #text = 
            #if aliases == column_name.strip(','):
                #print("yes")
            final_column_name = substitute_parameters(unique_values[0], parameter_dict)
            #print(final_column_name)
            updated2_columns[i] = ((final_column_name + '.', column_name))

data = [column for column in updated2_columns]

df = pd.DataFrame(data, columns=['source_column', 'target_column'])

# Split source column into multiple rows if it contains multiple values
df = df.assign(source_column=df.source_column.str.split(',')).explode('source_column')

# Remove trailing comma from target column
df['target_column'] = df.target_column.str.rstrip(',')

df.to_excel('medium1_sql.xlsx', index=False)


