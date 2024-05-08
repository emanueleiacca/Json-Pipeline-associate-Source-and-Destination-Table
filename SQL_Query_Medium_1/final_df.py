import re
from print_query import extract_select_statements
import json
from local_parameters_return import load_local_parameters
from print_query import extract_select_statements
from placeholders import substitute_parameters
import pandas as pd

file_path = 'json_file/medium.json'
query = extract_select_statements(file_path)

selected_columns = re.findall(r'SELECT\s+(.*?)\s+FROM', query, re.DOTALL)[0].strip().split('\n')

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

    # Load JSON data to extract the query
with open(file_path, 'r') as file:
    data = json.load(file)

    # Assuming extract_select_statements function returns the SQL query string
query = extract_select_statements(file_path)  # pass the actual data if the function expects it

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

            # Load local parameters from JSON
        parameters = load_local_parameters(file_path)
        parameter_dict = {param['Name']: param for param in parameters}

            # Substitute parameters in the table name
        table_name = substitute_parameters(table_name, parameter_dict)
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
    final_column_name = substitute_parameters(text, parameter_dict)

    updated2_columns.append((final_column_name, column_name))
    #print(updated2_columns)
transformations = re.search(r'CROSS JOIN.*?(?=;)', query, re.DOTALL).group(0)
aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)
columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    unique_values = list(set(unique_values))
    with open(file_path, 'r') as file:
        data = json.load(file)

        # Assuming extract_select_statements function returns the SQL query string
    query = extract_select_statements(file_path)  # pass the actual data if the function expects it

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

                # Load local parameters from JSON
            parameters = load_local_parameters(file_path)
            parameter_dict = {param['Name']: param for param in parameters}

                # Substitute parameters in the table name
            table_name = substitute_parameters(table_name, parameter_dict)
    updated_values = [value.replace(alias + '.', table_name + '.') if value.startswith(alias + '.') else value for value in unique_values]
    #print(alias)
    print(updated_values)
    for i, (text, column_name) in enumerate(updated2_columns):
        if alias == column_name.strip(','):
            updated2_columns.pop(i)  # Remove the previous entry
            break
    for updated_value in updated_values:
        parameters = load_local_parameters(file_path)
            #print(parameters)
        parameter_dict = {param['Name']: param for param in parameters}
                #print(parameter_dict)
        final_column_name = substitute_parameters(updated_values[0], parameter_dict)
        updated2_columns.append((final_column_name + '.', column_name))

data = [column for column in updated2_columns]

df = pd.DataFrame(data, columns=['source_column', 'target_column'])

# Split source column into multiple rows if it contains multiple values
df = df.assign(source_column=df.source_column.str.split(',')).explode('source_column')

# Remove trailing comma from target column
df['target_column'] = df.target_column.str.rstrip(',')

df.to_excel('medium_sql.xlsx', index=False)


