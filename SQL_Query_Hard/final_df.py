import re
from print_query import extract_select_statements
import json
from local_parameters_return import load_local_parameters
from print_query import extract_select_statements
from placeholders import substitute_parameters, substitute_parameters_PROJDEF
import pandas as pd
file_path ='json_file/hard.json'
query = extract_select_statements(file_path)
rows = query.split('\n')

# Remove the rows that contain "THEN CAST" or "ELSE CAST"
rows = [row for row in rows if not re.search(r'THEN CAST|ELSE CAST', row)]

# Join the rows back into a single query
query = '\n'.join(rows)

text = query.replace('\n', '')
# Split the text into rows
rows = re.split(r',(?![^()]*\))', text)
query = '\n'.join(rows)
#print(query)

selected_columns_match = re.search(r'SELECT\s+(.*?)\s+FROM', query, re.DOTALL)
if selected_columns_match:
    columns_text = selected_columns_match.group(1).strip()
    # Normalize spaces and handle line transitions within the query
    columns_text = re.sub(r'\s*\n\s*', ', ', columns_text)
    columns_text = re.sub(r'[ \t]+', ' ', columns_text)
    # Splitting the columns, ensuring we don't split inside brackets
    columns = re.split(r',(?![^\(\[]*[\]\)])', columns_text)

    parsed_columns = []
    for column in columns:
        column = column.strip()
        # Remove substrings starting with '%' using a regular expression
        column = re.sub(r'%\S+', '', column)
        if ' AS ' in column:
            parts = re.split(r'\s+AS\s+', column)
            column_name = parts[0].strip()
            alias = parts[1].strip() if len(parts) > 1 else column_name
            parsed_columns.append((column_name, alias))
        else:
            parsed_columns.append((column, column))

updated_columns = []
for column, alias in parsed_columns:
    #print(alias)
    text = column
    #print(text)
    while True:
        innermost_texts = re.findall(r'\(([^()]*)\)', text)
        if not innermost_texts:
            break
        names = [item.split(',')[0].strip().replace("'", "").replace("$", "").split(' as ')[0] for item in innermost_texts]
        unique_names = [name for name in set(names) if not name.startswith('%')]
        #print(unique_names)
        for innermost_text in innermost_texts:
            text = re.sub(r'.*\(([^()]*)\).*', ', '.join(unique_names), text)
    # Load parameters, ideally this should be loaded outside of the loop if they don't change per iteration
    # For demonstration, it's included here:
    parameters = load_local_parameters(file_path) 
    parameter_dict = {param['Name']: param for param in parameters}
    # Substitute parameters in the column text
    final_column_name = substitute_parameters(text, parameter_dict)
    #print(final_column_name)
    updated_columns.append((final_column_name, alias))
#print(updated_columns)

#print(updated_columns)
query = extract_select_statements('json_file/hard.json')
rows = query.split('\n')

# Remove the rows that contain "THEN CAST" or "ELSE CAST"
rows = [row for row in rows if not re.search(r'THEN CAST|ELSE CAST', row)]

# Join the rows back into a single query
query = '\n'.join(rows)

text = query.replace('\n', '')
rows = re.split(r',(?![^()]*\))', text)
query = '\n'.join(rows)

from_clause = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', text, re.DOTALL).group(1)

table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()

table_name = table_alias[0].strip()
#print(table_name)
alias_MSTR = table_alias[1].strip()
with open(file_path, 'r') as file:
    data = json.load(file)

parameters = data['parameters']['local_parameters']
#print(parameters)
parameter_dict = {param['name'] for param in parameters}
#print(parameter_dict)
default_value_dict = {param['value'] for param in parameters}
#print(default_value_dict)
#table_name = substitute_parameters(table_name, parameter_dict) 
#print(table_name)
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
#print(alias_MSTR)
updated2_columns = []
for text, column_name in updated_columns:
    #print(text)
    text = text.replace(alias_MSTR + '.', table_name + '.')
    #print(text)
    column_name = column_name.replace(alias_MSTR + '.', table_name + '.')
    parameters = load_local_parameters(file_path)
    #print(parameters)
    parameter_dict = {param['Name']: param for param in parameters}
    #print(parameter_dict)
    #print(text)
    text = substitute_parameters_PROJDEF(text, filtered_dict)
    #print(text)
    #print(filtered_dict)
    column_name = substitute_parameters_PROJDEF(column_name, filtered_dict)
    #print(column_name)
    #print(final_column2_name)
    updated2_columns.append((text, column_name))
#print(updated2_columns)

query = extract_select_statements('json_file/hard.json')
rows = query.split('\n')

# Remove the rows that contain "THEN CAST" or "ELSE CAST"
rows = [row for row in rows if not re.search(r'THEN CAST|ELSE CAST', row)]

# Join the rows back into a single query
query = '\n'.join(rows)

text = query.replace('\n', '')
rows = re.split(r',(?![^()]*\))', text)
query = '\n'.join(rows)

joins = re.findall(r'(CROSS JOIN|LEFT OUTER JOIN)\s+(.*?)(?=\s+LEFT OUTER JOIN|\s+CROSS JOIN|$)', text, re.DOTALL)

    # Dictionary to track table names associated with aliases for substitution
alias_table_map = {}
results_unnested = []  # Collection to hold results instead of printing them

for join_type, content in joins:
    if join_type == 'CROSS JOIN':
            # Extract aliases typically following 'AS' keyword for CROSS JOIN
        aliases = re.findall(r'AS\s+(\w+)', content)
        columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', content, re.DOTALL)

    elif join_type == 'LEFT OUTER JOIN':
            # Extract tables and aliases for LEFT OUTER JOIN
        aliases = re.findall(r'<=\s+(\w+)', content)
        columns = re.findall(r'ON\s+(.*?)(?=\s+LEFT OUTER JOIN|\s+CROSS JOIN|\s+WHERE|$)', content, re.DOTALL)
        pattern = r'LEFT OUTER JOIN\s+([^\s]+)\s+AS\s+([^\s]+)\s+ON'
        joins = re.findall(pattern, text, re.IGNORECASE)

        # Process each alias and the respective SQL segment
    for alias, column in zip(aliases, columns):
        table_aliases = re.findall(r'LEFT OUTER JOIN\s+([^\s]+)\s+AS\s+([^\s]+)\s+ON', text)
        table_table = table_aliases[0][0]
        alias_table = table_aliases[0][1]
        #print(alias)
        #print(alias_table) #A
        if column:
            unique_values = set(re.findall(r'\(([^()]*)\)', column))
            processed_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
            updated_values = []

            for value in processed_values:
                    # Perform the replacement in each string individually
                prefix = value.split('.')[0] if '.' in value else None
                if prefix and prefix == alias_table:
                    prefix_table = table_table
                    value = value.replace(alias_table, prefix_table, 1)  # Replace only the first occurrence in each string
                updated_values.append(value)

            results_unnested.append({"Alias": alias, "Processed Values": list(set(updated_values))})
            #print(alias)  # Deduplicate the final list
    #

    alias_to_processed_values = {item['Alias']: item['Processed Values'] for item in results_unnested}

#print(alias_to_processed_values)
#print(results_unnested)
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    unique_values = list(set(unique_values))
    #print(unique_values)
    updated_values = [value.replace(alias + '.', table_name + '.') if value.startswith(alias + '.') else value for value in unique_values]
    alias_fin = alias
    #print(alias_fin)
    #print(updated_values)
# We substitute alias with the corresponding table name and from its placeholders we obtain the real path
    for updated_value in updated_values:
        #print(updated_value)
        parameters = load_local_parameters(file_path)
                #print(parameters)
        parameter_dict = {param['Name']: param for param in parameters}
                    #print(parameter_dict)
        final_column_name = substitute_parameters(updated_value, parameter_dict)
        #print("text here")
        #print(final_column_name)
        #print("text here")

#print(final_column_name)
#print(alias_fin)
joins = re.findall(r'(CROSS JOIN|LEFT OUTER JOIN)\s+(.*?)(?=\s+LEFT OUTER JOIN|\s+CROSS JOIN|$)', text, re.DOTALL)

for join_type, content in joins:
    if join_type == 'CROSS JOIN':
            # Extract aliases typically following 'AS' keyword for CROSS JOIN
        aliases = re.findall(r'AS\s+(\w+)', content)
        columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', content, re.DOTALL)

    elif join_type == 'LEFT OUTER JOIN':
            # Extract tables and aliases for LEFT OUTER JOIN
        aliases = re.findall(r'<=\s+(\w+)', content)
        columns = re.findall(r'ON\s+(.*?)(?=\s+LEFT OUTER JOIN|\s+CROSS JOIN|\s+WHERE|$)', content, re.DOTALL)
        pattern = r'LEFT OUTER JOIN\s+([^\s]+)\s+AS\s+([^\s]+)\s+ON'
        joins = re.findall(pattern, text, re.IGNORECASE)

    for alias, column in zip(aliases, columns):
        unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
        unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
        unique_values = list(set(unique_values))
        #print(unique_values)
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
            #print(final_column_name)
            #print("text here")
        for i, (text, column_name) in enumerate(updated2_columns):
            #print(alias)
            #print(column_name)
            #print("prova")
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

# Print the final updated columns
for col in updated2_columns:
    print(col)

data = [column for column in updated2_columns]

df = pd.DataFrame(data, columns=['source_column', 'target_column'])

# Split source column into multiple rows if it contains multiple values
df = df.assign(source_column=df.source_column.str.split(',')).explode('source_column')

# Remove trailing comma from target column
df['target_column'] = df.target_column.str.rstrip(',')

df.to_excel('hard_sql.xlsx', index=False)
