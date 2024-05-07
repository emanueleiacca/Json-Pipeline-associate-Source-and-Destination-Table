import re
from print_query import extract_select_statements

# Assuming extract_select_statements returns the full query as a string
query = extract_select_statements('json_file/medium.json')

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
    updated_columns.append((text, column[1]))

from_clause = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', query, re.DOTALL).group(1)
table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()
#print(table_alias)
table_name = table_alias[0].strip()
alias = table_alias[1].strip()
#print(alias)
updated2_columns = []
for text, column_name in updated_columns:
    text = text.replace(alias + '.', table_name + '.')
    column_name = column_name.replace(alias + '.', table_name + '.')
    updated2_columns.append((text, column_name))

transformations = re.search(r'CROSS JOIN.*?(?=;)', query, re.DOTALL).group(0)
aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)
columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    unique_values = list(set(unique_values))
    #print(unique_values)
    table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()
    #print(table_alias)
    table_name = table_alias[0].strip()
    alias = table_alias[1].strip()
    updated_values = [value.replace(alias + '.', table_name + '.') if value.startswith(alias + '.') else value for value in unique_values]
    #print(alias)
    #print(updated_values)
    for i, (text, column_name) in enumerate(updated2_columns):
        if alias == column_name.strip(','):
            updated2_columns.pop(i)  # Remove the previous entry
            break
    for updated_value in updated_values:
        updated2_columns.append((updated_value + '.', column_name))

for column in updated2_columns:
    print(column)
