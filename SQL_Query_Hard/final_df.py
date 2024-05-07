import re
from utils3 import *
from print_query import extract_select_statements
query = extract_select_statements('json_file/hard.json')
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
# Now apply the regex to find the part between SELECT and FROM
updated_columns = process_sql_columns(query)
#print(updated_columns)

from_clause = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', text, re.DOTALL).group(1)

table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()

table_name = table_alias[0].strip()
#print(table_name)
alias = table_alias[1].strip()
#print(alias)
results_unnested = extract_sql_details(text)
alias_to_processed_values = {item['Alias']: item['Processed Values'] for item in results_unnested}
updated2_columns = []

for text, column_name in updated_columns:
    original_text = text  # Keep the original text for possible output or debugging

    # Replace table alias in text and column_name if they contain the SQL alias
    if alias + '.' in text:
        text = text.replace(alias + '.', table_name + '.')
    if alias + '.' in column_name:
        column_name = column_name.replace(alias + '.', table_name + '.')

    # Check if the column_name is a key in the alias_to_processed_values to replace text
    if column_name in alias_to_processed_values:
        # Assuming that you want to join multiple processed values with a comma, modify if necessary
        text = ', '.join(alias_to_processed_values[column_name])

    updated2_columns.append((text, column_name))  # Append the potentially modified text and the original or modified column_name
import pandas as pd
data = [column for column in updated2_columns]

df = pd.DataFrame(data, columns=['source_column', 'target_column'])

# Split source column into multiple rows if it contains multiple values
df = df.assign(source_column=df.source_column.str.split(',')).explode('source_column')

# Remove trailing comma from target column
df['target_column'] = df.target_column.str.rstrip(',')

df.to_excel('hard_sql.xlsx', index=False)
