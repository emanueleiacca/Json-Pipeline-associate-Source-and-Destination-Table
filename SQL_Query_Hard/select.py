import re
from utils3 import process_sql_columns
from print_query import extract_select_statements
query = extract_select_statements('json_file/hard.json')

# Split the query into rows
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
process_sql_columns(query)
