import re

from print_query import extract_select_statements
query = extract_select_statements('json_file/hard.json')

from_clause = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', query, re.DOTALL).group(1)

table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()

table_name = table_alias[0].strip()
alias = table_alias[1].strip()

print("Table Name:", table_name)
print("Alias:", alias)
