import re
from print_query import extract_select_statements
query = extract_select_statements('json_file/medium.json')
transformations = re.search(r'INNER JOIN.*?(?=;)', query, re.DOTALL).group(0)

aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)

columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    print("Alias:", alias)
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    print("Unique Values:", list(set(unique_values)))  # Use set to remove duplicates
    print()
#no transformation done
