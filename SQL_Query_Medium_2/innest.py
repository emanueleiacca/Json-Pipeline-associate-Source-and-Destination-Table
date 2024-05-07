import re

from print_query import extract_select_statements
query = extract_select_statements('json_file/medium.json')
innermost_texts = re.findall(r'\(([^()]*)\)', query)

print(innermost_texts)
# Output:
# ['python', 'a', 'multiple', 'another', 'example', 'more']

names = [item.split(',')[0].strip().replace("'", '').replace('$', '') for item in innermost_texts]

# Remove duplicates
unique_names = list(set(names))

print(unique_names)
