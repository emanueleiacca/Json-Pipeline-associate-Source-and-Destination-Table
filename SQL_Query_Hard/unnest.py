import re
from utils3 import *
from print_query import extract_select_statements
query = extract_select_statements('json_file/hard.json')
results = extract_sql_details(query)
print(results)


