
import json
import re

# Import the function that loads local parameters
from local_parameters_return import load_local_parameters
from print_query import extract_select_statements

file_path = 'json_file/medium.json'

def substitute_parameters(table_name, parameters):
    # Using a regex to find and directly replace all matches
    pattern = re.compile(r'\#(.*?)\#')
    def replace_match(match):
        key = match.group(1)  # Get the key from the match
        # Replace with the parameter's default value if it exists, otherwise keep the original
        return parameters.get(key, {'Default Value': match.group(0)})['Default Value']
    
    # Replace all matches in the table name
    return pattern.sub(replace_match, table_name)

if __name__ == "__main__":
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
            final_table_name = substitute_parameters(table_name, parameter_dict)

            print("Final Table Name:", final_table_name) 
        else:
            print("No valid table alias found in the FROM clause.")
    else:
        print("No valid FROM clause found in the query.")
