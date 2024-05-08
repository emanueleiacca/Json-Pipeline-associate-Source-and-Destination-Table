
import json
import re
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
            unique_names = list(set(names))
            for innermost_text in innermost_texts:
                text = re.sub(r'.*\(([^()]*)\).*', r', '.join(unique_names), text)
        updated_columns.append((text, column[1]))

        parameters = load_local_parameters(file_path)
        parameter_dict = {param['Name']: param for param in parameters}

            # Substitute parameters in the table name
        final_column_name = substitute_parameters(text, parameter_dict)

        print("Final Column Name:", final_column_name) 
    else:
        print("No valid Column found in the SELECT clause.")
else:
    print("No valid SELECT clause found in the query.")
