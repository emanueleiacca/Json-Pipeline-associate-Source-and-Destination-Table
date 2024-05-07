import json
def extract_select_statements(json_filename):
    """
    Extracts select statements from JSON data where the structure involves
    pipelines containing nodes with connection properties.

    Args:
    json_filename (str): The path to the JSON file.

    Returns:
    list: A list containing all select statements found in the JSON file.
    """
    select_statements = []

    # Load the JSON data from the file
    with open(json_filename, 'r') as file:
        data = json.load(file)

    # Navigate through the first pipeline's nodes to find select statements
    for node in data['pipelines'][0]['nodes']:
        if 'connection' in node:
            connection_properties = node['connection']['properties']
            if 'select_statement' in connection_properties:
                select_statements.append(connection_properties['select_statement'])

    return select_statements[0]
