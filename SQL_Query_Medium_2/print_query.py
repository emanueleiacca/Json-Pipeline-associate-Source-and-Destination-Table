import json

with open('json_file/medium.json') as f:
    data = json.load(f)

for node in data['pipelines'][0]['nodes']:
    if 'connection' in node:
        connection_properties = node['connection']['properties']
        if 'select_statement' in connection_properties:
            print(connection_properties['select_statement'])
