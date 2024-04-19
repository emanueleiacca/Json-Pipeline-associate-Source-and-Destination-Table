import json

with open('SSCC0_FE_060_TW_SSC_RAI_OPERAZIONI_CSDR_TOP_TT_01_JPL.json', 'r') as file:
    pipeline_data = json.load(file)

# schema IDs corresponding to each table
table_schema_mapping = {}

for node in pipeline_data['pipelines'][0]['nodes']:
    node_id = node['id']
    node_label = node['app_data']['ui_data']['label'] if 'ui_data' in node['app_data'] else ''
    
    if 'outputs' in node:
        for output in node['outputs']:
            if 'schema_ref' in output:
                schema_id = output['schema_ref']
                table_schema_mapping[node_label] = schema_id
                break  

print(table_schema_mapping)
