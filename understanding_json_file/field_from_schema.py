import json
from utils_data_pipelines import *
with open('SSCC0_FE_060_TW_SSC_RAI_OPERAZIONI_CSDR_TOP_TT_01_JPL.json', 'r') as file:
    pipeline_data = json.load(file)

# fields corresponding to each table derived by the schema 
table_fields_mapping = {}

for node in pipeline_data['pipelines'][0]['nodes']:
    node_label = node['app_data']['ui_data']['label'] if 'ui_data' in node['app_data'] else ''
    
    if 'outputs' in node:
        for output in node['outputs']:
            if 'schema_ref' in output:
                schema_id = output['schema_ref']
                for schema in pipeline_data['schemas']:
                    if schema['id'] == schema_id:
                        schema_fields = schema['fields']
                        field_names = [field['name'] for field in schema_fields]
                        table_fields_mapping[node_label] = field_names
                        break 
                break  
print(table_fields_mapping)
