import json
from utils2 import *

with open('SSCC0_FE_060_TW_SSC_RAI_OPERAZIONI_CSDR_TOP_TT_01_JPL.json', 'r') as file:
    pipeline_data = json.load(file)

# For each field, his from and to node 
nodes = pipeline_data['pipelines'][0]['nodes']
schemas = {schema['id']: schema for schema in pipeline_data['schemas']}

def get_field_and_table_identifier(node_id, field_name):
    """Retrieve the schema field identifier and table name from a node and field name."""
    for node in nodes:
        if node['id'] == node_id:
            schema_id = node.get('outputs', [{}])[0].get('schema_ref')
            if schema_id in schemas:
                fields = schemas[schema_id]['fields']
                table_name = node['app_data']['ui_data'].get('label', 'Unknown Table')
                for field in fields:
                    if field['name'] == field_name:
                        return f"{schema_id}/{field_name}", table_name
    return "Unknown", "Unknown Table"

field_asset_movements = process_pipeline(pipeline_data)
print(field_asset_movements)
node_connection_map = map_node_connections(nodes)

for field, asset in field_asset_movements.items():

    from_node = asset['from_node']
    from_field_name = asset['from_field'].split('/')[-1]  
    schema_field_id, table_name = get_field_and_table_identifier(from_node, from_field_name)
    asset['from_field'] = schema_field_id
    asset['table_name'] = table_name  
 
    to_nodes = asset['to_node']
    to_field_name = asset['to_field'].split('/')[-1] 

    to_node = to_nodes[0]
    schema_field_id, _ = get_field_and_table_identifier(to_node, to_field_name)
    asset['to_field'] = schema_field_id

print(json.dumps(field_asset_movements, indent=2))
