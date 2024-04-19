import json
import pandas as pd
import json

def process_pipeline(pipeline_data):
    schemas = {s['id']: s for s in pipeline_data['schemas']}
    def find_source_field(schema_id, field_name):
        schema = schemas.get(schema_id)
        if schema:
            for field in schema['fields']:
                if field['name'] == field_name:
                    return field['metadata'].get('source_field_id') or f"{schema_id}/{field_name}"
        return None


    field_assets = {}

    for pipeline in pipeline_data['pipelines']:
        for node in pipeline['nodes']:
   
            if 'outputs' in node:
                for output in node['outputs']:
                    if 'schema_ref' in output:
                        for field in schemas[output['schema_ref']]['fields']:
                            field_name = field['name']
                            source_field = find_source_field(output['schema_ref'], field_name)
                            if source_field:
                                field_assets[field_name] = {
                                    'from_node': node['id'],
                                    'from_field': source_field
                                }
            
            if 'inputs' in node:
                for input in node['inputs']:
                    if 'links' in input:
                        for link in input['links']:
                            if 'schema_ref' in input:
                                for field in schemas[input['schema_ref']]['fields']:
                                    field_name = field['name']
                                    if field_name in field_assets:
                                        field_assets[field_name]['to_node'] = node['id']
                                        field_assets[field_name]['to_field'] = find_source_field(input['schema_ref'], field_name)

    return field_assets

# Mapping node IDs to their source node IDs
def map_node_connections(nodes):
    node_connection_map = {}
    for node in nodes:
        if 'inputs' in node:
            for input in node['inputs']:
                if 'links' in input:
                    for link in input['links']:
                        node_connection_map[node['id']] = link['node_id_ref']
    return node_connection_map

# Trace back the original source node ID
def get_original_source_node(node_id, node_connection_map):
    while node_id in node_connection_map:
        node_id = node_connection_map[node_id]
    return node_id


# Revised function to extract the table names from the nodes based on the "label" 
def get_field_identifier(node_id, field_name):
    nodes = pipeline_data['pipelines'][0]['nodes']
    schemas = {schema['id']: schema for schema in pipeline_data['schemas']}
    for node in nodes:
        if node['id'] == node_id:
            schema_id = node.get('outputs', [{}])[0].get('schema_ref')
            if schema_id in schemas:
                fields = schemas[schema_id]['fields']
                for field in fields:
                    if field['name'] == field_name:
                        return f"{schema_id}/{field_name}"
    return "Unknown"

# Update the function to use the actual names from the "label" field
def convert_to_dataframe_with_labels(field_assets, nodes):
    data_rows = []

    for field_name, field_info in field_assets.items():
        source_table_name = get_table_name_from_node(nodes, field_info['from_node'])
        
        target_table_name = get_table_name_from_node(nodes, field_info['to_node']) if field_info['to_node'] else 'Unknown'

        data_rows.append({
            'sorgente': source_table_name,  
            'campo': field_name,            
            'target': target_table_name,   
            'campo_target': field_name     
        })

    df = pd.DataFrame(data_rows)
    return df

# Function to summarize the pipeline schemas and their corresponding nodes
def summarize_pipeline_schemas(pipeline_data):
    nodes = pipeline_data['pipelines'][0]['nodes']
    schema_summary = {}

    for pipeline in pipeline_data['pipelines']:
        for node in pipeline['nodes']:
            if 'outputs' in node:
                for output in node['outputs']:
                    if 'schema_ref' in output:
                        schema_id = output['schema_ref']
                        schema_summary[schema_id] = get_table_name_from_node(nodes, node['id'])

            if 'inputs' in node:
                for input in node['inputs']:
                    if 'schema_ref' in input:
                        schema_id = input['schema_ref']
                        if schema_id not in schema_summary:
                            schema_summary[schema_id] = get_table_name_from_node(nodes, node['id'])

    return schema_summary

