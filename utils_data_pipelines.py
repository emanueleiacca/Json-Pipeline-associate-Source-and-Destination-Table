# Function to summarize the pipeline nodes and their connections
def summarize_pipeline(pipeline_data):
    summary = {
        'primary_pipeline_id': pipeline_data.get("primary_pipeline"),
        'nodes': [],
        'schemas': [],
        'links': []
    }

    # Summarize nodes
    for node in pipeline_data.get('pipelines', [])[0].get('nodes', []):
        node_summary = {
            'id': node.get('id'),
            'operation': node.get('op'),
            'type': node.get('type'),
            'label': node.get('app_data', {}).get('ui_data', {}).get('label')
        }
        summary['nodes'].append(node_summary)

    # Summarize schemas
    for schema in pipeline_data.get('schemas', []):
        schema_summary = {
            'id': schema.get('id'),
            'fields': [field.get('name') for field in schema.get('fields', [])]
        }
        summary['schemas'].append(schema_summary)

    # Summarize links
    for pipeline in pipeline_data.get('pipelines', []):
        for node in pipeline.get('nodes', []):
            if 'inputs' in node:
                for input in node['inputs']:
                    if 'links' in input:
                        for link in input['links']:
                            link_summary = {
                                'source_node_id': link.get('node_id_ref'),
                                'target_node_id': node.get('id'),
                                'source_field': link.get('source_field_id'),
                                'target_field': link.get('target_field_id')
                            }
                            summary['links'].append(link_summary)

    return summary

# Function to find the target table for the pipeline
def find_target_tables_from_links(pipeline_summary):
    links = pipeline_summary['links']
    source_nodes = set(link['source_node_id'] for link in links)
    target_nodes = set(link['target_node_id'] for link in links)
    
    target_tables = []
    for link in links:
        target_node_id = link['target_node_id']
        if target_node_id not in source_nodes:
            target_table_label = next(node['label'] for node in pipeline_summary['nodes'] if node['id'] == target_node_id)
            target_tables.append(target_table_label)
    
    if target_tables:
        return target_tables
    else:
        return ['No target table found']

def get_pipeline_edges(pipeline_data):
    edges = []  #will store tuples of (source_node_id, target_node_id)
    
    primary_pipeline = next(
        (p for p in pipeline_data['pipelines'] if p['id'] == pipeline_data['primary_pipeline']),
        None
    )

    if not primary_pipeline:
        print("Primary pipeline not found.")
        return []

    for node in primary_pipeline.get('nodes', []):
        node_id = node.get('id')

        if 'outputs' in node:
            for output in node['outputs']:
                if 'app_data' in output and 'datastage' in output['app_data']:
                    source_of_link = output['app_data']['datastage'].get('is_source_of_link')
                    if source_of_link:
                        for target in primary_pipeline.get('nodes', []):
                            if 'inputs' in target:
                                for input in target['inputs']:
                                    if 'links' in input:
                                        for link in input['links']:
                                            if link['node_id_ref'] == node_id:
                                                edges.append((node_id, target['id']))

    return edges

def get_pipeline_edges2(pipeline_data):
    """
    Extracts edges from the primary pipeline in a given pipeline data structure.
    An edge is defined as a tuple (source_node_id, target_node_id) representing data flow.

    Parameters:
    - pipeline_data (dict): The pipeline data containing nodes and links.

    Returns:
    - list: A list of tuples, each representing an edge.
    """
    edges = []
    
    # Find the primary pipeline
    primary_pipeline = next(
        (p for p in pipeline_data['pipelines'] if p['id'] == pipeline_data['primary_pipeline']),
        None
    )

    if not primary_pipeline:
        print("Primary pipeline not found.")
        return []

    # Mapping node_id to the list of its inputs for fast lookup
    node_inputs_map = {
        node['id']: node.get('inputs', [])
        for node in primary_pipeline.get('nodes', [])
    }

    # Traverse each node and its outputs to find corresponding inputs
    for node in primary_pipeline.get('nodes', []):
        node_id = node.get('id')
        if 'outputs' in node:
            for output in node['outputs']:
                if 'app_data' in output and 'datastage' in output['app_data']:
                    source_of_link = output['app_data']['datastage'].get('is_source_of_link')
                    if source_of_link:
                        for target_id, inputs in node_inputs_map.items():
                            for input in inputs:
                                if 'links' in input:
                                    for link in input['links']:
                                        if link['node_id_ref'] == node_id:
                                            edges.append((node_id, target_id))

    return edges
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

def print_pipeline_links(pipeline_data):
    primary_pipeline = next(
        (p for p in pipeline_data['pipelines'] if p['id'] == pipeline_data['primary_pipeline']),
        None
    )
    if not primary_pipeline:
        print("Primary pipeline not found.")
        return