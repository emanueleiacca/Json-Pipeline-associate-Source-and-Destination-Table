import json

def load_pipeline_data(json_path):
    with open(json_path, 'r') as file:
        return json.load(file)

def get_node_id_to_label(nodes):
    return {node['id']: node.get('app_data', {}).get('ui_data', {}).get('label', '') for node in nodes}

def get_node_id_to_schema_ref(nodes):
    schema_ref = {}
    for node in nodes:
        node_id = node['id']
        if 'outputs' in node:
            for output in node['outputs']:
                if 'schema_ref' in output:
                    schema_ref[node_id] = output['schema_ref']
                    break
    return schema_ref

def get_schema_id_to_fields(schemas):
    return {schema['id']: [field['name'] for field in schema['fields']] for schema in schemas}

def map_nodes_to_fields(nodes, edges, node_id_to_schema_ref, schema_id_to_fields):
    node_fields = {}
    for node in nodes:
        node_id = node['id']
        node_label = node.get('app_data', {}).get('ui_data', {}).get('label', '')
        if 'Write' in node_label:  
            source_node_id = next((src for src, dst in edges if dst == node_id), None)
            if source_node_id:
                source_schema_ref = node_id_to_schema_ref.get(source_node_id)
                node_fields[node_label] = schema_id_to_fields.get(source_schema_ref, [])
            else:
                node_fields[node_label] = []
        else:
            schema_ref = node_id_to_schema_ref.get(node_id)
            fields = schema_id_to_fields.get(schema_ref, [])
            node_fields[node_label] = fields
    return node_fields

def get_pipeline_edges(nodes):
    edges = []
    for node in nodes:
        if 'inputs' in node:
            for input_node in node['inputs']:
                for link in input_node['links']:
                    edges.append((link['node_id_ref'], node['id']))
    return edges

def main(json_path):
    pipeline_data = load_pipeline_data(json_path)
    nodes = pipeline_data['pipelines'][0]['nodes']
    schemas = pipeline_data['schemas']
    edges = get_pipeline_edges(nodes)

    node_id_to_label = get_node_id_to_label(nodes)
    node_id_to_schema_ref = get_node_id_to_schema_ref(nodes)
    schema_id_to_fields = get_schema_id_to_fields(schemas)
    
    node_to_fields = map_nodes_to_fields(nodes, edges, node_id_to_schema_ref, schema_id_to_fields)
    
    return node_to_fields  
