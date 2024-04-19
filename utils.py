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
