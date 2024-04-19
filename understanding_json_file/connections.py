import json

with open('hard.json', 'r') as file:
    pipeline_data = json.load(file)
def print_pipeline_links(pipeline_data):
    primary_pipeline = next(
        (p for p in pipeline_data['pipelines'] if p['id'] == pipeline_data['primary_pipeline']),
        None
    )
    if not primary_pipeline:
        print("Primary pipeline not found.")
        return
# From links obtain outputs and inputs
    for node in primary_pipeline.get('nodes', []):
        if 'outputs' in node:
            for output in node['outputs']:
                if 'app_data' in output and 'datastage' in output['app_data']:
                    is_source_of_link = output['app_data']['datastage'].get('is_source_of_link')
                    if is_source_of_link:
                        print(f"Node ID: {node['id']} is the source of link ID: {is_source_of_link}")

        if 'inputs' in node:
            for input in node['inputs']:
                if 'links' in input:
                    for link in input['links']:
                        print(f"Node ID: {node['id']} is the target of link ID: {link['id']} from source node ID: {link['node_id_ref']}")

print_pipeline_links(pipeline_data)