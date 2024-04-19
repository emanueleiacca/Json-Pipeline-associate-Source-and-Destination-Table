import json

# Load the JSON data from the file
with open('SSCC0_FE_060_TW_SSC_RAI_OPERAZIONI_CSDR_TOP_TT_01_JPL.json', 'r') as file:
    pipeline_data = json.load(file)

# Get the primary pipeline information and the nodes within it
primary_pipeline_id = pipeline_data['pipelines'][0]['id']
nodes = pipeline_data['pipelines'][0]['nodes']

# Create a dictionary to map node IDs to their labels for easy reference
node_id_to_label = {node['id']: node['app_data']['ui_data']['label'] for node in nodes if 'app_data' in node and 'ui_data' in node['app_data']}

# Print the mapping
print(f"Primary Pipeline ID: {primary_pipeline_id}")
print("Node ID to Label Mapping:")
for node_id, label in node_id_to_label.items():
    print(f"  - Node ID: {node_id}, Label: {label}")
