import json
import networkx as nx
import pandas as pd
from utils_data_pipelines import *


json_path = 'json_file\hard.json' # Replace with your JSON file path
field_in_eachNode = main(json_path)

#print("Type of field_in_eachNode:", type(field_in_eachNode))
print("Contents of field_in_eachNode:", field_in_eachNode)

with open('json_file\hard.json', 'r') as file:
    pipeline_data = json.load(file)

# Extract edges 
edges = get_pipeline_edges2(pipeline_data)
print(edges)
# Create label mapping and graph
nodes = pipeline_data['pipelines'][0]['nodes']
node_id_to_label = {node['id']: node['app_data']['ui_data']['label'] for node in nodes if 'app_data' in node and 'ui_data' in node['app_data']}
G = nx.DiGraph([(node_id_to_label.get(src, src), node_id_to_label.get(dest, dest)) for src, dest in edges])

# Identify initial sources and final targets
initial_sources = [node for node in G.nodes() if G.in_degree(node) == 0]
final_targets = [node for node in G.nodes() if G.out_degree(node) == 0]

# Create a list of edges from initial sources to final targets
source_to_target_edges = []
for source in initial_sources:
    for target in final_targets:
        paths = list(nx.all_simple_paths(G, source, target))
        if paths:
            source_to_target_edges.append((source, target))

edges_df = pd.DataFrame(source_to_target_edges, columns=['Initial Source', 'Final Target'])

#print(edges_df)

import pandas as pd

expanded_rows = []

for index, row in edges_df.iterrows():
    source_node = row['Initial Source']
    target_node = row['Final Target']
    source_fields = field_in_eachNode.get(source_node, [])
    target_fields = field_in_eachNode.get(target_node, [])
    # For every source and target combo, add a row to df
    for source_field in source_fields:
        for target_field in target_fields:
            if source_field == target_field:
                expanded_rows.append({
                    'Initial Source': source_node,
                    'Source Field': source_field,
                    'Final Target': target_node,
                    'Target Field': target_field
                })

expanded_df = pd.DataFrame(expanded_rows)

excel_path = 'hard_outcome.xlsx'
expanded_df.to_excel(excel_path, index=False)

print(f"Expanded edges with fields saved to {excel_path}")
