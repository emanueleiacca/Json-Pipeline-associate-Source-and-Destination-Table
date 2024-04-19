import json
import networkx as nx
import matplotlib.pyplot as plt
from utils import *
with open('json_file\hard.json', 'r') as file:
    pipeline_data = json.load(file)

edges = get_pipeline_edges(pipeline_data)

# code from label_to_node.py
primary_pipeline_id = pipeline_data['pipelines'][0]['id']
nodes = pipeline_data['pipelines'][0]['nodes']
node_id_to_label = {node['id']: node['app_data']['ui_data']['label'] for node in nodes if 'app_data' in node and 'ui_data' in node['app_data']}

edges_with_labels = [(node_id_to_label.get(src, src), node_id_to_label.get(dest, dest)) for src, dest in edges]


G = nx.DiGraph()

# Add nodes with labels to the graph
for node_id, label in node_id_to_label.items():
    G.add_node(label)

# Add edges with labels to the graph
for src, dest in edges_with_labels:
    G.add_edge(src, dest)

plt.figure(figsize=(15, 15))  
pos = nx.spring_layout(G) 
nx.draw(G, pos, with_labels=True, node_color='skyblue', node_size=2000, edge_color='k', linewidths=1, font_size=12)

plt.show()
