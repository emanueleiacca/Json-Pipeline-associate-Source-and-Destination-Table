import json

with open('json_file\hard.json', 'r') as file:
    pipeline_data = json.load(file)
    
print_pipeline_links(pipeline_data)