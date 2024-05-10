import json

# Path to the JSON file
file_path = 'json_file\PROJDEF.json'

# Function to load JSON data and transform parameters into a list of dictionaries
def load_and_transform_parameters(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    
    # Navigate to the 'parameters' section
    # You should adjust the path to where the parameters are located in your actual JSON structure
    parameters = data['entity']['parameter_set']['parameters']

    # Transform parameters into the desired format
    transformed_parameters = [{
        "Name": param['name'],
        "Type": param['type'],
        "Default Value": param['value'],
        "Prompt": param['prompt']
    } for param in parameters]

    return transformed_parameters

# Example usage
if __name__ == "__main__":
    parameters = load_and_transform_parameters(file_path)
    for param in parameters:
        print(param)
