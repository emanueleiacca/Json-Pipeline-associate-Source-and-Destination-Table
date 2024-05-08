import json
file_path = 'json_file/medium.json'
def load_local_parameters(file_path):
    # Load JSON data from file
    with open(file_path, 'r') as file:
        data = json.load(file)

    # Extract local parameters
    local_parameters = data['parameters']['local_parameters']
    
    # Return the local parameters as a list of dictionaries
    return [{
        "Name": param['name'],
        "Type": param['type'],
        "Default Value": param['value'],
        "Prompt": param['prompt']
    } for param in local_parameters]

if __name__ == "__main__":
    parameters = load_local_parameters(file_path)
    for param in parameters:
        print(f"Name: {param['Name']}")
        print(f"Type: {param['Type']}")
        print(f"Default Value: {param['Default Value']}")
        print(f"Prompt: {param['Prompt']}\n")