import json
import ast

def convert_object_to_dict(obj):
    """
    Convert an object to Python dictionary with proper type casting.
    Fields with string values that look like arrays '[{...}]' are converted to actual arrays.
    """
    result = {}
    
    for key, value in obj.items():
        if isinstance(value, str):
            # Check if the string value looks like an array (starts with [ and ends with ])
            stripped_value = value.strip()
            if stripped_value.startswith('[') and stripped_value.endswith(']'):
                try:
                    # Try to parse as JSON first (more reliable)
                    result[key] = json.loads(stripped_value)
                except json.JSONDecodeError:
                    try:
                        # Fallback to ast.literal_eval for Python-like syntax
                        result[key] = ast.literal_eval(stripped_value)
                    except (ValueError, SyntaxError):
                        # If parsing fails, keep as string
                        result[key] = value
            else:
                result[key] = value
        else:
            # Keep non-string values as they are
            result[key] = value
    
    return result

# Example usage with your sample object
sample_obj = {
    'orgno': '2021000076', 
    'vehicles_meta': '[{"vehicle_status":"I trafik","vehicle_type":"Personbil","leased":"Nej","count":39456}, {"vehicle_status":"I trafik","vehicle_type":"Moped","leased":"Nej","count":8768}, {"vehicle_status":"I trafik","vehicle_type":"Lätt lastbil","leased":"Nej","count":8768}, {"vehicle_status":"I trafik","vehicle_type":"Motorcykel","leased":"Nej","count":8768}, {"vehicle_status":"Avst","vehicle_type":"Motorcykel","leased":"Nej","count":13152}, {"vehicle_status":"Avst","vehicle_type":"Personbil","leased":"Nej","count":35072}]'
}

# Convert the object
converted_dict = convert_object_to_dict(sample_obj)

# Print the result
print("Original object:")
print(sample_obj)
print("\nConverted dictionary:")
print(converted_dict)
print(f"\nType of 'vehicles_meta' field: {type(converted_dict['vehicles_meta'])}")
print(f"Number of vehicles in array: {len(converted_dict['vehicles_meta'])}")

# Access individual elements from the converted array
print("\nFirst vehicle record:")
print(converted_dict['vehicles_meta'][0])