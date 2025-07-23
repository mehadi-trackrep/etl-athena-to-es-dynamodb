import boto3
import os
import dotenv
from pprint import pprint

dotenv.load_dotenv()

import boto3
import json

def inspect_dynamodb_table(table_name):
    """
    Inspect DynamoDB table schema and show exactly what it expects
    """
    try:
        # Using both client and resource for comprehensive info
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_resource = boto3.resource('dynamodb')
        
        # Get detailed table info
        response = dynamodb_client.describe_table(TableName=table_name)
        table_info = response['Table']
        
        print(f"🔍 INSPECTING TABLE: {table_name}")
        print("=" * 60)
        
        # Key Schema (This is what's causing your error!)
        print("📋 KEY SCHEMA (Required fields):")
        key_schema = table_info['KeySchema']
        required_keys = {}
        
        for key in key_schema:
            key_type = "PARTITION KEY" if key['KeyType'] == 'HASH' else "SORT KEY"
            attr_name = key['AttributeName']
            
            # Find the data type for this key
            attr_type = next(
                attr['AttributeType'] 
                for attr in table_info['AttributeDefinitions'] 
                if attr['AttributeName'] == attr_name
            )
            
            required_keys[attr_name] = attr_type
            print(f"  ✅ {attr_name} ({key_type}) - Type: {attr_type}")
        
        print(f"\n📊 TABLE STATUS: {table_info['TableStatus']}")
        print(f"🏷️  TABLE ARN: {table_info['TableArn']}")
        
        # Show what your data should look like
        print(f"\n🎯 YOUR DATA MUST INCLUDE THESE KEYS:")
        print("-" * 40)
        for key_name, key_type in required_keys.items():
            type_description = {
                'S': 'String',
                'N': 'Number', 
                'B': 'Binary'
            }.get(key_type, key_type)
            print(f"  '{key_name}': <{type_description} value>")
        
        # Example of correct data structure
        print(f"\n💡 EXAMPLE CORRECT DATA STRUCTURE:")
        example_item = {}
        for key_name, key_type in required_keys.items():
            if key_type == 'S':
                example_item[key_name] = "example_string_value"
            elif key_type == 'N':
                example_item[key_name] = 123
            else:
                example_item[key_name] = f"example_{key_type}_value"
        
        print(json.dumps(example_item, indent=2))
        
        # Global Secondary Indexes (if any)
        if 'GlobalSecondaryIndexes' in table_info:
            print(f"\n🔍 GLOBAL SECONDARY INDEXES:")
            for gsi in table_info['GlobalSecondaryIndexes']:
                print(f"  Index: {gsi['IndexName']}")
                for key in gsi['KeySchema']:
                    key_type = "PARTITION" if key['KeyType'] == 'HASH' else "SORT"
                    print(f"    {key['AttributeName']} ({key_type} KEY)")
        
        return required_keys
        
    except Exception as e:
        print(f"❌ Error inspecting table: {e}")
        return None

def validate_your_data_structure():
    """
    Show what your current data looks like vs what DynamoDB expects
    """
    print("\n" + "=" * 60)
    print("🔍 YOUR CURRENT DATA STRUCTURE:")
    print("-" * 40)
    
    # This is what you showed in your error message
    sample_data = {
        'orgno': '5592902331', 
        'vehicles_meta': '[{"vehicle_status":"Avst","vehicle_type":"Personbil","leased":"Nej","count":4384}]'
    }
    
    print("Current fields in your data:")
    for key, value in sample_data.items():
        print(f"  '{key}': {type(value).__name__} = {repr(value)}")
    
    return sample_data

# Usage example:
if __name__ == "__main__":
    # Replace with your actual table name
    TABLE_NAME = "your-actual-table-name"  # UPDATE THIS!
    
    # Inspect the table
    required_keys = inspect_dynamodb_table(os.getenv('DYNAMODB_TABLE_NAME', TABLE_NAME))
    
    # Show your current data
    your_data = validate_your_data_structure()
    
    # Compare them
    if required_keys:
        print("\n" + "=" * 60)
        print("🚨 COMPARISON:")
        print("-" * 40)
        
        for required_key in required_keys:
            if required_key in your_data:
                print(f"  ✅ '{required_key}' - Present in your data")
            else:
                print(f"  ❌ '{required_key}' - MISSING from your data!")
        
        missing_keys = set(required_keys.keys()) - set(your_data.keys())
        if missing_keys:
            print(f"\n🔧 FIX: Add these missing keys to your data:")
            for key in missing_keys:
                print(f"  - {key}")