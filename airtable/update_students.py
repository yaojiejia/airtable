"""
Utility functions for Airtable operations - Student Profile management.
"""

from pyairtable import Api
from typing import List, Dict


def get_all_column_names(api_key: str, base_id: str, table_name: str) -> List[str]:
    """
    Fetch all column names (field names) from an Airtable table.
    
    Note: Fetches multiple records to ensure we capture all columns,
    since empty fields don't appear in individual records.
    
    Args:
        api_key: Your Airtable API key
        base_id: Your Airtable base ID
        table_name: The name of the table
        
    Returns:
        List of unique column names sorted alphabetically
    """
    # Initialize the Airtable API
    api = Api(api_key)
    
    # Get the table
    table = api.table(base_id, table_name)
    
    # Fetch multiple records to get all possible fields
    # Since empty fields don't appear in records, we need to check many records
    records = table.all(max_records=100)
    
    if not records:
        print("No records found in the table.")
        return []
    
    # Collect all unique field names from all records
    all_column_names = set()
    for record in records:
        all_column_names.update(record['fields'].keys())
    
    # Return sorted list of column names
    return sorted(list(all_column_names))


def print_column_names(api_key: str, base_id: str, table_name: str):
    """
    Fetch and print all column names from an Airtable table.
    
    Args:
        api_key: Your Airtable API key
        base_id: Your Airtable base ID
        table_name: The name of the table
    """
    print(f"\nFetching column names from table: {table_name}")
    print("="*80)
    
    column_names = get_all_column_names(api_key, base_id, table_name)
    
    if column_names:
        print(f"\nFound {len(column_names)} columns:\n")
        for i, column in enumerate(column_names, 1):
            print(f"  {i}. {column}")
        print("\n" + "="*80 + "\n")
    else:
        print("No columns found.\n")
    
    return column_names


def map_acuity_to_airtable(acuity_record: dict) -> dict:
    """
    Map Acuity intake form data to Airtable field format.
    Handles special field types like multi-select fields.
    
    Args:
        acuity_record: Single Acuity intake form record
        
    Returns:
        Dictionary with Airtable field names and values
    """
    airtable_data = {}
    
    # Fields that are multi-select in Airtable (should be arrays)
    # Based on field names containing "Check all that apply"
    multi_select_indicators = ['check all that apply', 'select all']
    
    # Map top-level fields
    if acuity_record.get('client_name'):
        airtable_data['Name'] = acuity_record['client_name']
    
    if acuity_record.get('email'):
        airtable_data['What is your email?'] = acuity_record['email']
    
    # Map intake form fields
    for form in acuity_record.get('forms', []):
        for field in form.get('values', []):
            field_name = field.get('name', '').strip()
            field_value = field.get('value')
            
            # Skip if field_name is empty
            if not field_name:
                continue
            
            # Skip if field_value is None or empty string
            if field_value is None or (isinstance(field_value, str) and not field_value.strip()):
                continue
            
            # Clean string values
            if isinstance(field_value, str):
                field_value = field_value.strip()
            
            # Check if this is a multi-select field
            is_multi_select = any(indicator in field_name.lower() for indicator in multi_select_indicators)
            
            if is_multi_select:
                # Convert to array if it's not already
                if isinstance(field_value, str):
                    # Split by comma if multiple values, otherwise wrap in array
                    if ',' in field_value:
                        airtable_data[field_name] = [v.strip() for v in field_value.split(',')]
                    else:
                        airtable_data[field_name] = [field_value]
                elif isinstance(field_value, list):
                    airtable_data[field_name] = field_value
                else:
                    airtable_data[field_name] = [str(field_value)]
            else:
                # Regular field - use as is
                airtable_data[field_name] = field_value
    
    return airtable_data


def insert_record_to_airtable(api_key: str, base_id: str, table_name: str, record_data: dict) -> dict:
    """
    Insert a new record into Airtable.
    
    Args:
        api_key: Airtable API key
        base_id: Airtable base ID
        table_name: Table name
        record_data: Dictionary with field names and values
        
    Returns:
        Created record from Airtable
    """
    api = Api(api_key)
    table = api.table(base_id, table_name)
    
    # Create the record
    created_record = table.create(record_data)
    
    return created_record


def push_acuity_to_airtable(api_key: str, base_id: str, table_name: str, acuity_record: dict, 
                            matching_fields: set = None, airtable_columns: List[str] = None) -> dict:
    """
    Map and push an Acuity intake form record to Airtable.
    Only pushes fields that exist in both systems (if matching_fields provided).
    Automatically adds "Last Update" timestamp.
    
    Args:
        api_key: Airtable API key
        base_id: Airtable base ID
        table_name: Table name
        acuity_record: Acuity intake form record
        matching_fields: Set of field names that exist in both systems (optional)
        airtable_columns: List of actual Airtable column names for exact matching
        
    Returns:
        Created Airtable record
    """
    from datetime import datetime
    
    # Map the Acuity data to Airtable format
    mapped_data = map_acuity_to_airtable(acuity_record)
    
    # Create a mapping from stripped names to actual Airtable column names
    if airtable_columns:
        name_mapping = {col.strip(): col for col in airtable_columns}
    else:
        name_mapping = {}
    
    # Filter to only matching fields and map to exact Airtable column names
    filtered_data = {}
    for k, v in mapped_data.items():
        k_stripped = k.strip()
        if matching_fields and k_stripped in matching_fields:
            # Use the exact Airtable column name if available
            exact_name = name_mapping.get(k_stripped, k_stripped)
            filtered_data[exact_name] = v
        elif not matching_fields:
            # No filtering, use as-is
            filtered_data[k] = v
    
    # Add "Last Update" field with current date (YYYY-MM-DD format for Airtable date field)
    last_update_field = name_mapping.get("Last Update", "Last Update")
    filtered_data[last_update_field] = datetime.now().strftime("%Y-%m-%d")
    
    print(f"\nMapping Acuity record to Airtable...")
    print(f"Acuity Appointment ID: {acuity_record.get('appointment_id')}")
    print(f"Client: {acuity_record.get('client_name')}")
    print(f"Fields to insert: {len(filtered_data)} (includes auto-generated 'Last Update')")
    
    # Display what will be inserted
    print("\nData to be inserted:")
    for field_name, field_value in filtered_data.items():
        preview = str(field_value)[:50] + "..." if len(str(field_value)) > 50 else str(field_value)
        marker = " [AUTO]" if field_name == last_update_field else ""
        print(f"  - {field_name}: {preview}{marker}")
    
    # Insert the record
    print("\nInserting into Airtable...")
    created_record = insert_record_to_airtable(api_key, base_id, table_name, filtered_data)
    
    print(f"[SUCCESS] Record created with ID: {created_record['id']}")
    
    return created_record


if __name__ == "__main__":
    # For testing purposes
    from airtable_main import API_KEY, BASE_ID, TABLE_NAME
    
    try:
        print_column_names(API_KEY, BASE_ID, TABLE_NAME)
    except Exception as e:
        print(f"Error: {e}")

