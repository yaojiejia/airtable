"""
Simple Airtable script to fetch the top 5 records from a table.
"""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'acuity'))

from dotenv import load_dotenv
from pyairtable import Api
from update_students import (
    print_column_names, 
    get_all_column_names,
    push_acuity_to_airtable,
    map_acuity_to_airtable
)
from acuity_intake_check import AcuityIntakeChecker

# Load environment variables
load_dotenv()

# Get Airtable credentials from environment
API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

# Get Acuity credentials from environment
ACUITY_USER_ID = os.getenv("ACUITY_USER_ID")
ACUITY_API_KEY = os.getenv("ACUITY_API_KEY")

def fetch_top_5_records():
    """Fetch the top 5 records from an Airtable table."""
    
    # Initialize the Airtable API
    api = Api(API_KEY)
    
    # Get the table
    table = api.table(BASE_ID, TABLE_NAME)
    
    # Fetch the top 5 records
    # You can add parameters like max_records, view, formula, etc.
    records = table.all(max_records=5)
    
    # Display the records
    print(f"Fetched {len(records)} records:\n")
    
    for i, record in enumerate(records, 1):
        print(f"Record {i}:")
        print(f"  ID: {record['id']}")
        print(f"  Fields: {record['fields']}")
        print(f"  Created: {record['createdTime']}")
        print("-" * 50)
    
    return records


def get_acuity_field_names(acuity_record):
    """
    Extract all field names from an Acuity intake form record.
    Strips whitespace from field names for consistent comparison.
    
    Args:
        acuity_record: Single Acuity intake form record (dict)
        
    Returns:
        Set of field names from the intake forms (whitespace stripped)
    """
    field_names = set()
    
    # Add top-level fields
    field_names.add("Name")  # From client_name
    field_names.add("What is your email?")  # From email
    
    # Extract field names from intake forms
    for form in acuity_record.get('forms', []):
        for field in form.get('values', []):
            field_name = field.get('name')
            if field_name:
                # Strip leading/trailing whitespace
                field_names.add(field_name.strip())
    
    return field_names


def compare_fields():
    """
    Compare field names between Acuity intake forms and Airtable columns.
    Shows which fields exist in both, only in Acuity, and only in Airtable.
    """
    print("\n" + "="*80)
    print("COMPARING ACUITY AND AIRTABLE FIELDS")
    print("="*80 + "\n")
    
    # 1. Get Airtable column names (strip whitespace)
    print("Fetching Airtable columns...")
    airtable_columns_raw = get_all_column_names(API_KEY, BASE_ID, TABLE_NAME)
    airtable_columns = set(col.strip() for col in airtable_columns_raw)
    print(f"Found {len(airtable_columns)} Airtable columns\n")
    
    # 2. Get one Acuity intake form record
    print("Fetching one Acuity intake form...")
    acuity_checker = AcuityIntakeChecker(ACUITY_USER_ID, ACUITY_API_KEY)
    acuity_record = acuity_checker.fetch_one_record(hours=24)
    
    if not acuity_record:
        print("No Acuity intake forms found in the last 24 hours.")
        return
    
    # 3. Get Acuity field names
    acuity_fields = get_acuity_field_names(acuity_record)
    print(f"Found {len(acuity_fields)} Acuity form fields\n")
    
    # 4. Compare fields
    matching_fields = airtable_columns.intersection(acuity_fields)
    only_in_airtable = airtable_columns - acuity_fields
    only_in_acuity = acuity_fields - airtable_columns
    
    # 5. Display results
    print("="*80)
    print(f"MATCHING FIELDS (exist in BOTH): {len(matching_fields)}")
    print("="*80)
    if matching_fields:
        for field in sorted(matching_fields):
            print(f"  [MATCH] {field}")
    else:
        print("  (none)")
    
    print("\n" + "="*80)
    print(f"ONLY IN AIRTABLE (not in Acuity): {len(only_in_airtable)}")
    print("="*80)
    if only_in_airtable:
        for field in sorted(only_in_airtable):
            print(f"  [AIRTABLE] {field}")
    else:
        print("  (none)")
    
    print("\n" + "="*80)
    print(f"ONLY IN ACUITY (not in Airtable): {len(only_in_acuity)}")
    print("="*80)
    if only_in_acuity:
        for field in sorted(only_in_acuity):
            print(f"  [ACUITY] {field}")
    else:
        print("  (none)")
    
    print("\n" + "="*80 + "\n")
    
    return {
        "matching": matching_fields,
        "only_in_airtable": only_in_airtable,
        "only_in_acuity": only_in_acuity
    }


def inject_one_record():
    """
    Fetch one Acuity intake form and inject it into Airtable.
    Only pushes fields that match between Acuity and Airtable.
    """
    print("\n" + "="*80)
    print("INJECTING ACUITY RECORD INTO AIRTABLE")
    print("="*80 + "\n")
    
    # 1. Get Airtable columns (exact names)
    print("Step 1: Getting Airtable column names...")
    airtable_columns_raw = get_all_column_names(API_KEY, BASE_ID, TABLE_NAME)
    airtable_columns = [col.strip() for col in airtable_columns_raw]
    print(f"[OK] Found {len(airtable_columns)} Airtable columns\n")
    
    # 2. Get matching fields
    print("Step 2: Comparing fields...")
    comparison = compare_fields()
    matching_fields = comparison['matching']
    
    if not matching_fields:
        print("\n[ERROR] No matching fields found between Acuity and Airtable!")
        return None
    
    print(f"\n[OK] Found {len(matching_fields)} matching fields to map\n")
    
    # 3. Get one Acuity record
    print("Step 3: Fetching one Acuity intake form...")
    acuity_checker = AcuityIntakeChecker(ACUITY_USER_ID, ACUITY_API_KEY)
    acuity_record = acuity_checker.fetch_one_record(hours=24)
    
    if not acuity_record:
        print("\n[ERROR] No Acuity intake forms found in the last 24 hours.")
        return None
    
    print(f"[OK] Found record for: {acuity_record.get('client_name')}\n")
    
    # 4. Push to Airtable
    print("Step 4: Pushing to Airtable...")
    print("="*80)
    
    try:
        created_record = push_acuity_to_airtable(
            API_KEY, 
            BASE_ID, 
            TABLE_NAME, 
            acuity_record,
            matching_fields=matching_fields,
            airtable_columns=airtable_columns_raw  # Pass the raw column names
        )
        
        print("\n" + "="*80)
        print("[SUCCESS] Record successfully injected into Airtable!")
        print("="*80)
        print(f"Airtable Record ID: {created_record['id']}")
        print(f"Fields inserted: {len(created_record['fields'])}")
        print("\nInserted data:")
        for field_name, field_value in created_record['fields'].items():
            preview = str(field_value)[:50] + "..." if len(str(field_value)) > 50 else str(field_value)
            print(f"  - {field_name}: {preview}")
        print("="*80 + "\n")
        
        return created_record
        
    except Exception as e:
        print(f"\n[ERROR] Failed to insert record: {e}")
        return None


if __name__ == "__main__":
    try:
        # Check if credentials are loaded
        if not API_KEY or not BASE_ID or not TABLE_NAME:
            print("Error: Missing Airtable credentials!")
            print("Please set AIRTABLE_API_KEY, AIRTABLE_BASE_ID, and AIRTABLE_TABLE_NAME in your .env file")
            exit(1)
        
        if not ACUITY_USER_ID or not ACUITY_API_KEY:
            print("Error: Missing Acuity credentials!")
            print("Please set ACUITY_USER_ID and ACUITY_API_KEY in your .env file")
            exit(1)
        
        # Check command line arguments
        if len(sys.argv) > 1 and sys.argv[1] == "--compare":
            # Compare fields between Acuity and Airtable
            comparison = compare_fields()
        elif len(sys.argv) > 1 and sys.argv[1] == "--inject":
            # Inject one Acuity record into Airtable
            record = inject_one_record()
        else:
            # Default: show columns and fetch records
            # First, print all column names
            print_column_names(API_KEY, BASE_ID, TABLE_NAME)
            
            # Then fetch the top 5 records
            records = fetch_top_5_records()
            
    except Exception as e:
        print(f"Error: {e}")

