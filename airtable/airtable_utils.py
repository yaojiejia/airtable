"""
Airtable utility functions.
Backward-compatible wrapper around airtable_client.
"""
import sys
import os
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from airtable.airtable_client import AirtableClient, AirtableService, FieldMapper


def get_all_column_names(api_key: str, base_id: str, table_name: str) -> List[str]:
    """Fetch all column names from an Airtable table."""
    client = AirtableClient(api_key, base_id, table_name)
    return client.get_all_field_names()


def print_column_names(api_key: str, base_id: str, table_name: str):
    """Fetch and print all column names."""
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
    """Map Acuity intake form data to Airtable field format."""
    mapper = FieldMapper([])
    return mapper.map_acuity_to_airtable(acuity_record, matching_fields=None)


def insert_record_to_airtable(api_key: str, base_id: str, table_name: str, record_data: dict) -> dict:
    """Insert a new record into Airtable."""
    client = AirtableClient(api_key, base_id, table_name)
    return client.create_record(record_data)


def push_acuity_to_airtable(
    api_key: str, 
    base_id: str, 
    table_name: str, 
    acuity_record: dict,
    matching_fields: set = None, 
    airtable_columns: List[str] = None
) -> dict:
    """Map and push an Acuity intake form record to Airtable."""
    client = AirtableClient(api_key, base_id, table_name)
    service = AirtableService(client)
    return service.inject_acuity_record(acuity_record, verbose=True)
