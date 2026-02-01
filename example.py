"""
Acuity-Airtable SDK Usage Examples
"""
from acuity_airtable_sdk import AcuityAirtableSDK


def example_basic_sync():
    """Basic sync from Acuity to Airtable"""
    sdk = AcuityAirtableSDK()
    
    results = sdk.sync(hours=24, verbose=True)
    print(f"Synced {results['successful']} records")


def example_explore_forms():
    """Explore available form types and their fields"""
    sdk = AcuityAirtableSDK()
    
    form_types = sdk.acuity.get_all_form_types(hours=168)
    print(f"Found {len(form_types)} form types:")
    
    for form_type in form_types:
        columns = sdk.acuity.get_columns_by_form_type(form_type)
        print(f"  {form_type}: {len(columns['all_fields'])} fields")


def example_csv_export():
    """Export forms to CSV grouped by type"""
    sdk = AcuityAirtableSDK()
    
    files = sdk.export_to_csv(
        hours=24,
        group_by_appointment_type=True,
        output_dir="exports"
    )
    
    print(f"Exported to {len(files)} CSV files")


def example_multi_table():
    """Work with multiple Airtable tables"""
    sdk = AcuityAirtableSDK()
    
    print(f"Current table: {sdk.airtable.table_name}")
    columns1 = sdk.airtable.get_columns()
    print(f"Columns: {len(columns1)}")
    
    sdk.airtable.use_table("Another Table")
    print(f"Switched to: {sdk.airtable.table_name}")


def example_with_timestamp():
    """Sync with automatic timestamp field"""
    sdk = AcuityAirtableSDK()
    
    results = sdk.sync(
        hours=24,
        timestamp_field="Last Updated",
        verbose=True
    )
    print(f"Synced with timestamp: {results['successful']} records")


if __name__ == "__main__":
    import sys
    
    examples = {
        "1": ("Basic Sync", example_basic_sync),
        "2": ("Explore Forms", example_explore_forms),
        "3": ("CSV Export", example_csv_export),
        "4": ("Multi-Table", example_multi_table),
        "5": ("With Timestamp", example_with_timestamp),
    }
    
    if len(sys.argv) > 1 and sys.argv[1] in examples:
        name, func = examples[sys.argv[1]]
        print(f"\n{'='*60}")
        print(f"EXAMPLE: {name}")
        print(f"{'='*60}\n")
        func()
    else:
        print("\nAcuity-Airtable SDK Examples")
        print("="*60)
        for num, (name, _) in examples.items():
            print(f"  {num}. {name}")
        print("\nUsage: python example.py <number>")

