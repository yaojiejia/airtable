# Acuity-Airtable SDK

Python SDK for connecting Acuity Scheduling to Airtable. Provides high-level API for fetching appointments, managing tables, and syncing data.

## Features

- Fetch Acuity intake forms with flexible time windows
- Discover form types and analyze form structure
- Switch between Airtable tables dynamically
- Sync data from Acuity to Airtable automatically
- Export to CSV with grouping by form type
- Field mapping and comparison tools

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```python
from acuity_airtable_sdk import AcuityAirtableSDK

sdk = AcuityAirtableSDK()

# Get intake forms
forms = sdk.acuity.get_intake_forms(hours=24)

# Sync to Airtable
results = sdk.sync(hours=24)

# Export to CSV
sdk.export_to_csv(group_by_appointment_type=True)
```

## Configuration

Create a `.env` file:

```
ACUITY_USER_ID=your_user_id
ACUITY_API_KEY=your_api_key
AIRTABLE_API_KEY=your_airtable_key
AIRTABLE_BASE_ID=appXXXXXXXXXXXXXX
AIRTABLE_TABLE_NAME=Your Table Name
```

## SDK Structure

### Initialization

```python
from acuity_airtable_sdk import AcuityAirtableSDK

# Use environment variables
sdk = AcuityAirtableSDK()

# Or provide credentials
sdk = AcuityAirtableSDK(
    acuity_user_id="12345",
    acuity_api_key="your_key",
    airtable_api_key="your_key",
    airtable_base_id="appXXX",
    airtable_table_name="TableName"
)
```

### Sub-SDKs

- `sdk.acuity` - Acuity operations
- `sdk.airtable` - Airtable operations
- `sdk.csv` - CSV export operations

## Acuity Operations

### Get Intake Forms

```python
forms = sdk.acuity.get_intake_forms(hours=24, include_canceled=False)
```

### Get Form Types

```python
form_types = sdk.acuity.get_all_form_types(hours=168)
```

### Get Columns by Form Type

```python
columns = sdk.acuity.get_columns_by_form_type("Appointment Type Name")
# Returns: {'top_level': {...}, 'form_fields': {...}, 'all_fields': {...}}
```

### Get All Columns Grouped

```python
all_columns = sdk.acuity.get_all_columns_by_form_type(hours=168)
# Returns: {'Form Type 1': {...}, 'Form Type 2': {...}, ...}
```

### Get Appointment by ID

```python
appointment = sdk.acuity.get_form_by_id(1234567)
```

## Airtable Operations

### Switch Tables

```python
sdk.airtable.use_table("Another Table")
```

### Get Columns

```python
columns = sdk.airtable.get_columns()
```

### Get Records

```python
records = sdk.airtable.get_records(max_records=100)
```

### Inject Single Record

```python
record = sdk.airtable.inject_record(
    acuity_record,
    verbose=True,
    timestamp_field="Last Updated"  # Optional
)
```

### Get Matching Fields

```python
matching = sdk.airtable.get_matching_fields(acuity_record)
```

## Integration Operations

### Sync Acuity to Airtable

```python
results = sdk.sync(
    hours=24,
    include_canceled=False,
    verbose=True,
    timestamp_field="Last Updated"  # Optional
)

print(f"Forms fetched: {results['forms_fetched']}")
print(f"Successful: {results['successful']}")
print(f"Failed: {results['failed']}")
```

### Export to CSV

```python
# Grouped by form type
files = sdk.export_to_csv(
    hours=24,
    include_canceled=True,
    group_by_appointment_type=True,
    output_dir="exports"
)

# Single file
files = sdk.export_to_csv(group_by_appointment_type=False)
```

### Compare Fields

```python
comparison = sdk.get_field_comparison(acuity_record)

print(comparison['matching'])        # Fields in both systems
print(comparison['only_in_acuity'])  # Only in Acuity
print(comparison['only_in_airtable']) # Only in Airtable
```

## Examples

### Daily Sync Script

```python
from acuity_airtable_sdk import AcuityAirtableSDK

sdk = AcuityAirtableSDK()
results = sdk.sync(hours=24, timestamp_field="Last Updated")

if results['failed'] > 0:
    print(f"Warning: {results['failed']} records failed")
```

### Form Analysis

```python
sdk = AcuityAirtableSDK()

form_types = sdk.acuity.get_all_form_types(hours=720)

for form_type in form_types:
    columns = sdk.acuity.get_columns_by_form_type(form_type)
    print(f"{form_type}: {len(columns['all_fields'])} fields")
```

### Multi-Table Sync

```python
sdk = AcuityAirtableSDK()

tables = {
    "Form Type A": "Table 1",
    "Form Type B": "Table 2",
}

for form_type, table_name in tables.items():
    sdk.airtable.use_table(table_name)
    # Fetch and filter forms by type, then sync
```

### Weekly CSV Export

```python
sdk = AcuityAirtableSDK()

files = sdk.export_to_csv(
    hours=168,  # 7 days
    group_by_appointment_type=True,
    output_dir="weekly_reports"
)

print(f"Generated {len(files)} files")
```

## Running Examples

### Generic SDK Examples

```bash
# See all examples
python example.py

# Run specific example
python example.py 1  # Basic sync
python example.py 2  # Explore forms
python example.py 3  # CSV export
```

### Business Use Case Example

Complete implementation of a production workflow:

```bash
# See all workflows
python example_business_use_case.py

# Run daily sync (24 hours)
python example_business_use_case.py 1

# Run daily sync (48 hours)
python example_business_use_case.py 2

# Analyze form types
python example_business_use_case.py 3

# Check field coverage
python example_business_use_case.py 4

# Generate weekly report
python example_business_use_case.py 5

# Production scheduled sync
python example_business_use_case.py 6 48
```

This example demonstrates:
- Daily sync to "Student Profile" table
- Automatic "Last Update" timestamp
- CSV export grouped by form type
- Including cancelled appointments
- Field coverage analysis
- Weekly reporting

## Implementing Your Business Logic

The SDK is generic and doesn't include business-specific logic. Here's how to implement your use case:

### Example: Daily Student Sync

```python
from acuity_airtable_sdk import AcuityAirtableSDK

def daily_student_sync(lookback_hours=24):
    sdk = AcuityAirtableSDK()
    
    # Target your specific table
    sdk.airtable.use_table("Student Profile")
    
    # Sync with your specific requirements
    results = sdk.sync(
        hours=lookback_hours,
        include_canceled=True,  # Your business rule
        timestamp_field="Last Update",  # Your timestamp field
        verbose=True
    )
    
    # Export to CSV for your analysis
    csv_files = sdk.export_to_csv(
        hours=lookback_hours,
        include_canceled=True,
        group_by_appointment_type=True,  # Group by form type
        output_dir="forms_csv"
    )
    
    return results, csv_files

# Run daily via scheduler
if __name__ == "__main__":
    daily_student_sync(48)
```

See `example_business_use_case.py` for a complete implementation with error handling, analysis tools, and production scheduling.

## Project Structure

```
.
├── acuity_airtable_sdk.py          # Main SDK
├── config.py                        # Configuration
├── csv_logger.py                    # CSV operations
├── example.py                       # Generic SDK examples
├── example_business_use_case.py    # Business logic example
├── acuity/
│   ├── acuity_client.py            # Acuity API client
│   └── acuity_intake_check.py      # Legacy wrapper
├── airtable/
│   ├── airtable_client.py          # Airtable API client
│   └── airtable_utils.py           # Legacy wrapper
├── requirements.txt
└── README.md
```

## Error Handling

```python
sdk = AcuityAirtableSDK()

try:
    results = sdk.sync(hours=24, verbose=False)
    
    if results['failed'] > 0:
        for error in results['errors']:
            form = error['form']
            error_msg = error['error']
            print(f"Failed: {form.get('client_name')} - {error_msg}")
            
except Exception as e:
    print(f"Sync failed: {e}")
```

## API Reference

### AcuitySDK

| Method | Description |
|--------|-------------|
| `get_intake_forms(hours, include_canceled)` | Get intake forms from last N hours |
| `get_all_form_types(hours, include_canceled)` | Get unique form types |
| `get_columns_by_form_type(form_type, hours)` | Get columns for specific form type |
| `get_all_columns_by_form_type(hours)` | Get columns for all form types |
| `get_form_by_id(appointment_id)` | Get specific appointment by ID |

### AirtableSDK

| Method | Description |
|--------|-------------|
| `use_table(table_name)` | Switch to different table |
| `get_columns()` | Get all column names |
| `get_records(max_records)` | Get records from table |
| `inject_record(acuity_record, verbose, timestamp_field)` | Inject Acuity record |
| `get_matching_fields(acuity_record)` | Get matching field names |

### CSVSDK

| Method | Description |
|--------|-------------|
| `export_forms_grouped(forms, output_dir, by_appointment_type)` | Export forms to CSV |

### AcuityAirtableSDK

| Method | Description |
|--------|-------------|
| `sync(hours, include_canceled, verbose, timestamp_field)` | Sync Acuity to Airtable |
| `export_to_csv(hours, include_canceled, group_by_appointment_type, output_dir)` | Export to CSV |
| `get_field_comparison(acuity_form)` | Compare fields between systems |

## Best Practices

1. Use environment variables for credentials
2. Handle errors gracefully with try/except
3. Use appropriate time windows (24h for daily, 168h for weekly)
4. Group CSV exports by form type for better analysis
5. Test with small time windows first

## Troubleshooting

### No forms found

Check time window and whether forms have intake data:

```python
forms = sdk.acuity.get_intake_forms(hours=168)  # Try last week
form_types = sdk.acuity.get_all_form_types(hours=168)
```

### Fields not matching

Use field comparison:

```python
comparison = sdk.get_field_comparison(forms[0])
print(comparison['only_in_acuity'])
print(comparison['only_in_airtable'])
```

### Sync fails with 422 error

Check field type mismatches. Multi-select fields need arrays:

```python
columns = sdk.airtable.get_columns()
# SDK handles multi-select automatically
```

## License

This SDK is provided as-is for integrating Acuity Scheduling with Airtable.

## Support

For issues or questions, please refer to:
- Acuity API: https://developers.acuityscheduling.com/
- Airtable API: https://airtable.com/developers/web/api/introduction
