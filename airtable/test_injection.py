"""
Test file for Acuity to Airtable injection with mock data.
Tests the mapping functionality without making real API calls.
Can also inject mock data into Airtable with --inject flag.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
from update_students import map_acuity_to_airtable, push_acuity_to_airtable, get_all_column_names
import json

# Load environment variables
load_dotenv()

# Get Airtable credentials from environment
API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")


# Mock Acuity data - Different test cases
MOCK_ACUITY_RECORDS = [
    {
        "appointment_id": 9999991,
        "client_name": "TEST STUDENT 2",
        "email": "sarah.chen@nyu.edu",
        "phone": "+16465551234",
        "datetime": "2026-02-15T14:00:00-0500",
        "appointment_type": "FREE | Startup Essentials | Steph Shyu",
        "forms": [
            {
                "id": 3039749,
                "name": "2025 Startup Essentials (Q&A)",
                "values": [
                    {"id": 1, "fieldID": 100, "name": "What is your current NYU status?", "value": "Master's Student"},
                    {"id": 2, "fieldID": 101, "name": "What is/was your NYU Net ID? (e.g. abc123)", "value": "sc1234"},
                    {"id": 3, "fieldID": 102, "name": "What is your NYU school affiliation?", "value": "Tandon School of Engineering"},
                    {"id": 4, "fieldID": 103, "name": "Current Students and Alumni: What is/was your degree/program (e.g., B.S. in Economics)?", "value": "M.S. in Computer Science"},
                    {"id": 5, "fieldID": 104, "name": "If you are an MBA Student, what is your MBA status?", "value": "N/A"},
                    {"id": 6, "fieldID": 105, "name": "What is/was your graduation year?", "value": "2026"},
                    {"id": 7, "fieldID": 106, "name": "List all the names of all other founding team members and their NYU affiliations, if any (student/faculty, school, degree/program, graduation year). ", "value": "Michael Zhang (Stern, MBA, 2026), Lisa Park (no NYU affiliation)"},
                    {"id": 8, "fieldID": 107, "name": "What is your venture name?", "value": "EcoTrack AI"},
                    {"id": 9, "fieldID": 108, "name": "What is your venture stage?", "value": "MVP launched (with users)"},
                    {"id": 10, "fieldID": 109, "name": "Are you a first-time founder? (Check all that apply)", "value": "Yes"},
                    {"id": 11, "fieldID": 110, "name": "How would you describe your venture and its unique value proposition?", "value": "EcoTrack AI is a sustainability platform that uses computer vision and machine learning to help businesses track and reduce their carbon footprint in real-time. Our unique value is providing actionable insights that integrate seamlessly with existing supply chain management systems."},
                ]
            }
        ]
    },
    {
        "appointment_id": 9999992,
        "client_name": "James Rodriguez",
        "email": "james.r@stern.nyu.edu",
        "phone": "+12125559876",
        "datetime": "2026-02-20T10:30:00-0500",
        "appointment_type": "FREE | Legal Help Desk | Peter Rothberg",
        "forms": [
            {
                "id": 2864882,
                "name": "2025 Legal Help Desk | Client Intake Form",
                "values": [
                    {"id": 1, "fieldID": 200, "name": "What is your current NYU status?", "value": "Alumnus"},
                    {"id": 2, "fieldID": 201, "name": "What is/was your NYU Net ID? (e.g. abc123)", "value": "jr5678"},
                    {"id": 3, "fieldID": 202, "name": "What is your NYU school affiliation?", "value": "Leonard N. Stern School of Business"},
                    {"id": 4, "fieldID": 203, "name": "Current Students and Alumni: What is/was your degree/program (e.g., B.S. in Economics)?", "value": "MBA"},
                    {"id": 5, "fieldID": 204, "name": "If you are an MBA Student, what is your MBA status?", "value": "N/A"},
                    {"id": 6, "fieldID": 205, "name": "What is/was your graduation year?", "value": "2024"},
                    {"id": 7, "fieldID": 206, "name": "List all the names of all other founding team members and their NYU affiliations, if any (student/faculty, school, degree/program, graduation year). ", "value": ""},
                    {"id": 8, "fieldID": 207, "name": "What is your venture name?", "value": "HealthBridge Solutions"},
                    {"id": 9, "fieldID": 208, "name": "What is your venture stage?", "value": "Paying customers"},
                    {"id": 10, "fieldID": 209, "name": "Are you a first-time founder? (Check all that apply)", "value": "No - I have previous experience as a solopreneur/consultant"},
                ]
            }
        ]
    },
    {
        "appointment_id": 9999993,
        "client_name": "Emily Thompson",
        "email": "emily.t@nyu.edu",
        "phone": "+19175552468",
        "datetime": "2026-03-01T16:00:00-0500",
        "appointment_type": "FREE | Product Development Help Desk | Eric Chan",
        "forms": [
            {
                "id": 2864898,
                "name": "2025 Product Dev Help Desk | Client Intake Form",
                "values": [
                    {"id": 1, "fieldID": 300, "name": "What is your current NYU status?", "value": "Current Student"},
                    {"id": 2, "fieldID": 301, "name": "What is/was your NYU Net ID? (e.g. abc123)", "value": "et9012"},
                    {"id": 3, "fieldID": 302, "name": "What is your NYU school affiliation?", "value": "School of Professional Studies"},
                    {"id": 4, "fieldID": 303, "name": "Current Students and Alumni: What is/was your degree/program (e.g., B.S. in Economics)?", "value": "M.S. in Marketing Analytics"},
                    {"id": 5, "fieldID": 304, "name": "If you are an MBA Student, what is your MBA status?", "value": "N/A"},
                    {"id": 6, "fieldID": 305, "name": "What is/was your graduation year?", "value": "2027"},
                    {"id": 7, "fieldID": 306, "name": "List all the names of all other founding team members and their NYU affiliations, if any (student/faculty, school, degree/program, graduation year). ", "value": "David Kim (Tisch, MFA, 2026), Rachel Adams (no affiliation)"},
                    {"id": 8, "fieldID": 307, "name": "What is your venture name?", "value": "CreativeFlow Studio"},
                    {"id": 9, "fieldID": 308, "name": "What is your venture stage?", "value": "Prototype"},
                    {"id": 10, "fieldID": 309, "name": "Are you a first-time founder? (Check all that apply)", "value": "Yes, No - I sold a previous company"},
                    {"id": 11, "fieldID": 310, "name": "How would you describe your venture and its unique value proposition?", "value": "CreativeFlow is a collaborative design platform that combines AI-powered design suggestions with real-time team collaboration. We help creative teams work faster and produce better results by streamlining the feedback process and providing intelligent design recommendations."},
                ]
            }
        ]
    }
]


def test_mapping(record_index=0):
    """
    Test the Acuity to Airtable mapping with mock data.
    
    Args:
        record_index: Which mock record to test (0, 1, or 2)
    """
    if record_index >= len(MOCK_ACUITY_RECORDS):
        print(f"[ERROR] Invalid record index. Available: 0-{len(MOCK_ACUITY_RECORDS)-1}")
        return
    
    mock_record = MOCK_ACUITY_RECORDS[record_index]
    
    print("\n" + "="*80)
    print(f"TESTING MOCK ACUITY RECORD #{record_index + 1}")
    print("="*80)
    
    print(f"\nMock Acuity Data:")
    print(f"  Appointment ID: {mock_record['appointment_id']}")
    print(f"  Client: {mock_record['client_name']}")
    print(f"  Email: {mock_record['email']}")
    print(f"  Phone: {mock_record['phone']}")
    print(f"  Type: {mock_record['appointment_type']}")
    
    # Map the data
    print("\n" + "-"*80)
    print("MAPPING TO AIRTABLE FORMAT:")
    print("-"*80)
    
    mapped_data = map_acuity_to_airtable(mock_record)
    
    print(f"\nTotal fields mapped: {len(mapped_data)}")
    print("\nMapped Data (what would be inserted into Airtable):\n")
    
    for i, (field_name, field_value) in enumerate(mapped_data.items(), 1):
        # Format value display
        if isinstance(field_value, list):
            value_display = f"[Array: {field_value}]"
        elif isinstance(field_value, str) and len(field_value) > 80:
            value_display = field_value[:80] + "..."
        else:
            value_display = field_value
        
        print(f"{i:2d}. {field_name}")
        print(f"    VALUE: {value_display}")
        print()
    
    # Show as JSON
    print("\n" + "="*80)
    print("JSON OUTPUT (for debugging):")
    print("="*80 + "\n")
    
    print(json.dumps(mapped_data, indent=2, ensure_ascii=False))
    
    print("\n" + "="*80 + "\n")
    
    return mapped_data


def test_all_records():
    """Test all mock records."""
    print("\n" + "="*80)
    print("TESTING ALL MOCK RECORDS")
    print("="*80 + "\n")
    
    for i in range(len(MOCK_ACUITY_RECORDS)):
        test_mapping(i)
        print("\n")


def compare_mock_records():
    """Compare the fields across different mock records."""
    print("\n" + "="*80)
    print("COMPARING FIELDS ACROSS MOCK RECORDS")
    print("="*80 + "\n")
    
    all_fields = set()
    record_fields = []
    
    for i, record in enumerate(MOCK_ACUITY_RECORDS):
        mapped = map_acuity_to_airtable(record)
        fields = set(mapped.keys())
        record_fields.append(fields)
        all_fields.update(fields)
        print(f"Record {i+1} ({record['client_name']}): {len(fields)} fields")
    
    print(f"\nTotal unique fields across all records: {len(all_fields)}")
    
    # Find common fields
    common_fields = record_fields[0]
    for fields in record_fields[1:]:
        common_fields = common_fields.intersection(fields)
    
    print(f"Common fields in ALL records: {len(common_fields)}")
    print("\nCommon fields:")
    for field in sorted(common_fields):
        print(f"  - {field}")
    
    # Find unique fields per record
    print("\n" + "-"*80)
    print("UNIQUE FIELDS PER RECORD:")
    print("-"*80 + "\n")
    
    for i, fields in enumerate(record_fields):
        unique = fields - common_fields
        if unique:
            print(f"Record {i+1} ({MOCK_ACUITY_RECORDS[i]['client_name']}):")
            for field in sorted(unique):
                print(f"  - {field}")
            print()


def inject_mock_record(record_index=0):
    """
    Actually inject a mock record into Airtable.
    
    Args:
        record_index: Which mock record to inject (0, 1, or 2)
    """
    if not API_KEY or not BASE_ID or not TABLE_NAME:
        print("[ERROR] Missing Airtable credentials!")
        print("Please set AIRTABLE_API_KEY, AIRTABLE_BASE_ID, and AIRTABLE_TABLE_NAME in your .env file")
        return None
    
    if record_index >= len(MOCK_ACUITY_RECORDS):
        print(f"[ERROR] Invalid record index. Available: 0-{len(MOCK_ACUITY_RECORDS)-1}")
        return None
    
    mock_record = MOCK_ACUITY_RECORDS[record_index]
    
    print("\n" + "="*80)
    print(f"INJECTING MOCK RECORD #{record_index + 1} INTO AIRTABLE")
    print("="*80)
    
    print(f"\nMock Record to Inject:")
    print(f"  Client: {mock_record['client_name']}")
    print(f"  Email: {mock_record['email']}")
    print(f"  Venture: {[f.get('values', [{}])[7].get('value') for f in mock_record.get('forms', [])]}")
    
    # Get Airtable columns for exact matching
    print("\nStep 1: Getting Airtable column names...")
    airtable_columns_raw = get_all_column_names(API_KEY, BASE_ID, TABLE_NAME)
    airtable_columns_set = set(col.strip() for col in airtable_columns_raw)
    print(f"[OK] Found {len(airtable_columns_set)} Airtable columns")
    
    # Get matching fields (all fields that exist in both)
    print("\nStep 2: Mapping fields...")
    mapped_data = map_acuity_to_airtable(mock_record)
    matching_fields = set(k.strip() for k in mapped_data.keys()).intersection(airtable_columns_set)
    print(f"[OK] {len(matching_fields)} fields will be inserted")
    
    # Inject into Airtable
    print("\nStep 3: Injecting into Airtable...")
    print("-"*80)
    
    try:
        created_record = push_acuity_to_airtable(
            API_KEY,
            BASE_ID,
            TABLE_NAME,
            mock_record,
            matching_fields=matching_fields,
            airtable_columns=airtable_columns_raw
        )
        
        print("\n" + "="*80)
        print("[SUCCESS] Mock record successfully injected into Airtable!")
        print("="*80)
        print(f"Airtable Record ID: {created_record['id']}")
        print(f"Table: {TABLE_NAME}")
        print(f"Fields inserted: {len(created_record['fields'])}")
        print("\nGo to Airtable to see the record:")
        print(f"https://airtable.com/{BASE_ID}")
        print("="*80 + "\n")
        
        return created_record
        
    except Exception as e:
        print(f"\n[ERROR] Failed to inject record: {e}")
        return None


def inject_all_mock_records():
    """Inject all mock records into Airtable."""
    print("\n" + "="*80)
    print("INJECTING ALL MOCK RECORDS INTO AIRTABLE")
    print("="*80 + "\n")
    
    results = []
    for i in range(len(MOCK_ACUITY_RECORDS)):
        result = inject_mock_record(i)
        results.append(result)
        if i < len(MOCK_ACUITY_RECORDS) - 1:
            print("\n" + "-"*80 + "\n")
    
    # Summary
    successful = sum(1 for r in results if r is not None)
    print("\n" + "="*80)
    print(f"INJECTION SUMMARY: {successful}/{len(results)} records successfully inserted")
    print("="*80 + "\n")
    
    return results


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--all":
            # Test all records
            test_all_records()
        elif sys.argv[1] == "--compare":
            # Compare records
            compare_mock_records()
        elif sys.argv[1] == "--inject":
            # Inject first mock record into Airtable
            if len(sys.argv) > 2:
                try:
                    index = int(sys.argv[2]) - 1
                    inject_mock_record(index)
                except ValueError:
                    print("Usage: python test_injection.py --inject [1|2|3]")
            else:
                inject_mock_record(0)
        elif sys.argv[1] == "--inject-all":
            # Inject all mock records into Airtable
            inject_all_mock_records()
        else:
            # Test specific record by index
            try:
                index = int(sys.argv[1]) - 1
                test_mapping(index)
            except ValueError:
                print("Usage: python test_injection.py [1|2|3|--all|--compare|--inject|--inject-all]")
    else:
        # Default: test first record
        print("\nUsage:")
        print("  python test_injection.py              # Test record 1 (no injection)")
        print("  python test_injection.py 2            # Test record 2 (no injection)")
        print("  python test_injection.py 3            # Test record 3 (no injection)")
        print("  python test_injection.py --all        # Test all records (no injection)")
        print("  python test_injection.py --compare    # Compare all records")
        print("  python test_injection.py --inject     # INJECT record 1 into Airtable")
        print("  python test_injection.py --inject 2   # INJECT record 2 into Airtable")
        print("  python test_injection.py --inject 3   # INJECT record 3 into Airtable")
        print("  python test_injection.py --inject-all # INJECT all 3 records into Airtable")
        print()
        test_mapping(0)

