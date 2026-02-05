import sys
import traceback
from datetime import datetime
from acuity_airtable_sdk import AcuityAirtableSDK


def daily_student_sync(lookback_hours=24):
    print("="*80)
    print("DAILY STUDENT PROFILE SYNC")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Lookback period: {lookback_hours} hours")
    print("="*80 + "\n")
    
    form_type_keywords = [
        'help desk', 'helpdesk', 'q&a', 'q & a', 'session',
        'essentials', 'advising', 'workshop', 'clinic', 'appointment'
    ]
    
    sdk = AcuityAirtableSDK(
        form_type_keywords=form_type_keywords,
        fallback_form_name="advisor_1_on_1_session"
    )
    
    sdk.airtable.use_table("Student Profile")
    
    print("Step 1: Syncing to Airtable with timestamp...")
    results = sdk.sync(
        hours=lookback_hours,
        include_canceled=True,
        verbose=True,
        timestamp_field="Last Update"
    )
    
    print("\nStep 2: Exporting to CSV grouped by form type...")
    csv_files = sdk.export_to_csv(
        hours=lookback_hours,
        include_canceled=True,
        group_by_appointment_type=True,
        output_dir="forms_csv"
    )
    
    print(f"\nExported to {len(csv_files)} CSV file(s):")
    for form_type, filepath in csv_files.items():
        print(f"  - {form_type}: {filepath}")
    
    print("\n" + "="*80)
    print("SYNC SUMMARY")
    print("="*80)
    print(f"  Forms fetched: {results['forms_fetched']}")
    print(f"  Successfully synced: {results['successful']}")
    print(f"  Failed: {results['failed']}")
    print(f"  CSV files created: {len(csv_files)}")
    print(f"  Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80 + "\n")
    
    return results


if __name__ == "__main__":
    lookback_hours = 24
    
    if len(sys.argv) > 1:
        try:
            lookback_hours = int(sys.argv[1])
        except ValueError:
            print(f"Error: '{sys.argv[1]}' is not a valid number of hours")
            print("\nUsage:")
            print("  python example_business_use_case.py <hours>")
            print("\nExamples:")
            print("  python example_business_use_case.py 24")
            print("  python example_business_use_case.py 48")
            print("  python example_business_use_case.py 20")
            sys.exit(1)
    
    try:
        results = daily_student_sync(lookback_hours)
        
        if results['failed'] > 0:
            print(f"WARNING: {results['failed']} record(s) failed to sync")
            for error in results['errors']:
                print(f"  Failed: {error['form'].get('client_name')} - {error['error']}")
            sys.exit(1)
        
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: Sync failed - {e}")
        traceback.print_exc()
        sys.exit(2)

