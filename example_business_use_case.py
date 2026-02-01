"""
Business Use Case Example: Daily Student Profile Sync

Demonstrates implementing your specific business logic using the generic SDK.
"""
import sys
from datetime import datetime
from acuity_airtable_sdk import AcuityAirtableSDK


def daily_student_sync(lookback_hours=24):
    """
    Daily sync workflow for student profiles.
    
    Features:
    - Fetch forms from last N hours
    - Inject to Student Profile table in Airtable
    - Add "Last Update" timestamp automatically
    - Log all records to CSV grouped by form type
    - Include cancelled appointments
    """
    print("="*80)
    print("DAILY STUDENT PROFILE SYNC")
    print("="*80)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Lookback period: {lookback_hours} hours")
    print("="*80 + "\n")
    
    sdk = AcuityAirtableSDK()
    
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




def scheduled_sync():
    """
    Production sync script for scheduled execution.
    Runs once per day via Task Scheduler or cron.
    """
    try:
        lookback_hours = int(sys.argv[1]) if len(sys.argv) > 1 else 48
        
        results = daily_student_sync(lookback_hours)
        
        if results['failed'] > 0:
            print(f"WARNING: {results['failed']} record(s) failed to sync")
            for error in results['errors']:
                print(f"  Failed: {error['form'].get('client_name')} - {error['error']}")
            sys.exit(1)
        
        sys.exit(0)
        
    except Exception as e:
        print(f"ERROR: Sync failed - {e}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    use_cases = {
        "1": ("Daily Sync (24 hours)", lambda: daily_student_sync(24)),
        "2": ("Daily Sync (48 hours)", lambda: daily_student_sync(48)),
        "3": ("Scheduled Sync (Production)", scheduled_sync),
    }
    
    if len(sys.argv) > 1 and sys.argv[1] in use_cases:
        name, func = use_cases[sys.argv[1]]
        print(f"\nRunning: {name}\n")
        func()
    else:
        print("\nBusiness Use Case: Daily Student Sync")
        print("="*60)
        print("\nAvailable options:")
        for num, (name, _) in use_cases.items():
            print(f"  {num}. {name}")
        print("\nUsage:")
        print("  python example_business_use_case.py <number>")
        print("  python example_business_use_case.py 3 <hours>")
        print()

