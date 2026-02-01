"""
Example Business Use Case: Daily Student Profile Sync

This example demonstrates how to implement a daily sync workflow
that was previously in main.py using the generic SDK.
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


def analyze_form_types():
    """
    Analyze what form types are available and their field structure.
    Useful for understanding data before syncing.
    """
    print("="*80)
    print("FORM TYPE ANALYSIS")
    print("="*80 + "\n")
    
    sdk = AcuityAirtableSDK()
    
    form_types = sdk.acuity.get_all_form_types(hours=168)
    
    print(f"Found {len(form_types)} different form types:\n")
    
    for i, form_type in enumerate(form_types, 1):
        columns = sdk.acuity.get_columns_by_form_type(form_type, hours=168)
        print(f"{i}. {form_type}")
        print(f"   Total fields: {len(columns['all_fields'])}")
        print(f"   Top-level: {len(columns['top_level'])}")
        print(f"   Form fields: {len(columns['form_fields'])}")
        print()


def compare_fields_coverage():
    """
    Compare Acuity fields with Airtable Student Profile table.
    Shows what will be synced and what won't.
    """
    print("="*80)
    print("FIELD COVERAGE ANALYSIS")
    print("="*80 + "\n")
    
    sdk = AcuityAirtableSDK()
    
    sdk.airtable.use_table("Student Profile")
    
    forms = sdk.acuity.get_intake_forms(hours=168)
    
    if not forms:
        print("No forms found to analyze")
        return
    
    print(f"Analyzing {len(forms)} form(s)...\n")
    
    for form in forms[:5]:
        comparison = sdk.get_field_comparison(form)
        
        total_acuity = len(comparison['matching']) + len(comparison['only_in_acuity'])
        coverage = len(comparison['matching']) / total_acuity * 100 if total_acuity > 0 else 0
        
        print(f"Form: {form.get('appointment_type')}")
        print(f"Client: {form.get('client_name')}")
        print(f"  Matching fields: {len(comparison['matching'])}")
        print(f"  Only in Acuity: {len(comparison['only_in_acuity'])}")
        print(f"  Coverage: {coverage:.1f}%")
        print()


def weekly_report():
    """
    Generate weekly report of all intake forms.
    Exports CSVs grouped by form type for analysis.
    """
    print("="*80)
    print("WEEKLY REPORT GENERATION")
    print("="*80 + "\n")
    
    sdk = AcuityAirtableSDK()
    
    print("Exporting last 7 days of data...")
    
    csv_files = sdk.export_to_csv(
        hours=168,
        include_canceled=True,
        group_by_appointment_type=True,
        output_dir="weekly_reports"
    )
    
    print(f"\nGenerated {len(csv_files)} report file(s):")
    
    forms = sdk.acuity.get_intake_forms(hours=168, include_canceled=True)
    form_counts = {}
    
    for form in forms:
        form_type = form.get('appointment_type', 'Unknown')
        form_counts[form_type] = form_counts.get(form_type, 0) + 1
    
    for form_type, filepath in csv_files.items():
        count = form_counts.get(form_type, 0)
        print(f"  {form_type}: {count} form(s) -> {filepath}")
    
    print()


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
        "3": ("Analyze Form Types", analyze_form_types),
        "4": ("Field Coverage Analysis", compare_fields_coverage),
        "5": ("Weekly Report", weekly_report),
        "6": ("Scheduled Sync (Production)", scheduled_sync),
    }
    
    if len(sys.argv) > 1 and sys.argv[1] in use_cases:
        name, func = use_cases[sys.argv[1]]
        print(f"\nRunning: {name}\n")
        func()
    else:
        print("\nBusiness Use Case Examples")
        print("="*60)
        print("\nAvailable workflows:")
        for num, (name, _) in use_cases.items():
            print(f"  {num}. {name}")
        print("\nUsage:")
        print("  python example_business_use_case.py <number>")
        print("\nExamples:")
        print("  python example_business_use_case.py 1")
        print("  python example_business_use_case.py 6 48")
        print()

