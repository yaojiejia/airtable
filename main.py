"""
Main integration script: Acuity -> Airtable + CSV
Fetches intake forms submitted since yesterday and:
1. Injects them into Airtable
2. Logs all records (including cancelled/rescheduled) to CSV

Run this script once per day (via Task Scheduler, cron, etc.)

Usage:
    python main.py              # Fetch forms from last 24 hours
    python main.py 48           # Fetch forms from last 48 hours
"""

import sys
import os
import json
import csv
from datetime import datetime, timedelta
from typing import Set, List, Dict, Optional
from pathlib import Path

# Add paths for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'acuity'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'airtable'))

from dotenv import load_dotenv
from acuity_intake_check import AcuityIntakeChecker
from update_students import push_acuity_to_airtable, get_all_column_names

# Load environment variables
load_dotenv()

# Acuity credentials
ACUITY_USER_ID = os.getenv("ACUITY_USER_ID")
ACUITY_API_KEY = os.getenv("ACUITY_API_KEY")

# Airtable credentials
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

# File to track processed appointment IDs
PROCESSED_IDS_FILE = "processed_appointments.json"

# CSV file to log all Acuity records
CSV_LOG_FILE = "acuity_records_log.csv"

# Directory for form-specific CSV files
FORMS_CSV_DIR = "forms_csv"


class DailyAcuityAirtableSync:
    """Daily sync between Acuity and Airtable - runs once and exits."""
    
    def __init__(self, lookback_hours: int = 24):
        """
        Initialize the daily sync.
        
        Args:
            lookback_hours: How many hours back to fetch forms (default: 24)
        """
        self.lookback_hours = lookback_hours
        
        # Initialize Acuity checker
        self.acuity_checker = AcuityIntakeChecker(ACUITY_USER_ID, ACUITY_API_KEY)
        
        # Cache Airtable columns for matching
        self.airtable_columns = None
        
        # Initialize CSV log file
        self.init_csv_log()
        
        # Create forms CSV directory
        self.init_forms_csv_dir()
    
    def save_run_log(self, total_processed: int):
        """Save a simple log of the last run."""
        try:
            data = {
                'last_run': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_processed': total_processed
            }
            with open(PROCESSED_IDS_FILE, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"[WARNING] Could not save run log: {e}")
    
    def init_csv_log(self):
        """Initialize CSV log file with headers if it doesn't exist."""
        if not os.path.exists(CSV_LOG_FILE):
            try:
                with open(CSV_LOG_FILE, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        'Sync Timestamp',
                        'Appointment ID',
                        'Client Name',
                        'Email',
                        'Phone',
                        'Appointment DateTime',
                        'Appointment Type',
                        'Status',
                        'Canceled',
                        'Date Created',
                        'Action',
                        'Injected to Airtable',
                        'Airtable Record ID',
                        'Notes'
                    ])
                print(f"[INFO] Created CSV log file: {CSV_LOG_FILE}")
            except Exception as e:
                print(f"[WARNING] Could not create CSV log file: {e}")
    
    def init_forms_csv_dir(self):
        """Create directory for form-specific CSV files."""
        if not os.path.exists(FORMS_CSV_DIR):
            try:
                os.makedirs(FORMS_CSV_DIR)
                print(f"[INFO] Created forms CSV directory: {FORMS_CSV_DIR}")
            except Exception as e:
                print(f"[WARNING] Could not create forms CSV directory: {e}")
    
    def get_form_csv_filename(self, appointment_type: str) -> str:
        """
        Generate a clean CSV filename from appointment type.
        Extracts the help desk/session name and ignores instructor names.
        
        Args:
            appointment_type: Full appointment type string
            
        Returns:
            Clean filename (e.g., "product_development_help_desk.csv")
        """
        import re
        
        # Split by pipe (|) to get parts
        parts = [p.strip() for p in appointment_type.split('|')]
        
        # Find the part that contains the actual session/help desk name
        # Usually it's the one with keywords like "Help Desk", "Q&A", "Session", "Essentials"
        session_keywords = [
            'help desk', 'helpdesk', 'q&a', 'q & a', 'session', 
            'essentials', 'advising', 'workshop', 'clinic'
        ]
        
        form_name = None
        for part in parts:
            part_lower = part.lower()
            # Skip parts that are just price indicators or prefixes
            if re.match(r'^(free|paid|\$\d+)', part_lower):
                continue
            # Skip parts with names in parentheses (likely instructor names)
            if '(' in part and ')' in part:
                # Extract the part before parentheses
                before_paren = part.split('(')[0].strip()
                if any(keyword in before_paren.lower() for keyword in session_keywords):
                    form_name = before_paren
                    break
                continue
            # Check if this part contains a session keyword
            if any(keyword in part_lower for keyword in session_keywords):
                form_name = part
                break
        
        # If no keyword match, check if this looks like a person's name
        if not form_name:
            # Check if appointment type is just a name (typically: FirstName LastName)
            # Names usually: short, 2-4 words, capitalized, no special keywords
            is_likely_name = (
                len(parts) <= 2 and 
                len(appointment_type.split()) <= 4 and
                appointment_type[0].isupper() and
                not any(keyword in appointment_type.lower() for keyword in session_keywords)
            )
            
            if is_likely_name:
                # Use generic name for advisor/1-on-1 appointments
                form_name = "advisor_1_on_1_session"
            else:
                # Filter out short parts and parts that are likely names
                meaningful_parts = [
                    p for p in parts 
                    if len(p) > 10 and not re.match(r'^(free|paid|\$\d+)', p.lower())
                ]
                if meaningful_parts:
                    form_name = meaningful_parts[0]
                else:
                    form_name = appointment_type
        
        # Clean up the name
        # Remove any prefix like "Current Students Only:"
        form_name = re.sub(r'^[^:]+:\s*', '', form_name)
        
        # Remove price prefixes
        form_name = re.sub(r'^(FREE|PAID|\$\d+)\s*\|\s*', '', form_name, flags=re.IGNORECASE)
        
        # Remove any remaining parenthetical content
        form_name = re.sub(r'\s*\([^)]*\)', '', form_name)
        
        # Convert to lowercase and replace spaces/special chars with underscores
        cleaned = form_name.lower()
        cleaned = re.sub(r'[^a-z0-9]+', '_', cleaned)
        cleaned = cleaned.strip('_')
        
        # Limit length and ensure it's not empty
        if not cleaned or len(cleaned) < 3:
            cleaned = "unknown_form_type"
        elif len(cleaned) > 100:
            cleaned = cleaned[:100]
        
        return f"{cleaned}.csv"
    
    def save_form_to_csv(self, acuity_record: Dict):
        """
        Save intake form questions and answers to form-specific CSV.
        
        Args:
            acuity_record: Acuity appointment record with forms
        """
        try:
            appointment_type = acuity_record.get('appointment_type', 'unknown')
            csv_filename = self.get_form_csv_filename(appointment_type)
            csv_filepath = os.path.join(FORMS_CSV_DIR, csv_filename)
            
            # Extract form data
            forms = acuity_record.get('forms', [])
            if not forms:
                return
            
            # Get all form field names and values
            form_data = {}
            form_data['Sync Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            form_data['Appointment ID'] = acuity_record.get('appointment_id', '')
            form_data['Client Name'] = acuity_record.get('client_name', '')
            form_data['Email'] = acuity_record.get('email', '')
            form_data['Phone'] = acuity_record.get('phone', '')
            form_data['Appointment DateTime'] = acuity_record.get('datetime', '')
            form_data['Canceled'] = 'Yes' if acuity_record.get('canceled', False) else 'No'
            
            # Add all form field Q&A
            for form in forms:
                for field in form.get('values', []):
                    field_name = field.get('name', '').strip()
                    field_value = field.get('value', '')
                    if field_name:
                        form_data[field_name] = field_value
            
            # Check if file exists to determine if we need to write headers
            file_exists = os.path.exists(csv_filepath)
            
            # Read existing headers if file exists
            existing_headers = []
            if file_exists:
                try:
                    with open(csv_filepath, 'r', newline='', encoding='utf-8') as f:
                        reader = csv.reader(f)
                        existing_headers = next(reader, [])
                except Exception:
                    existing_headers = []
            
            # Merge headers (existing + new fields)
            all_headers = list(existing_headers) if existing_headers else [
                'Sync Timestamp', 'Appointment ID', 'Client Name', 'Email',
                'Phone', 'Appointment DateTime', 'Canceled'
            ]
            
            # Add any new form fields to headers
            for key in form_data.keys():
                if key not in all_headers:
                    all_headers.append(key)
            
            # If file exists and headers changed, we need to rewrite with new headers
            if file_exists and existing_headers and set(all_headers) != set(existing_headers):
                # Read all existing data
                existing_data = []
                try:
                    with open(csv_filepath, 'r', newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        existing_data = list(reader)
                except Exception as e:
                    print(f"[WARNING] Could not read existing CSV: {e}")
                
                # Rewrite file with new headers
                try:
                    with open(csv_filepath, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=all_headers)
                        writer.writeheader()
                        # Write existing rows
                        for row in existing_data:
                            writer.writerow(row)
                        # Write new row
                        writer.writerow(form_data)
                except Exception as e:
                    print(f"[WARNING] Could not rewrite CSV with new headers: {e}")
            else:
                # Normal append (or create new file)
                try:
                    with open(csv_filepath, 'a' if file_exists else 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=all_headers)
                        if not file_exists:
                            writer.writeheader()
                        writer.writerow(form_data)
                except Exception as e:
                    print(f"[WARNING] Could not write to form CSV: {e}")
            
        except Exception as e:
            print(f"[WARNING] Could not save form to CSV: {e}")
    
    def log_to_csv(self, acuity_record: Dict, action: str, injected: bool = False, 
                    airtable_record_id: str = '', notes: str = ''):
        """
        Log an Acuity record to CSV.
        
        Args:
            acuity_record: The Acuity appointment record
            action: What action was taken (NEW, CANCELLED, RESCHEDULED, DUPLICATE)
            injected: Whether it was injected to Airtable
            airtable_record_id: Airtable record ID if injected
            notes: Additional notes
        """
        try:
            with open(CSV_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    acuity_record.get('appointment_id', ''),
                    acuity_record.get('client_name', ''),
                    acuity_record.get('email', ''),
                    acuity_record.get('phone', ''),
                    acuity_record.get('datetime', ''),
                    acuity_record.get('appointment_type', ''),
                    'Cancelled' if action == 'CANCELLED' else 'Active',
                    'Yes' if action == 'CANCELLED' else 'No',
                    acuity_record.get('dateCreated', ''),
                    action,
                    'Yes' if injected else 'No',
                    airtable_record_id,
                    notes
                ])
        except Exception as e:
            print(f"[WARNING] Could not log to CSV: {e}")
    
    def load_airtable_columns(self):
        """Load and cache Airtable column names for field matching."""
        print("\n[STEP 1] Loading Airtable column names...")
        self.airtable_columns = get_all_column_names(
            AIRTABLE_API_KEY, 
            AIRTABLE_BASE_ID, 
            AIRTABLE_TABLE_NAME
        )
        print(f"[OK] Found {len(self.airtable_columns)} Airtable columns")
    
    def get_matching_fields(self, acuity_record: Dict) -> Set[str]:
        """
        Get fields that match between Acuity form and Airtable.
        
        Args:
            acuity_record: Acuity intake form record
            
        Returns:
            Set of matching field names
        """
        # Get Acuity field names
        acuity_fields = set()
        acuity_fields.add("Name")
        acuity_fields.add("What is your email?")
        
        for form in acuity_record.get('forms', []):
            for field in form.get('values', []):
                field_name = field.get('name', '').strip()
                if field_name:
                    acuity_fields.add(field_name)
        
        # Get Airtable field names (stripped)
        airtable_fields = set(col.strip() for col in self.airtable_columns)
        
        # Find matching fields
        matching = acuity_fields.intersection(airtable_fields)
        
        return matching
    
    def inject_to_airtable(self, acuity_record: Dict, index: int, total: int) -> Optional[str]:
        """
        Inject an Acuity record into Airtable and log to CSV.
        
        Args:
            acuity_record: Acuity intake form record
            index: Current record number (for display)
            total: Total number of records to process
            
        Returns:
            Airtable record ID if successful, None otherwise
        """
        apt_id = acuity_record.get('appointment_id')
        is_cancelled = acuity_record.get('canceled', False)
        
        try:
            # Get matching fields for this record
            matching_fields = self.get_matching_fields(acuity_record)
            
            status_label = "[CANCELLED]" if is_cancelled else ""
            print(f"\n{'='*80}")
            print(f"[{index}/{total}] PROCESSING {status_label}: {acuity_record.get('client_name')}")
            print(f"{'='*80}")
            print(f"  Acuity ID: {apt_id}")
            print(f"  Email: {acuity_record.get('email')}")
            print(f"  Date/Time: {acuity_record.get('datetime')}")
            print(f"  Status: {'CANCELLED' if is_cancelled else 'ACTIVE'}")
            print(f"  Matching fields: {len(matching_fields)}")
            print(f"{'='*80}\n")
            
            # Always inject to Airtable (even if cancelled)
            created_record = push_acuity_to_airtable(
                AIRTABLE_API_KEY,
                AIRTABLE_BASE_ID,
                AIRTABLE_TABLE_NAME,
                acuity_record,
                matching_fields=matching_fields,
                airtable_columns=self.airtable_columns
            )
            
            airtable_id = created_record['id']
            
            print(f"\n[SUCCESS] Injected into Airtable!")
            print(f"  Airtable Record ID: {airtable_id}")
            print(f"  Fields inserted: {len(created_record['fields'])}")
            print(f"{'='*80}\n")
            
            # Log to main CSV
            action = 'CANCELLED' if is_cancelled else 'PROCESSED'
            self.log_to_csv(
                acuity_record, 
                action=action,
                injected=True,
                airtable_record_id=airtable_id,
                notes='Injected successfully'
            )
            
            # Save form Q&A to form-specific CSV
            self.save_form_to_csv(acuity_record)
            
            return airtable_id
            
        except Exception as e:
            print(f"\n[ERROR] Failed to inject record: {e}")
            print(f"{'='*80}\n")
            
            # Still log to CSV even if injection failed
            action = 'CANCELLED' if is_cancelled else 'PROCESSED'
            self.log_to_csv(
                acuity_record,
                action=action,
                injected=False,
                notes=f'Injection failed: {str(e)}'
            )
            
            return None
    
    def fetch_and_inject_all(self):
        """Fetch all forms from the lookback period and inject ALL of them."""
        print(f"\n[STEP 2] Fetching intake forms from last {self.lookback_hours} hours...")
        
        # Calculate the time range
        now = datetime.now()
        since = now - timedelta(hours=self.lookback_hours)
        
        print(f"  From: {since.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  To:   {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Fetch ALL forms including cancelled
        all_forms = self.acuity_checker.get_new_intake_forms(
            hours=self.lookback_hours, 
            include_canceled=True
        )
        
        print(f"[OK] Found {len(all_forms)} total form(s) in this period")
        
        # Count by status
        active_count = sum(1 for f in all_forms if not f.get('canceled', False))
        cancelled_count = len(all_forms) - active_count
        print(f"  Active: {active_count}, Cancelled: {cancelled_count}")
        
        if not all_forms:
            print("\n[INFO] No forms found in this period.")
            return []
        
        # Process ALL forms (no duplication check)
        print(f"\n[STEP 3] Processing ALL {len(all_forms)} record(s)...")
        print("="*80)
        
        successful = []
        failed = []
        
        for i, form in enumerate(all_forms, 1):
            apt_id = form.get("appointment_id")
            
            # Inject into Airtable and log to CSV
            airtable_id = self.inject_to_airtable(form, i, len(all_forms))
            
            if airtable_id:
                successful.append(apt_id)
            else:
                failed.append(apt_id)
        
        return successful, failed
    
    def run(self):
        """Main execution - run once and exit."""
        start_time = datetime.now()
        
        print("\n" + "="*80)
        print("ACUITY -> AIRTABLE DAILY SYNC")
        print("="*80)
        print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Lookback period: {self.lookback_hours} hours")
        print(f"Target Airtable table: {AIRTABLE_TABLE_NAME}")
        print("="*80)
        
        try:
            # Load Airtable columns
            self.load_airtable_columns()
            
            # Fetch and inject all new forms
            results = self.fetch_and_inject_all()
            
            if results:
                successful, failed = results
            else:
                successful, failed = [], []
            
            # Save run log
            total_processed = len(successful) + len(failed)
            self.save_run_log(total_processed)
            
            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print("\n" + "="*80)
            print("SYNC COMPLETE")
            print("="*80)
            print(f"  Duration: {duration:.1f} seconds")
            print(f"  Successfully injected: {len(successful)}")
            print(f"  Failed: {len(failed)}")
            print(f"  Total processed this run: {total_processed}")
            print(f"  Finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*80 + "\n")
            
            if failed:
                print(f"[WARNING] {len(failed)} record(s) failed to inject")
                print(f"Failed appointment IDs: {failed}\n")
            
            return len(successful), len(failed)
            
        except KeyboardInterrupt:
            print("\n\n[INTERRUPTED] Sync stopped by user")
            return None, None
        except Exception as e:
            print(f"\n[ERROR] Sync failed: {e}")
            return None, None


if __name__ == "__main__":
    # Check credentials
    if not ACUITY_USER_ID or not ACUITY_API_KEY:
        print("[ERROR] Missing Acuity credentials!")
        print("Please set ACUITY_USER_ID and ACUITY_API_KEY in your .env file")
        sys.exit(1)
    
    if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID or not AIRTABLE_TABLE_NAME:
        print("[ERROR] Missing Airtable credentials!")
        print("Please set AIRTABLE_API_KEY, AIRTABLE_BASE_ID, and AIRTABLE_TABLE_NAME in your .env file")
        sys.exit(1)
    
    # Parse command line argument for lookback hours
    lookback_hours = 24  # Default: last 24 hours
    
    if len(sys.argv) > 1:
        try:
            lookback_hours = int(sys.argv[1])
            if lookback_hours <= 0:
                print(f"[ERROR] Hours must be positive. Using default: 24 hours")
                lookback_hours = 24
        except ValueError:
            print(f"[ERROR] Invalid hours '{sys.argv[1]}'. Using default: 24 hours")
            lookback_hours = 24
    
    # Create and run sync
    sync = DailyAcuityAirtableSync(lookback_hours=lookback_hours)
    successful, failed = sync.run()
    
    # Exit with appropriate code
    if successful is None:
        sys.exit(2)  # Error occurred
    elif failed and len(failed) > 0:
        sys.exit(1)  # Some records failed
    else:
        sys.exit(0)  # All good
