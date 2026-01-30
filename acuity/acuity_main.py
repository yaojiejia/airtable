"""
Continuously monitor Acuity for new intake form submissions.
Usage: python acuity_main.py [interval_minutes]
Example: python acuity_main.py 60  (checks every 60 minutes)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
from acuity_intake_check import AcuityIntakeChecker
import time
from datetime import datetime
from typing import Set

# Load environment variables
load_dotenv()

# Get Acuity credentials from environment
ACUITY_USER_ID = os.getenv("ACUITY_USER_ID")
ACUITY_API_KEY = os.getenv("ACUITY_API_KEY")

# Default configuration
DEFAULT_CHECK_INTERVAL_MINUTES = 5


class AcuityMonitor(AcuityIntakeChecker):
    """Monitor Acuity for new intake forms - extends AcuityIntakeChecker."""
    
    def __init__(self, user_id: str, api_key: str, check_interval_minutes: int = DEFAULT_CHECK_INTERVAL_MINUTES):
        super().__init__(user_id, api_key)
        self.seen_appointments: Set[int] = set()
        self.check_interval_minutes = check_interval_minutes
        self.lookback_minutes = check_interval_minutes * 2  # Look back 2x the check interval
    
    def filter_new_forms(self, forms):
        """Filter out forms we've already seen."""
        new_forms = []
        for form in forms:
            apt_id = form.get("appointment_id")
            if apt_id not in self.seen_appointments:
                new_forms.append(form)
                self.seen_appointments.add(apt_id)
        return new_forms
    
    def display_new_form(self, form_data):
        """Display new form submission to console only."""
        print("\n" + "="*80)
        print("[NEW FORM DETECTED]")
        print("="*80)
        print(f"Appointment ID: {form_data.get('appointment_id')}")
        print(f"Client: {form_data.get('client_name')}")
        print(f"Email: {form_data.get('email')}")
        print(f"Phone: {form_data.get('phone')}")
        print(f"DateTime: {form_data.get('datetime')}")
        print(f"Type: {form_data.get('appointment_type')}")
        
        for form in form_data.get('forms', []):
            print(f"\nForm: {form.get('name')}")
            print(f"Form ID: {form.get('id')}")
            print(f"Number of fields: {len(form.get('values', []))}")
            
            # Show first few questions as preview
            for i, field in enumerate(form.get('values', [])[:3], 1):
                field_value = str(field.get('value', ''))
                preview = field_value[:100] if len(field_value) > 100 else field_value
                print(f"  Q{i}: {field.get('name')}")
                print(f"  A{i}: {preview}")
        
        print("="*80 + "\n")
    
    def monitor(self):
        """Main monitoring loop - console output only."""
        print("\n" + "="*80)
        print("ACUITY INTAKE FORM MONITOR")
        print("="*80)
        print(f"Checking every {self.check_interval_minutes} minutes")
        print(f"Looking back {self.lookback_minutes} minutes each check")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)
        print("\nPress Ctrl+C to stop\n")
        
        check_count = 0
        
        try:
            while True:
                check_count += 1
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                print(f"[{current_time}] Check #{check_count}: Fetching appointments...")
                
                # Get recent forms using parent class method
                all_forms = self.get_new_intake_forms(hours=self.lookback_minutes / 60)
                
                # Filter for new forms we haven't seen
                new_forms = self.filter_new_forms(all_forms)
                
                if new_forms:
                    print(f"[OK] Found {len(new_forms)} NEW form submission(s)!")
                    
                    for form_data in new_forms:
                        # Display the form (console only)
                        self.display_new_form(form_data)
                else:
                    print(f"[OK] No new forms. Total appointments checked: {len(all_forms)}")
                
                # Wait before next check
                print(f"[WAITING] Next check in {self.check_interval_minutes} minutes...\n")
                time.sleep(self.check_interval_minutes * 60)
                
        except KeyboardInterrupt:
            print("\n\n" + "="*80)
            print("Monitor stopped by user")
            print(f"Total checks performed: {check_count}")
            print(f"Total forms processed: {len(self.seen_appointments)}")
            print("="*80 + "\n")


if __name__ == "__main__":
    try:
        # Check if credentials are loaded
        if not ACUITY_USER_ID or not ACUITY_API_KEY:
            print("[ERROR] Missing Acuity credentials!")
            print("Please set ACUITY_USER_ID and ACUITY_API_KEY in your .env file")
            sys.exit(1)
        
        # Parse command line argument for interval
        check_interval = DEFAULT_CHECK_INTERVAL_MINUTES
        
        if len(sys.argv) > 1:
            try:
                check_interval = int(sys.argv[1])
                if check_interval <= 0:
                    print(f"[ERROR] Interval must be positive. Using default: {DEFAULT_CHECK_INTERVAL_MINUTES} minutes")
                    check_interval = DEFAULT_CHECK_INTERVAL_MINUTES
            except ValueError:
                print(f"[ERROR] Invalid interval '{sys.argv[1]}'. Using default: {DEFAULT_CHECK_INTERVAL_MINUTES} minutes")
                check_interval = DEFAULT_CHECK_INTERVAL_MINUTES
        
        # Create and start monitor
        monitor = AcuityMonitor(ACUITY_USER_ID, ACUITY_API_KEY, check_interval_minutes=check_interval)
        monitor.monitor()
        
    except Exception as e:
        print(f"[ERROR] {e}")

