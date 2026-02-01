"""
Acuity Scheduling API utilities.
Backward-compatible wrapper around acuity_client.
"""
import json
import sys
import os
from typing import List, Dict, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from acuity.acuity_client import AcuityClient, IntakeFormService
from config import config

ACUITY_USER_ID = config.ACUITY_USER_ID
ACUITY_API_KEY = config.ACUITY_API_KEY
BASE_URL = config.ACUITY_BASE_URL


class AcuityIntakeChecker:
    """Wrapper for AcuityClient and IntakeFormService."""
    
    def __init__(self, user_id: str, api_key: str):
        self._client = AcuityClient(user_id, api_key)
        self._service = IntakeFormService(self._client)
        self.auth = (user_id, api_key)
        self.base_url = BASE_URL
    
    def get_appointments(
        self, 
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        max_results: int = 100
    ) -> List[Dict]:
        return self._client.get_appointments(min_date, max_date, max_results)
    
    def get_recent_appointments_with_forms(
        self, 
        hours: int = 24, 
        include_canceled: bool = False
    ) -> List[Dict]:
        return self._client.get_appointments_with_forms(hours, include_canceled)
    
    def get_new_intake_forms(
        self, 
        hours: int = 1, 
        include_canceled: bool = False
    ) -> List[Dict]:
        return self._service.get_recent_forms(hours, include_canceled)
    
    def get_appointment_by_id(self, appointment_id: int) -> Optional[Dict]:
        return self._client.get_appointment_by_id(appointment_id)
    
    def fetch_one_record(self, hours: int = 24) -> Optional[Dict]:
        return self._service.get_single_form(hours)
    
    def print_one_record_as_json(self, hours: int = 24):
        """
        Fetch one intake form record and print it as JSON to console.
        
        Args:
            hours: Number of hours to look back (default: 24)
        """
        print(f"Fetching one intake form from the last {hours} hours...")
        
        record = self.fetch_one_record(hours=hours)
        
        if not record:
            print("No intake forms found.")
            return None
        
        # Print as JSON
        print("\n" + "="*80)
        print("JSON OUTPUT (One Record):")
        print("="*80 + "\n")
        
        try:
            json_output = json.dumps(record, indent=2, ensure_ascii=False)
            print(json_output)
        except UnicodeEncodeError:
            # Fallback to ASCII if there are encoding issues
            json_output = json.dumps(record, indent=2, ensure_ascii=True)
            print(json_output)
        
        print("\n" + "="*80 + "\n")
        
        return record
    
    def print_intake_forms(self, appointments: List[Dict]):
        """Pretty print intake form data."""
        if not appointments:
            print("No intake forms found.")
            return
        
        print(f"\n{'='*80}")
        print(f"Found {len(appointments)} appointment(s) with intake forms:")
        print(f"{'='*80}\n")
        
        for i, apt in enumerate(appointments, 1):
            try:
                print(f"Appointment #{i}:")
                print(f"  ID: {apt.get('appointment_id')}")
                print(f"  Client: {apt.get('client_name')}")
                print(f"  Email: {apt.get('email')}")
                print(f"  Phone: {apt.get('phone')}")
                print(f"  Date/Time: {apt.get('datetime')}")
                print(f"  Type: {apt.get('appointment_type')}")
                print(f"\n  Intake Form Responses:")
                
                for form in apt.get('forms', []):
                    print(f"    Form ID: {form.get('id')}")
                    print(f"    Form Name: {form.get('name', 'N/A')}")
                    
                    for field in form.get('values', []):
                        field_name = field.get('name', 'Unknown Field')
                        field_value = field.get('value', 'N/A')
                        # Handle Unicode encoding issues on Windows
                        try:
                            print(f"      - {field_name}: {field_value}")
                        except UnicodeEncodeError:
                            # Replace problematic characters
                            safe_value = str(field_value).encode('ascii', 'replace').decode('ascii')
                            print(f"      - {field_name}: {safe_value}")
                
                print(f"\n{'-'*80}\n")
            except Exception as e:
                print(f"  Error printing appointment #{i}: {e}")
                print(f"\n{'-'*80}\n")


def main():
    """Main function to check for new intake forms"""
    
    # Initialize the checker
    checker = AcuityIntakeChecker(ACUITY_USER_ID, ACUITY_API_KEY)
    
    # Check for forms submitted in the last 24 hours
    print("Checking for intake forms submitted in the last 24 hours...")
    new_forms = checker.get_new_intake_forms(hours=24)
    
    # Display the results in readable format (console only)
    checker.print_intake_forms(new_forms)
    
    # Return the data for further processing
    return new_forms


def main_one_record():
    """Fetch and display ONE intake form record as JSON"""
    
    # Initialize the checker
    checker = AcuityIntakeChecker(ACUITY_USER_ID, ACUITY_API_KEY)
    
    # Fetch and print one record as JSON
    record = checker.print_one_record_as_json(hours=24)
    
    return record


if __name__ == "__main__":
    import sys
    
    try:
        # Check if credentials are loaded
        if not ACUITY_USER_ID or not ACUITY_API_KEY:
            print("Error: Missing Acuity credentials!")
            print("Please set ACUITY_USER_ID and ACUITY_API_KEY in your .env file")
            exit(1)
        
        # Check if user wants to fetch just one record
        if len(sys.argv) > 1 and sys.argv[1] == "--one":
            # Fetch and display one record as JSON
            record = main_one_record()
        else:
            # Default: fetch all records in last 24 hours
            intake_forms = main()
        
        # Console output only - no file saving
        
    except Exception as e:
        print(f"Error: {e}")

