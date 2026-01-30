"""
Acuity Scheduling API script to detect new intake form submissions.
"""

import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Acuity credentials from environment
ACUITY_USER_ID = os.getenv("ACUITY_USER_ID")
ACUITY_API_KEY = os.getenv("ACUITY_API_KEY")
BASE_URL = "https://acuityscheduling.com/api/v1"


class AcuityIntakeChecker:
    """Class to interact with Acuity Scheduling API and check for intake forms."""
    
    def __init__(self, user_id: str, api_key: str):
        """Initialize with Acuity credentials."""
        self.auth = (user_id, api_key)
        self.base_url = BASE_URL
    
    def get_appointments(
        self, 
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        max_results: int = 100
    ) -> List[Dict]:
        """
        Fetch appointments from Acuity.
        
        Args:
            min_date: Start date in YYYY-MM-DD format (defaults to today)
            max_date: End date in YYYY-MM-DD format
            max_results: Maximum number of results to return
            
        Returns:
            List of appointment dictionaries
        """
        url = f"{self.base_url}/appointments"
        
        params = {
            "max": max_results
        }
        
        if min_date:
            params["minDate"] = min_date
        if max_date:
            params["maxDate"] = max_date
        
        try:
            response = requests.get(url, auth=self.auth, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching appointments: {e}")
            return []
    
    def get_recent_appointments_with_forms(self, hours: int = 24, include_canceled: bool = False) -> List[Dict]:
        """
        Get appointments from the last N hours that have intake forms filled out.
        
        Args:
            hours: Number of hours to look back (default: 24)
            include_canceled: Include cancelled appointments (default: False)
            
        Returns:
            List of appointments with intake form data
        """
        from dateutil import parser as date_parser
        from datetime import timezone
        
        # Calculate date range - use UTC timezone-aware datetime
        now = datetime.now(timezone.utc)
        cutoff_datetime = now - timedelta(hours=hours)
        
        # Acuity API only accepts dates (YYYY-MM-DD), not timestamps
        # So we need to fetch from the date and then filter by time on client side
        min_date = cutoff_datetime.strftime("%Y-%m-%d")
        
        appointments = self.get_appointments(min_date=min_date)
        
        # Filter appointments that:
        # 1. Have intake forms
        # 2. Are within the hour window (client-side filtering)
        # 3. Are not cancelled (unless include_canceled=True)
        appointments_with_forms = []
        for apt in appointments:
            # Check if has forms
            if not apt.get("forms") or len(apt.get("forms", [])) == 0:
                continue
            
            # Check if cancelled
            if not include_canceled and apt.get("canceled", False):
                continue
            
            # Check if within hour window (use datetimeCreated for when form was filled)
            datetime_created = apt.get("datetimeCreated")
            if datetime_created:
                try:
                    # Parse the datetime (format: "2026-01-27T10:59:36-0600")
                    apt_datetime = date_parser.parse(datetime_created)
                    
                    # Convert to UTC for proper comparison
                    if apt_datetime.tzinfo is not None:
                        apt_datetime_utc = apt_datetime.astimezone(timezone.utc)
                    else:
                        # If no timezone, assume local and convert to UTC
                        apt_datetime_utc = apt_datetime.replace(tzinfo=timezone.utc)
                    
                    # Only include if created after cutoff
                    if apt_datetime_utc >= cutoff_datetime:
                        appointments_with_forms.append(apt)
                except Exception as e:
                    # Skip appointments with unparseable datetimes
                    print(f"Warning: Skipping appointment {apt.get('id')} - could not parse datetime: {e}")
                    continue
            else:
                # Skip appointments without datetime
                continue
        
        return appointments_with_forms
    
    def get_new_intake_forms(self, hours: int = 1, include_canceled: bool = False) -> List[Dict]:
        """
        Get NEW intake forms submitted in the last N hours.
        Useful for checking recently submitted forms.
        
        Args:
            hours: Number of hours to look back (default: 1)
            include_canceled: Include cancelled appointments (default: False)
            
        Returns:
            List of appointments with their intake form responses (excludes cancelled by default)
        """
        appointments = self.get_recent_appointments_with_forms(hours=hours, include_canceled=include_canceled)
        
        results = []
        for apt in appointments:
            # Extract key information
            intake_data = {
                "appointment_id": apt.get("id"),
                "client_name": f"{apt.get('firstName', '')} {apt.get('lastName', '')}",
                "email": apt.get("email"),
                "phone": apt.get("phone"),
                "datetime": apt.get("datetime"),
                "appointment_type": apt.get("type"),
                "forms": apt.get("forms", [])
            }
            results.append(intake_data)
        
        return results
    
    def get_appointment_by_id(self, appointment_id: int) -> Optional[Dict]:
        """
        Get a specific appointment by ID.
        
        Args:
            appointment_id: The Acuity appointment ID
            
        Returns:
            Appointment dictionary or None
        """
        url = f"{self.base_url}/appointments/{appointment_id}"
        
        try:
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching appointment {appointment_id}: {e}")
            return None
    
    def fetch_one_record(self, hours: int = 24) -> Optional[Dict]:
        """
        Fetch one intake form record and return it as a dictionary.
        
        Args:
            hours: Number of hours to look back (default: 24)
            
        Returns:
            Single appointment dictionary or None if no forms found
        """
        forms = self.get_new_intake_forms(hours=hours)
        
        if not forms:
            return None
        
        return forms[0]
    
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

