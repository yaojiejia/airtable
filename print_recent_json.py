"""
Print full JSON of recent Acuity appointments (like intake_form_example.json)
"""
import os
import sys
import json
from dotenv import load_dotenv

# Add acuity directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'acuity'))
from acuity_intake_check import AcuityIntakeChecker

load_dotenv()

def main():
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    
    checker = AcuityIntakeChecker(
        user_id=os.getenv("ACUITY_USER_ID"),
        api_key=os.getenv("ACUITY_API_KEY")
    )
    
    print(f"\n{'='*80}")
    print(f"FETCHING APPOINTMENTS FROM LAST {hours} HOURS")
    print(f"{'='*80}\n")
    
    appointments = checker.get_recent_appointments_with_forms(hours=hours, include_canceled=False)
    
    print(f"Found {len(appointments)} appointment(s)\n")
    
    for i, apt in enumerate(appointments, 1):
        # Extract key fields like in intake_form_example.json
        formatted = {
            "appointment_id": apt.get("id"),
            "client_name": f"{apt.get('firstName', '')} {apt.get('lastName', '')}".strip(),
            "email": apt.get("email"),
            "phone": apt.get("phone"),
            "datetime": apt.get("datetime"),
            "appointment_type": apt.get("type"),
            "datetimeCreated": apt.get("datetimeCreated"),  # When form was submitted
            "canceled": apt.get("canceled", False),
            "forms": apt.get("forms", [])
        }
        
        print(f"{'='*80}")
        print(f"APPOINTMENT {i}/{len(appointments)}")
        print(f"{'='*80}")
        print(json.dumps(formatted, indent=2))
        print()

if __name__ == "__main__":
    main()

