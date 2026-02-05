"""
Acuity Scheduling API client for fetching appointments and intake forms.
"""
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from dateutil import parser as date_parser

from config import config


class AcuityClient:
    """Client for interacting with Acuity Scheduling API."""
    
    def __init__(self, user_id: str = None, api_key: str = None):
        """
        Initialize Acuity client.
        
        Args:
            user_id: Acuity user ID (defaults to config)
            api_key: Acuity API key (defaults to config)
        """
        self.user_id = user_id or config.ACUITY_USER_ID
        self.api_key = api_key or config.ACUITY_API_KEY
        self.auth = (self.user_id, self.api_key)
        self.base_url = config.ACUITY_BASE_URL
    
    def get_appointments(
        self,
        min_date: Optional[str] = None,
        max_date: Optional[str] = None,
        max_results: int = None
    ) -> List[Dict]:
        """
        Fetch appointments from Acuity API.
        
        Args:
            min_date: Start date in YYYY-MM-DD format
            max_date: End date in YYYY-MM-DD format
            max_results: Maximum number of results to return
            
        Returns:
            List of appointment dictionaries
        """
        url = f"{self.base_url}/appointments"
        
        params = {
            "max": max_results or config.MAX_APPOINTMENTS
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
            print(f"[ERROR] Failed to fetch appointments: {e}")
            return []
    
    def get_appointment_by_id(self, appointment_id: int) -> Optional[Dict]:
        """
        Get a specific appointment by ID.
        
        Args:
            appointment_id: The Acuity appointment ID
            
        Returns:
            Appointment dictionary or None if not found
        """
        url = f"{self.base_url}/appointments/{appointment_id}"
        
        try:
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Failed to fetch appointment {appointment_id}: {e}")
            return None
    
    def get_appointments_with_forms(
        self,
        hours: int = 24,
        include_canceled: bool = False
    ) -> List[Dict]:
        """
        Get appointments from the last N hours that have intake forms.
        
        Note: Acuity API only filters by appointment date, not submission date.
        This method performs client-side filtering by form submission time.
        
        Args:
            hours: Number of hours to look back
            include_canceled: Whether to include cancelled appointments
            
        Returns:
            List of appointments with intake forms
        """
        # Calculate cutoff time (timezone-aware UTC)
        now = datetime.now(timezone.utc)
        cutoff_datetime = now - timedelta(hours=hours)
        
        # Fetch appointments (API only accepts date format)
        min_date = cutoff_datetime.strftime("%Y-%m-%d")
        appointments = self.get_appointments(min_date=min_date)
        
        # Filter appointments client-side
        filtered_appointments = []
        
        for apt in appointments:
            if self._should_include_appointment(apt, cutoff_datetime, include_canceled):
                filtered_appointments.append(apt)
        
        return filtered_appointments
    
    def _should_include_appointment(
        self,
        appointment: Dict,
        cutoff_datetime: datetime,
        include_canceled: bool
    ) -> bool:
        """
        Determine if an appointment should be included based on filters.
        
        Args:
            appointment: Appointment dictionary
            cutoff_datetime: Cutoff datetime for filtering
            include_canceled: Whether to include cancelled appointments
            
        Returns:
            True if appointment should be included
        """
        # Must have intake forms
        if not appointment.get("forms") or len(appointment.get("forms", [])) == 0:
            return False
        
        # Check if cancelled (unless including cancelled)
        if not include_canceled and appointment.get("canceled", False):
            return False
        
        # For cancelled appointments, be more lenient with datetime checking
        is_canceled = appointment.get("canceled", False)
        
        # Try to get datetimeCreated first (form submission time)
        datetime_created = appointment.get("datetimeCreated")
        
        # If no datetimeCreated, try using appointment datetime as fallback
        # This is especially important for cancelled appointments
        if not datetime_created:
            if is_canceled and include_canceled:
                # For cancelled appointments, use appointment datetime as fallback
                datetime_created = appointment.get("datetime")
                # If still no datetime, check if there's a cancellation date
                if not datetime_created:
                    # Try to use current time as fallback for very recent cancellations
                    # This ensures we capture recently cancelled appointments
                    return True  # Include if cancelled and we're including cancelled
            else:
                # For non-cancelled appointments, require datetimeCreated
                return False
        
        if not datetime_created:
            # If cancelled and including cancelled, still include it
            if is_canceled and include_canceled:
                return True
            return False
        
        try:
            # Parse and convert to UTC
            apt_datetime = date_parser.parse(datetime_created)
            
            if apt_datetime.tzinfo is not None:
                apt_datetime_utc = apt_datetime.astimezone(timezone.utc)
            else:
                apt_datetime_utc = apt_datetime.replace(tzinfo=timezone.utc)
            
            # Only include if created/appointment date is after cutoff
            return apt_datetime_utc >= cutoff_datetime
            
        except Exception as e:
            print(f"[WARNING] Skipping appointment {appointment.get('id')} - "
                  f"could not parse datetime: {e}")
            return False


class IntakeFormService:
    """Service for working with Acuity intake forms."""
    
    def __init__(self, acuity_client: AcuityClient = None):
        """
        Initialize intake form service.
        
        Args:
            acuity_client: AcuityClient instance (creates new one if not provided)
        """
        self.client = acuity_client or AcuityClient()
    
    def get_recent_forms(
        self,
        hours: int = 24,
        include_canceled: bool = False
    ) -> List[Dict]:
        """
        Get intake forms submitted in the last N hours.
        
        Args:
            hours: Number of hours to look back
            include_canceled: Whether to include cancelled appointments
            
        Returns:
            List of structured intake form records
        """
        appointments = self.client.get_appointments_with_forms(
            hours=hours,
            include_canceled=include_canceled
        )
        
        return [self._structure_appointment_data(apt) for apt in appointments]
    
    def get_single_form(self, hours: int = 24) -> Optional[Dict]:
        """
        Get a single intake form record.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            Single structured intake form record or None
        """
        forms = self.get_recent_forms(hours=hours)
        return forms[0] if forms else None
    
    def _structure_appointment_data(self, appointment: Dict) -> Dict:
        """
        Structure appointment data into a consistent format.
        
        Args:
            appointment: Raw appointment data from Acuity API
            
        Returns:
            Structured appointment dictionary
        """
        return {
            "appointment_id": appointment.get("id"),
            "client_name": f"{appointment.get('firstName', '')} {appointment.get('lastName', '')}".strip(),
            "email": appointment.get("email"),
            "phone": appointment.get("phone"),
            "datetime": appointment.get("datetime"),
            "appointment_type": appointment.get("type"),
            "canceled": appointment.get("canceled", False),
            "dateCreated": appointment.get("datetimeCreated"),
            "forms": appointment.get("forms", [])
        }

