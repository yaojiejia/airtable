"""
Acuity-Airtable SDK

SDK for connecting Acuity Scheduling to Airtable.
"""

from typing import List, Dict, Optional, Set
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from dateutil import parser as date_parser
import pytz
import csv
import os

from config import config
from acuity.acuity_client import AcuityClient, IntakeFormService
from airtable.airtable_client import AirtableClient, AirtableService, FieldMapper
from csv_logger import CSVLogger


class AcuitySDK:
    """Acuity Scheduling operations."""
    
    def __init__(self, user_id: str, api_key: str):
        self.client = AcuityClient(user_id, api_key)
        self.service = IntakeFormService(self.client)
    
    def get_intake_forms(
        self,
        hours: int = 24,
        include_canceled: bool = False
    ) -> List[Dict]:
        """
        Get all intake forms from the last N hours.
        
        Args:
            hours: Number of hours to look back
            include_canceled: Whether to include cancelled appointments
            
        Returns:
            List of intake form records
        """
        return self.service.get_recent_forms(hours, include_canceled)
    
    def get_all_form_types(
        self,
        hours: int = 24,
        include_canceled: bool = False
    ) -> List[str]:
        """
        Get all unique form types (appointment types) from recent forms.
        
        Args:
            hours: Number of hours to look back
            include_canceled: Whether to include cancelled appointments
            
        Returns:
            List of unique form type names
        """
        forms = self.get_intake_forms(hours, include_canceled)
        form_types = set()
        
        for form in forms:
            form_type = form.get('appointment_type')
            if form_type:
                form_types.add(form_type)
        
        return sorted(list(form_types))
    
    def get_columns_by_form_type(
        self,
        form_type: str,
        hours: int = 24
    ) -> Dict[str, Set[str]]:
        """
        Get all unique column names for a specific form type.
        
        Args:
            form_type: The appointment type to filter by
            hours: Number of hours to look back
            
        Returns:
            Dictionary with:
            - 'top_level': Set of top-level fields (Name, Email, etc.)
            - 'form_fields': Set of intake form field names
            - 'all_fields': Combined set of all fields
        """
        forms = self.get_intake_forms(hours, include_canceled=True)
        
        # Filter by form type
        matching_forms = [
            f for f in forms 
            if f.get('appointment_type') == form_type
        ]
        
        if not matching_forms:
            return {
                'top_level': set(),
                'form_fields': set(),
                'all_fields': set()
            }
        
        # Collect all field names
        top_level_fields = {'Name', 'What is your email?'}
        form_fields = set()
        
        for form in matching_forms:
            for form_data in form.get('forms', []):
                for field in form_data.get('values', []):
                    field_name = field.get('name', '').strip()
                    if field_name:
                        form_fields.add(field_name)
        
        return {
            'top_level': top_level_fields,
            'form_fields': form_fields,
            'all_fields': top_level_fields.union(form_fields)
        }
    
    def get_all_columns_by_form_type(
        self,
        hours: int = 24
    ) -> Dict[str, Set[str]]:
        """
        Get columns for ALL form types.
        
        Args:
            hours: Number of hours to look back
            
        Returns:
            Dictionary mapping form type to set of column names
        """
        form_types = self.get_all_form_types(hours)
        result = {}
        
        for form_type in form_types:
            columns = self.get_columns_by_form_type(form_type, hours)
            result[form_type] = columns['all_fields']
        
        return result
    
    def get_form_by_id(self, appointment_id: int) -> Optional[Dict]:
        """
        Get a specific appointment by ID.
        
        Args:
            appointment_id: Acuity appointment ID
            
        Returns:
            Appointment record or None
        """
        return self.client.get_appointment_by_id(appointment_id)


class AirtableSDK:
    """High-level SDK for Airtable operations."""
    
    def __init__(self, api_key: str, base_id: str, table_name: str):
        """
        Initialize Airtable SDK.
        
        Args:
            api_key: Airtable API key
            base_id: Airtable base ID
            table_name: Initial table name to work with
        """
        self.api_key = api_key
        self.base_id = base_id
        self.table_name = table_name
        
        self._client = None
        self._service = None
        self._refresh_client()
    
    def _refresh_client(self):
        """Refresh the client and service instances."""
        self._client = AirtableClient(self.api_key, self.base_id, self.table_name)
        self._service = AirtableService(self._client)
    
    def use_table(self, table_name: str):
        """
        Switch to a different table in the same base.
        
        Args:
            table_name: Name of the table to switch to
        """
        self.table_name = table_name
        self._refresh_client()
    
    def get_columns(self) -> List[str]:
        """
        Get all column names from the current table.
        
        Returns:
            List of column names
        """
        return self._client.get_all_field_names()
    
    def get_records(self, max_records: int = 100) -> List[Dict]:
        """
        Get records from the current table.
        
        Args:
            max_records: Maximum number of records to fetch
            
        Returns:
            List of records
        """
        return self._client.get_all_records(max_records)
    
    def inject_record(
        self,
        acuity_record: Dict,
        verbose: bool = True,
        timestamp_field: Optional[str] = None
    ) -> Dict:
        """
        Inject an Acuity record into the current Airtable table.
        
        Args:
            acuity_record: Acuity intake form record
            verbose: Print detailed output
            timestamp_field: Field name for current timestamp (optional)
            
        Returns:
            Created Airtable record
        """
        return self._service.inject_acuity_record(acuity_record, verbose, timestamp_field)
    
    def get_matching_fields(self, acuity_record: Dict) -> Set[str]:
        """
        Get fields that match between an Acuity record and current Airtable table.
        
        Args:
            acuity_record: Acuity intake form record
            
        Returns:
            Set of matching field names
        """
        return self._service.mapper.get_matching_fields(acuity_record)
    
    @property
    def field_mapper(self) -> FieldMapper:
        """Get the field mapper instance."""
        return self._service.mapper


class CSVSDK:
    """High-level SDK for CSV export operations."""
    
    def __init__(
        self,
        form_type_keywords: Optional[List[str]] = None,
        fallback_form_name: Optional[str] = None
    ):
        """
        Initialize CSV SDK.
        
        Args:
            form_type_keywords: Optional list of keywords to identify form types
                               in appointment names. If None, uses a generic approach.
            fallback_form_name: Optional fallback name for forms that can't be categorized.
                               If None, uses "unknown_form_type".
        """
        self.logger = CSVLogger(
            form_type_keywords=form_type_keywords,
            fallback_form_name=fallback_form_name
        )
    
    def export_forms_grouped(
        self,
        forms: List[Dict],
        output_dir: str = "csv_exports",
        by_appointment_type: bool = True
    ) -> Dict[str, str]:
        """
        Export forms to CSV files, optionally grouped by appointment type.
        
        Args:
            forms: List of Acuity intake form records
            output_dir: Directory to save CSV files
            by_appointment_type: Whether to group by appointment type
            
        Returns:
            Dictionary mapping form types to CSV file paths
        """
        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        if not by_appointment_type:
            # Single CSV file for all forms
            filepath = os.path.join(output_dir, "all_forms.csv")
            self._write_forms_to_csv(forms, filepath)
            return {"all": filepath}
        
        # Group by appointment type
        grouped_forms = {}
        for form in forms:
            apt_type = form.get('appointment_type', 'unknown')
            if apt_type not in grouped_forms:
                grouped_forms[apt_type] = []
            grouped_forms[apt_type].append(form)
        
        # Write each group to its own CSV
        result = {}
        for apt_type, type_forms in grouped_forms.items():
            filename = self.logger._get_form_csv_filename(apt_type)
            filepath = os.path.join(output_dir, filename)
            self._write_forms_to_csv(type_forms, filepath)
            result[apt_type] = filepath
        
        return result
    
    def _write_forms_to_csv(self, forms: List[Dict], filepath: str):
        """
        Write forms to a CSV file.
        
        Args:
            forms: List of form records
            filepath: Path to CSV file
        """
        if not forms:
            return
        
        # Collect all unique field names
        all_fields = set()
        for form in forms:
            # Add top-level fields
            all_fields.update(['Appointment ID', 'Client Name', 'Email', 'Phone', 
                             'Appointment DateTime', 'Appointment Type', 'Canceled', 'Rescheduled'])
            
            # Add form fields
            for form_data in form.get('forms', []):
                for field in form_data.get('values', []):
                    field_name = field.get('name', '').strip()
                    if field_name:
                        all_fields.add(field_name)
        
        # Read existing records if file exists (for deduplication)
        existing_records_signatures = set()
        existing_records_by_id = {}
        file_exists = os.path.exists(filepath)
        if file_exists:
            try:
                with open(filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Create signature from all field values (excluding Export Timestamp)
                        signature = self._create_record_signature(row)
                        existing_records_signatures.add(signature)
                        
                        # Also track by appointment_id for reschedule detection
                        apt_id = row.get('Appointment ID', '')
                        if apt_id:
                            datetime_key = row.get('Appointment DateTime', '')
                            if apt_id not in existing_records_by_id:
                                existing_records_by_id[apt_id] = []
                            existing_records_by_id[apt_id].append({
                                'datetime': datetime_key,
                                'canceled': row.get('Canceled', 'No')
                            })
            except Exception as e:
                print(f"[WARNING] Could not read existing CSV: {e}")
                existing_records_signatures = set()
                existing_records_by_id = {}
        
        # Create header
        headers = ['Export Timestamp'] + sorted(list(all_fields))
        
        # Determine which records are new or changed
        records_to_add = []
        for form in forms:
            apt_id = form.get('appointment_id', '')
            appointment_datetime = self._format_datetime_to_est(form.get('datetime', ''))
            is_canceled = form.get('canceled', False)
            
            # Build the row first to check for duplicates
            row = {
                'Export Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'Appointment ID': apt_id,
                'Client Name': form.get('client_name', ''),
                'Email': form.get('email', ''),
                'Phone': form.get('phone', ''),
                'Appointment DateTime': appointment_datetime,
                'Appointment Type': form.get('appointment_type', ''),
                'Canceled': 'Yes' if is_canceled else 'No',
                'Rescheduled': 'No'  # Will update if rescheduled
            }
            
            # Add form field values
            for form_data in form.get('forms', []):
                for field in form_data.get('values', []):
                    field_name = field.get('name', '').strip()
                    field_value = field.get('value', '')
                    if field_name:
                        row[field_name] = field_value
            
            # Check if this exact record already exists (all fields identical)
            signature = self._create_record_signature(row)
            if signature in existing_records_signatures:
                # Exact duplicate - skip it
                continue
            
            # Check if this is a reschedule or status change
            is_rescheduled = False
            if apt_id in existing_records_by_id:
                # Check if datetime changed
                existing_datetimes = [r['datetime'] for r in existing_records_by_id[apt_id]]
                datetime_changed = appointment_datetime not in existing_datetimes
                
                # Check if canceled status changed
                existing_canceled_states = [r['canceled'] for r in existing_records_by_id[apt_id]]
                current_canceled = 'Yes' if is_canceled else 'No'
                status_changed = current_canceled not in existing_canceled_states
                
                if datetime_changed or status_changed:
                    is_rescheduled = True
            
            row['Rescheduled'] = 'Yes' if is_rescheduled else 'No'
            records_to_add.append(row)
        
        # Append new/changed records to CSV
        if records_to_add:
            with open(filepath, 'a' if file_exists else 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if not file_exists:
                    writer.writeheader()
                
                for row in records_to_add:
                    writer.writerow(row)
            
            # Fix rescheduled field for all records in the file
            self._fix_rescheduled_field_in_file(filepath)
            
            # Deduplicate records in the file
            self._dedupe_csv_file(filepath)
    
    def _fix_rescheduled_field_in_file(self, filepath: str):
        """
        Fix the Rescheduled field in a CSV file after writing new records.
        Marks records as rescheduled if they have the same appointment_id but different datetime.
        
        Args:
            filepath: Path to CSV file
        """
        if not os.path.exists(filepath):
            return
        
        try:
            # Read all records
            records = []
            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                if 'Rescheduled' not in headers:
                    return  # No Rescheduled column, skip
                
                records = list(reader)
            
            if not records:
                return
            
            # Group records by appointment_id
            records_by_appointment = defaultdict(list)
            for i, record in enumerate(records):
                apt_id = record.get('Appointment ID', '')
                if apt_id:
                    records_by_appointment[apt_id].append((i, record))
            
            # Track if any updates were made
            updates_made = False
            
            # For each appointment_id with multiple records
            for apt_id, record_list in records_by_appointment.items():
                if len(record_list) <= 1:
                    continue  # Only one record, can't be rescheduled
                
                # Get all unique datetimes for this appointment
                datetimes = set()
                for _, record in record_list:
                    datetime_val = record.get('Appointment DateTime', '')
                    if datetime_val:
                        datetimes.add(datetime_val)
                
                # If there are multiple datetimes, mark all but the first as rescheduled
                if len(datetimes) > 1:
                    # Sort by Export Timestamp to find the first one
                    sorted_records = sorted(record_list, key=lambda x: x[1].get('Export Timestamp', ''))
                    
                    # First record stays as "No" (or keep existing value if already "Yes")
                    first_idx, first_record = sorted_records[0]
                    first_datetime = first_record.get('Appointment DateTime', '')
                    
                    # Mark all others with different datetime as rescheduled
                    for idx, record in sorted_records[1:]:
                        record_datetime = record.get('Appointment DateTime', '')
                        if record_datetime != first_datetime:
                            if records[idx].get('Rescheduled', 'No') != 'Yes':
                                records[idx]['Rescheduled'] = 'Yes'
                                updates_made = True
            
            # Write updated records back if changes were made
            if updates_made:
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(records)
        except Exception as e:
            # Silently fail - don't break the export if fixing fails
            pass
    
    def _dedupe_csv_file(self, filepath: str):
        """
        Remove duplicate records from a CSV file.
        Compares all fields (excluding timestamps) to identify duplicates.
        Keeps the first occurrence of each unique record.
        
        Args:
            filepath: Path to CSV file
        """
        if not os.path.exists(filepath):
            return
        
        try:
            # Read all records
            records = []
            seen_signatures = set()
            duplicates_removed = 0
            
            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                for row in reader:
                    signature = self._create_record_signature(row)
                    
                    if signature in seen_signatures:
                        duplicates_removed += 1
                    else:
                        seen_signatures.add(signature)
                        records.append(row)
            
            # Only rewrite if duplicates were found
            if duplicates_removed > 0:
                with open(filepath, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    writer.writerows(records)
        except Exception as e:
            # Silently fail - don't break the export if deduplication fails
            pass
    
    def _create_record_signature(self, row: Dict) -> str:
        """
        Create a unique signature from all field values (excluding Export Timestamp).
        Used for deduplication by comparing all columns.
        
        Args:
            row: Dictionary of record data
            
        Returns:
            String signature representing all field values
        """
        # Create a sorted tuple of all key-value pairs (excluding Export Timestamp)
        fields = []
        for key, value in sorted(row.items()):
            if key != 'Export Timestamp':
                # Normalize values: convert to string and strip whitespace
                normalized_value = str(value).strip() if value is not None else ''
                fields.append(f"{key}:{normalized_value}")
        
        # Create signature from all fields
        return "|".join(fields)
    
    def _format_datetime_to_est(self, datetime_str: str) -> str:
        """
        Convert datetime string to EST timezone and format it nicely.
        
        Args:
            datetime_str: ISO datetime string from Acuity (e.g., "2026-03-09T16:00:00-0400")
            
        Returns:
            Formatted datetime string in EST (e.g., "March 9, 2026 4:00 PM EST")
        """
        if not datetime_str:
            return ''
        
        try:
            # Parse the datetime string
            dt = date_parser.parse(datetime_str)
            
            # Convert to EST/EDT (handles daylight saving automatically)
            est_tz = pytz.timezone('US/Eastern')
            
            # If datetime is timezone-aware, convert to EST
            if dt.tzinfo is not None:
                dt_est = dt.astimezone(est_tz)
            else:
                # If timezone-naive, assume UTC and convert to EST
                dt_utc = pytz.utc.localize(dt)
                dt_est = dt_utc.astimezone(est_tz)
            
            # Format: "March 9, 2026 4:00 PM EST" or "March 9, 2026 4:00 PM EDT"
            timezone_abbr = dt_est.strftime('%Z')  # EST or EDT
            formatted = dt_est.strftime('%B %d, %Y %I:%M %p') + f' {timezone_abbr}'
            
            return formatted
            
        except Exception as e:
            # If parsing fails, return original string
            print(f"[WARNING] Could not parse datetime '{datetime_str}': {e}")
            return datetime_str


class AcuityAirtableSDK:
    """
    High-level SDK for Acuity-Airtable integration.
    
    This SDK provides a simple, intuitive interface for:
    - Fetching Acuity intake forms
    - Getting columns by form type
    - Managing Airtable tables and columns
    - Syncing data between Acuity and Airtable
    - Exporting to CSV
    """
    
    def __init__(
        self,
        acuity_user_id: str = None,
        acuity_api_key: str = None,
        airtable_api_key: str = None,
        airtable_base_id: str = None,
        airtable_table_name: str = None,
        form_type_keywords: Optional[List[str]] = None,
        fallback_form_name: Optional[str] = None
    ):
        """
        Initialize the Acuity-Airtable SDK.
        
        Args:
            acuity_user_id: Acuity user ID (defaults to env var)
            acuity_api_key: Acuity API key (defaults to env var)
            airtable_api_key: Airtable API key (defaults to env var)
            airtable_base_id: Airtable base ID (defaults to env var)
            airtable_table_name: Airtable table name (defaults to env var)
            form_type_keywords: Optional list of keywords to identify form types
                               in appointment names. If None, uses a generic approach.
            fallback_form_name: Optional fallback name for forms that can't be categorized.
                               If None, uses "unknown_form_type".
        """
        # Use provided values or fall back to config
        self.acuity_user_id = acuity_user_id or config.ACUITY_USER_ID
        self.acuity_api_key = acuity_api_key or config.ACUITY_API_KEY
        self.airtable_api_key = airtable_api_key or config.AIRTABLE_API_KEY
        self.airtable_base_id = airtable_base_id or config.AIRTABLE_BASE_ID
        self.airtable_table_name = airtable_table_name or config.AIRTABLE_TABLE_NAME
        
        # Initialize sub-SDKs
        self.acuity = AcuitySDK(self.acuity_user_id, self.acuity_api_key)
        self.airtable = AirtableSDK(
            self.airtable_api_key,
            self.airtable_base_id,
            self.airtable_table_name
        )
        self.csv = CSVSDK(
            form_type_keywords=form_type_keywords,
            fallback_form_name=fallback_form_name
        )
    
    def sync(
        self,
        hours: int = 24,
        include_canceled: bool = False,
        verbose: bool = True,
        timestamp_field: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Sync Acuity intake forms to Airtable.
        
        Args:
            hours: Number of hours to look back
            include_canceled: Include cancelled appointments
            verbose: Print detailed output
            timestamp_field: Field name for current timestamp (optional)
            
        Returns:
            Dictionary with forms_fetched, successful, failed, records, errors
        """
        if verbose:
            print(f"\n{'='*80}")
            print(f"ACUITY TO AIRTABLE SYNC")
            print(f"{'='*80}")
            print(f"Fetching forms from last {hours} hours...")
        
        forms = self.acuity.get_intake_forms(hours, include_canceled)
        
        if verbose:
            print(f"Found {len(forms)} form(s)")
            print(f"Target table: {self.airtable.table_name}")
            print(f"{'='*80}\n")
        
        if not forms:
            return {
                'forms_fetched': 0,
                'successful': 0,
                'failed': 0,
                'records': [],
                'errors': []
            }
        
        successful = []
        failed = []
        
        for i, form in enumerate(forms, 1):
            try:
                if verbose:
                    print(f"[{i}/{len(forms)}] Processing: {form.get('client_name')}")
                
                record = self.airtable.inject_record(form, verbose=False, timestamp_field=timestamp_field)
                successful.append(record)
                
                if verbose:
                    print(f"  Success - Record ID: {record['id']}\n")
                    
            except Exception as e:
                failed.append({'form': form, 'error': str(e)})
                
                if verbose:
                    print(f"  Failed: {e}\n")
        
        if verbose:
            print(f"{'='*80}")
            print(f"SYNC COMPLETE")
            print(f"{'='*80}")
            print(f"  Total forms: {len(forms)}")
            print(f"  Successful: {len(successful)}")
            print(f"  Failed: {len(failed)}")
            print(f"{'='*80}\n")
        
        return {
            'forms_fetched': len(forms),
            'successful': len(successful),
            'failed': len(failed),
            'records': successful,
            'errors': failed
        }
    
    def export_to_csv(
        self,
        hours: int = 24,
        include_canceled: bool = True,
        group_by_appointment_type: bool = True,
        output_dir: str = "csv_exports",
        detect_cancellations: bool = True
    ) -> Dict[str, str]:
        """
        Export Acuity forms to CSV files.
        
        Args:
            hours: Number of hours to look back
            include_canceled: Whether to include cancelled appointments
            group_by_appointment_type: Whether to group by appointment type
            output_dir: Directory to save CSV files
            detect_cancellations: Whether to detect cancellations by comparing CSV with current sync
            
        Returns:
            Dictionary mapping form types to CSV file paths
        """
        forms = self.acuity.get_intake_forms(hours, include_canceled)
        
        csv_files = self.csv.export_forms_grouped(
            forms,
            output_dir,
            group_by_appointment_type
        )
        
        # Detect cancellations by comparing CSV records with current sync
        if detect_cancellations:
            self._detect_cancellations_from_csv(forms, output_dir, hours)
            
            # Deduplicate again after cancellation detection (may have added duplicates)
            for filepath in csv_files.values():
                if os.path.exists(filepath):
                    self.csv._dedupe_csv_file(filepath)
        
        return csv_files
    
    def _detect_cancellations_from_csv(
        self,
        current_forms: List[Dict],
        output_dir: str,
        hours: int
    ):
        """
        Detect cancellations by comparing existing CSV records with current sync.
        
        Logic:
        - Check all records in CSV where Export Timestamp is between Export Timestamp and now
        - Check if Appointment DateTime is in the future
        - If appointment_id is not in current_forms, it was likely cancelled
        - Create a cancelled record for those appointments
        
        Args:
            current_forms: List of forms from current sync
            output_dir: Directory containing CSV files
            hours: Not used - kept for API compatibility
        """
        # Get set of appointment IDs from current sync
        current_appointment_ids = {str(form.get('appointment_id', '')) for form in current_forms}
        
        # Current time
        now = datetime.now()
        
        # Get all CSV files in the output directory
        if not os.path.exists(output_dir):
            return
        
        csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
        
        for csv_filename in csv_files:
            csv_filepath = os.path.join(output_dir, csv_filename)
            
            try:
                # Read existing records
                records = []
                with open(csv_filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    headers = reader.fieldnames
                    
                    if not headers or 'Export Timestamp' not in headers or 'Appointment DateTime' not in headers:
                        continue
                    
                    records = list(reader)
                
                # Find records that might be cancelled
                cancelled_records_to_add = []
                
                for record in records:
                    export_timestamp_str = record.get('Export Timestamp', '')
                    appointment_datetime_str = record.get('Appointment DateTime', '')
                    appointment_id = str(record.get('Appointment ID', ''))
                    current_canceled = record.get('Canceled', 'No')
                    
                    # Skip if already marked as cancelled
                    if current_canceled == 'Yes':
                        continue
                    
                    # Skip if appointment_id is in current sync (still active)
                    if appointment_id in current_appointment_ids:
                        continue
                    
                    # Check if Export Timestamp is in the past (between Export Timestamp and now)
                    try:
                        export_timestamp = datetime.strptime(export_timestamp_str, '%Y-%m-%d %H:%M:%S')
                        # Only check records where Export Timestamp is in the past (before now)
                        if export_timestamp >= now:
                            continue  # Export Timestamp is in the future, skip
                    except Exception:
                        continue
                    
                    # Check if Appointment DateTime is in the future
                    try:
                        # Parse the formatted datetime (e.g., "March 09, 2026 04:00 PM EDT")
                        # Try to parse it back
                        appointment_datetime = self._parse_formatted_datetime(appointment_datetime_str)
                        if appointment_datetime and appointment_datetime > now:
                            # This appointment is in the future but not in current sync - likely cancelled
                            # Create a cancelled version of this record
                            cancelled_record = record.copy()
                            cancelled_record['Export Timestamp'] = now.strftime('%Y-%m-%d %H:%M:%S')
                            cancelled_record['Canceled'] = 'Yes'
                            cancelled_record['Rescheduled'] = 'No'
                            cancelled_records_to_add.append(cancelled_record)
                    except Exception:
                        continue
                
                # Add cancelled records to CSV
                if cancelled_records_to_add:
                    with open(csv_filepath, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=headers)
                        for cancelled_record in cancelled_records_to_add:
                            writer.writerow(cancelled_record)
                    
                    print(f"[INFO] Detected {len(cancelled_records_to_add)} cancellation(s) in {csv_filename}")
                    
                    # Deduplicate after adding cancelled records
                    self.csv._dedupe_csv_file(csv_filepath)
                    
            except Exception as e:
                # Silently continue if there's an error with a specific file
                pass
    
    def _parse_formatted_datetime(self, datetime_str: str):
        """
        Parse a formatted datetime string back to datetime object.
        Handles formats like "March 09, 2026 04:00 PM EDT"
        
        Args:
            datetime_str: Formatted datetime string
            
        Returns:
            Datetime object or None if parsing fails
        """
        if not datetime_str:
            return None
        
        try:
            # Map timezone abbreviations to timezone objects
            est_tz = pytz.timezone('US/Eastern')
            tzinfos = {
                'EST': est_tz,
                'EDT': est_tz,
                'CST': pytz.timezone('US/Central'),
                'CDT': pytz.timezone('US/Central'),
                'MST': pytz.timezone('US/Mountain'),
                'MDT': pytz.timezone('US/Mountain'),
                'PST': pytz.timezone('US/Pacific'),
                'PDT': pytz.timezone('US/Pacific'),
            }
            
            # Try to parse the formatted string with timezone info
            return date_parser.parse(datetime_str, tzinfos=tzinfos)
        except Exception:
            return None
    
    def get_field_comparison(
        self,
        acuity_form: Dict
    ) -> Dict[str, Set[str]]:
        """
        Compare fields between an Acuity form and current Airtable table.
        
        Args:
            acuity_form: Acuity intake form record
            
        Returns:
            Dictionary with:
            - 'matching': Fields in both systems
            - 'only_in_acuity': Fields only in Acuity
            - 'only_in_airtable': Fields only in Airtable
        """
        # Get Acuity fields
        acuity_fields = self.airtable.field_mapper.get_acuity_field_names(acuity_form)
        
        # Get Airtable fields
        airtable_fields = set(self.airtable.get_columns())
        
        return {
            'matching': acuity_fields.intersection(airtable_fields),
            'only_in_acuity': acuity_fields - airtable_fields,
            'only_in_airtable': airtable_fields - acuity_fields
        }

