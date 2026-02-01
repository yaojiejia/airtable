"""
Acuity-Airtable SDK

SDK for connecting Acuity Scheduling to Airtable.
"""

from typing import List, Dict, Optional, Set
from pathlib import Path
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
    
    def __init__(self):
        """Initialize CSV SDK."""
        self.logger = CSVLogger()
    
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
        import csv
        from datetime import datetime
        
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
        
        import csv
        from datetime import datetime
        
        # Collect all unique field names
        all_fields = set()
        for form in forms:
            # Add top-level fields
            all_fields.update(['Appointment ID', 'Client Name', 'Email', 'Phone', 
                             'Appointment DateTime', 'Appointment Type', 'Canceled'])
            
            # Add form fields
            for form_data in form.get('forms', []):
                for field in form_data.get('values', []):
                    field_name = field.get('name', '').strip()
                    if field_name:
                        all_fields.add(field_name)
        
        # Create header
        headers = ['Export Timestamp'] + sorted(list(all_fields))
        
        # Write CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for form in forms:
                row = {
                    'Export Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Appointment ID': form.get('appointment_id', ''),
                    'Client Name': form.get('client_name', ''),
                    'Email': form.get('email', ''),
                    'Phone': form.get('phone', ''),
                    'Appointment DateTime': form.get('datetime', ''),
                    'Appointment Type': form.get('appointment_type', ''),
                    'Canceled': 'Yes' if form.get('canceled', False) else 'No'
                }
                
                # Add form field values
                for form_data in form.get('forms', []):
                    for field in form_data.get('values', []):
                        field_name = field.get('name', '').strip()
                        field_value = field.get('value', '')
                        if field_name:
                            row[field_name] = field_value
                
                writer.writerow(row)


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
        airtable_table_name: str = None
    ):
        """
        Initialize the Acuity-Airtable SDK.
        
        Args:
            acuity_user_id: Acuity user ID (defaults to env var)
            acuity_api_key: Acuity API key (defaults to env var)
            airtable_api_key: Airtable API key (defaults to env var)
            airtable_base_id: Airtable base ID (defaults to env var)
            airtable_table_name: Airtable table name (defaults to env var)
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
        self.csv = CSVSDK()
    
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
        output_dir: str = "csv_exports"
    ) -> Dict[str, str]:
        """
        Export Acuity forms to CSV files.
        
        Args:
            hours: Number of hours to look back
            include_canceled: Whether to include cancelled appointments
            group_by_appointment_type: Whether to group by appointment type
            output_dir: Directory to save CSV files
            
        Returns:
            Dictionary mapping form types to CSV file paths
        """
        forms = self.acuity.get_intake_forms(hours, include_canceled)
        
        return self.csv.export_forms_grouped(
            forms,
            output_dir,
            group_by_appointment_type
        )
    
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

