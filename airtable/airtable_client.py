"""
Airtable API client.
"""
from pyairtable import Api
from typing import List, Dict, Optional, Set
from datetime import datetime
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from config import config


class AirtableClient:
    """Client for interacting with Airtable API."""
    
    def __init__(self, api_key: str = None, base_id: str = None, table_name: str = None):
        """
        Initialize Airtable client.
        
        Args:
            api_key: Airtable API key (defaults to config)
            base_id: Airtable base ID (defaults to config)
            table_name: Airtable table name (defaults to config)
        """
        self.api_key = api_key or config.AIRTABLE_API_KEY
        self.base_id = base_id or config.AIRTABLE_BASE_ID
        self.table_name = table_name or config.AIRTABLE_TABLE_NAME
        
        self.api = Api(self.api_key)
        self.table = self.api.table(self.base_id, self.table_name)
    
    def get_all_records(self, max_records: int = None) -> List[Dict]:
        """
        Fetch all records from the table.
        
        Args:
            max_records: Maximum number of records to fetch
            
        Returns:
            List of record dictionaries
        """
        try:
            return self.table.all(max_records=max_records or config.MAX_AIRTABLE_RECORDS_TO_SCAN)
        except Exception as e:
            print(f"[ERROR] Failed to fetch records: {e}")
            return []
    
    def get_all_field_names(self) -> List[str]:
        """
        Get all unique field names from the table.
        
        Note: Fetches multiple records to ensure all fields are captured,
        since empty fields don't appear in individual records.
        
        Returns:
            Sorted list of unique field names
        """
        records = self.get_all_records()
        
        if not records:
            print("[WARNING] No records found in table")
            return []
        
        # Collect all unique field names
        all_fields = set()
        for record in records:
            all_fields.update(record['fields'].keys())
        
        return sorted(list(all_fields))
    
    def create_record(self, fields: Dict) -> Dict:
        """
        Create a new record in the table.
        
        Args:
            fields: Dictionary of field names and values
            
        Returns:
            Created record dictionary
        """
        try:
            return self.table.create(fields)
        except Exception as e:
            print(f"[ERROR] Failed to create record: {e}")
            raise


class FieldMapper:
    """Maps fields between Acuity and Airtable systems."""
    
    def __init__(self, airtable_fields: List[str]):
        """
        Initialize field mapper.
        
        Args:
            airtable_fields: List of Airtable field names
        """
        self.airtable_fields = airtable_fields
        self.airtable_fields_set = set(f.strip() for f in airtable_fields)
        self.name_mapping = {f.strip(): f for f in airtable_fields}
    
    def get_acuity_field_names(self, acuity_record: Dict) -> Set[str]:
        """
        Extract all field names from an Acuity record.
        
        Args:
            acuity_record: Acuity intake form record
            
        Returns:
            Set of field names (whitespace stripped)
        """
        field_names = {"Name", "What is your email?"}
        
        for form in acuity_record.get('forms', []):
            for field in form.get('values', []):
                field_name = field.get('name', '').strip()
                if field_name:
                    field_names.add(field_name)
        
        return field_names
    
    def get_matching_fields(self, acuity_record: Dict) -> Set[str]:
        """
        Find fields that exist in both Acuity and Airtable.
        
        Args:
            acuity_record: Acuity intake form record
            
        Returns:
            Set of matching field names
        """
        acuity_fields = self.get_acuity_field_names(acuity_record)
        return acuity_fields.intersection(self.airtable_fields_set)
    
    def map_acuity_to_airtable(
        self,
        acuity_record: Dict,
        matching_fields: Optional[Set[str]] = None,
        add_timestamp_field: Optional[str] = None
    ) -> Dict:
        """
        Map Acuity record to Airtable field format.
        
        Args:
            acuity_record: Acuity intake form record
            matching_fields: Set of fields to include (None = include all)
            add_timestamp_field: Name of field to add current timestamp (None = don't add)
            
        Returns:
            Dictionary with Airtable field names and values
        """
        airtable_data = {}
        
        if acuity_record.get('client_name'):
            airtable_data['Name'] = acuity_record['client_name']
        
        if acuity_record.get('email'):
            airtable_data['Email'] = acuity_record['email']
        
        for form in acuity_record.get('forms', []):
            for field in form.get('values', []):
                field_name = field.get('name', '').strip()
                field_value = field.get('value')
                
                if not field_name or field_value is None:
                    continue
                
                if isinstance(field_value, str):
                    field_value = field_value.strip()
                    if not field_value:
                        continue
                
                if self._is_multi_select_field(field_name):
                    field_value = self._convert_to_array(field_value)
                
                airtable_data[field_name] = field_value
        
        if matching_fields is not None:
            filtered_data = {}
            for k, v in airtable_data.items():
                k_stripped = k.strip()
                if k_stripped in matching_fields:
                    exact_name = self.name_mapping.get(k_stripped, k_stripped)
                    filtered_data[exact_name] = v
            airtable_data = filtered_data
        
        if add_timestamp_field:
            timestamp_field_name = self.name_mapping.get(add_timestamp_field, add_timestamp_field)
            airtable_data[timestamp_field_name] = datetime.now().strftime("%Y-%m-%d")
        
        return airtable_data
    
    def _is_multi_select_field(self, field_name: str) -> bool:
        """
        Check if a field is a multi-select field in Airtable.
        
        Args:
            field_name: Field name to check
            
        Returns:
            True if field is multi-select
        """
        # Check if field is in the explicit multi-select fields list
        if field_name.strip() in config.MULTI_SELECT_FIELDS:
            return True
        
        # Check if field name contains multi-select indicators
        return any(indicator in field_name.lower() 
                  for indicator in config.MULTI_SELECT_INDICATORS)
    
    def _convert_to_array(self, value) -> List:
        """
        Convert value to array format for multi-select fields.
        
        Args:
            value: Value to convert
            
        Returns:
            Value as list
        """
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            if ',' in value:
                return [v.strip() for v in value.split(',')]
            return [value]
        return [str(value)]


class AirtableService:
    """Service for Airtable operations."""
    
    def __init__(self, client: AirtableClient = None):
        """Initialize Airtable service."""
        self.client = client or AirtableClient()
        self.field_names = self.client.get_all_field_names()
        self.mapper = FieldMapper(self.field_names)
    
    def inject_acuity_record(
        self,
        acuity_record: Dict,
        verbose: bool = True,
        timestamp_field: Optional[str] = None
    ) -> Dict:
        """
        Inject an Acuity record into Airtable.
        
        Args:
            acuity_record: Acuity intake form record
            verbose: Whether to print detailed output
            timestamp_field: Name of field to add current timestamp (optional)
            
        Returns:
            Created Airtable record
        """
        matching_fields = self.mapper.get_matching_fields(acuity_record)
        
        mapped_data = self.mapper.map_acuity_to_airtable(
            acuity_record,
            matching_fields=matching_fields,
            add_timestamp_field=timestamp_field
        )
        
        if verbose:
            self._print_injection_info(acuity_record, mapped_data, timestamp_field)
        
        created_record = self.client.create_record(mapped_data)
        
        if verbose:
            print(f"[SUCCESS] Record created with ID: {created_record['id']}")
        
        return created_record
    
    def _print_injection_info(self, acuity_record: Dict, mapped_data: Dict, timestamp_field: Optional[str]):
        """Print injection information."""
        print(f"\nMapping Acuity record to Airtable...")
        print(f"Acuity Appointment ID: {acuity_record.get('appointment_id')}")
        print(f"Client: {acuity_record.get('client_name')}")
        print(f"Fields to insert: {len(mapped_data)}")
        
        print("\nData to be inserted:")
        for field_name, field_value in mapped_data.items():
            preview = str(field_value)[:50] + "..." if len(str(field_value)) > 50 else str(field_value)
            marker = " [AUTO]" if timestamp_field and timestamp_field in field_name else ""
            print(f"  - {field_name}: {preview}{marker}")
        
        print("\nInserting into Airtable...")

