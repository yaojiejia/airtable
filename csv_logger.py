"""
CSV logging utilities for Acuity appointments.
Handles both the main log and form-specific CSV files.
"""
import csv
import os
import re
from datetime import datetime
from typing import Dict, Optional
from pathlib import Path

from config import config


class CSVLogger:
    """Handles all CSV logging operations for Acuity appointments."""
    
    def __init__(self):
        """Initialize CSV logger and ensure directories/files exist."""
        self.log_file = config.CSV_LOG_FILE
        self.forms_dir = config.FORMS_CSV_DIR
        
        self._init_main_log()
        self._init_forms_directory()
    
    def _init_main_log(self):
        """Initialize main CSV log file with headers if it doesn't exist."""
        if not os.path.exists(self.log_file):
            try:
                with open(self.log_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(config.CSV_LOG_HEADERS)
                print(f"[INFO] Created CSV log file: {self.log_file}")
            except Exception as e:
                print(f"[WARNING] Could not create CSV log file: {e}")
    
    def _init_forms_directory(self):
        """Create directory for form-specific CSV files."""
        if not os.path.exists(self.forms_dir):
            try:
                os.makedirs(self.forms_dir)
                print(f"[INFO] Created forms CSV directory: {self.forms_dir}")
            except Exception as e:
                print(f"[WARNING] Could not create forms CSV directory: {e}")
    
    def log_appointment(
        self,
        acuity_record: Dict,
        action: str,
        injected: bool = False,
        airtable_record_id: str = '',
        notes: str = ''
    ):
        """
        Log an appointment to the main CSV log.
        
        Args:
            acuity_record: Acuity appointment record
            action: Action taken (e.g., 'PROCESSED', 'CANCELLED')
            injected: Whether record was injected to Airtable
            airtable_record_id: Airtable record ID if injected
            notes: Additional notes
        """
        try:
            with open(self.log_file, 'a', newline='', encoding='utf-8') as f:
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
    
    def log_form_data(self, acuity_record: Dict):
        """
        Log intake form Q&A to form-specific CSV file.
        
        Args:
            acuity_record: Acuity appointment record with forms
        """
        try:
            appointment_type = acuity_record.get('appointment_type', 'unknown')
            csv_filename = self._get_form_csv_filename(appointment_type)
            csv_filepath = os.path.join(self.forms_dir, csv_filename)
            
            # Extract and structure form data
            form_data = self._extract_form_data(acuity_record)
            if not form_data:
                return
            
            # Handle CSV headers and write data
            self._write_form_csv(csv_filepath, form_data)
            
        except Exception as e:
            print(f"[WARNING] Could not save form to CSV: {e}")
    
    def _extract_form_data(self, acuity_record: Dict) -> Optional[Dict]:
        """
        Extract form data from Acuity record.
        
        Args:
            acuity_record: Acuity appointment record
            
        Returns:
            Dictionary of form data or None if no forms
        """
        forms = acuity_record.get('forms', [])
        if not forms:
            return None
        
        form_data = {
            'Sync Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Appointment ID': acuity_record.get('appointment_id', ''),
            'Client Name': acuity_record.get('client_name', ''),
            'Email': acuity_record.get('email', ''),
            'Phone': acuity_record.get('phone', ''),
            'Appointment DateTime': acuity_record.get('datetime', ''),
            'Canceled': 'Yes' if acuity_record.get('canceled', False) else 'No'
        }
        
        # Add all form field Q&A
        for form in forms:
            for field in form.get('values', []):
                field_name = field.get('name', '').strip()
                field_value = field.get('value', '')
                if field_name:
                    form_data[field_name] = field_value
        
        return form_data
    
    def _write_form_csv(self, filepath: str, form_data: Dict):
        """
        Write form data to CSV, handling dynamic headers.
        
        Args:
            filepath: Path to CSV file
            form_data: Dictionary of form data
        """
        file_exists = os.path.exists(filepath)
        
        # Read existing headers if file exists
        existing_headers = []
        if file_exists:
            try:
                with open(filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    existing_headers = next(reader, [])
            except Exception:
                existing_headers = []
        
        # Merge headers (existing + new fields)
        all_headers = list(existing_headers) if existing_headers else config.FORM_CSV_BASE_HEADERS.copy()
        
        # Add any new form fields to headers
        for key in form_data.keys():
            if key not in all_headers:
                all_headers.append(key)
        
        # If headers changed, rewrite file with new headers
        if file_exists and existing_headers and set(all_headers) != set(existing_headers):
            self._rewrite_csv_with_new_headers(filepath, all_headers, form_data)
        else:
            # Normal append (or create new file)
            try:
                with open(filepath, 'a' if file_exists else 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=all_headers)
                    if not file_exists:
                        writer.writeheader()
                    writer.writerow(form_data)
            except Exception as e:
                print(f"[WARNING] Could not write to form CSV: {e}")
    
    def _rewrite_csv_with_new_headers(self, filepath: str, new_headers: list, new_row: Dict):
        """
        Rewrite CSV file with updated headers.
        
        Args:
            filepath: Path to CSV file
            new_headers: Updated list of headers
            new_row: New row to append
        """
        # Read all existing data
        existing_data = []
        try:
            with open(filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                existing_data = list(reader)
        except Exception as e:
            print(f"[WARNING] Could not read existing CSV: {e}")
        
        # Rewrite file with new headers
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=new_headers)
                writer.writeheader()
                for row in existing_data:
                    writer.writerow(row)
                writer.writerow(new_row)
        except Exception as e:
            print(f"[WARNING] Could not rewrite CSV with new headers: {e}")
    
    def _get_form_csv_filename(self, appointment_type: str) -> str:
        """
        Generate a clean CSV filename from appointment type.
        
        Args:
            appointment_type: Full appointment type string
            
        Returns:
            Clean filename (e.g., "product_development_help_desk.csv")
        """
        # Split by pipe (|) to get parts
        parts = [p.strip() for p in appointment_type.split('|')]
        
        # Find the part that contains the session/help desk name
        form_name = self._extract_form_name_from_parts(parts)
        
        # Clean up the name
        cleaned_name = self._clean_form_name(form_name)
        
        return f"{cleaned_name}.csv"
    
    def _extract_form_name_from_parts(self, parts: list) -> str:
        """
        Extract form name from appointment type parts.
        
        Args:
            parts: List of appointment type parts split by '|'
            
        Returns:
            Extracted form name
        """
        form_name = None
        
        for part in parts:
            part_lower = part.lower()
            
            # Skip parts that are just price indicators or prefixes
            if re.match(r'^(free|paid|\$\d+)', part_lower):
                continue
            
            # Skip parts with names in parentheses (likely instructor names)
            if '(' in part and ')' in part:
                before_paren = part.split('(')[0].strip()
                if any(keyword in before_paren.lower() for keyword in config.FORM_TYPE_KEYWORDS):
                    form_name = before_paren
                    break
                continue
            
            # Check if this part contains a session keyword
            if any(keyword in part_lower for keyword in config.FORM_TYPE_KEYWORDS):
                form_name = part
                break
        
        # Fallback logic if no keyword match
        if not form_name:
            form_name = self._fallback_form_name(parts)
        
        return form_name
    
    def _fallback_form_name(self, parts: list) -> str:
        """
        Fallback logic for extracting form name when no keywords match.
        
        Args:
            parts: List of appointment type parts
            
        Returns:
            Fallback form name
        """
        appointment_type = ' | '.join(parts)
        
        # Check if this looks like a person's name
        is_likely_name = (
            len(parts) <= 2 and
            len(appointment_type.split()) <= 4 and
            appointment_type[0].isupper() and
            not any(keyword in appointment_type.lower() for keyword in config.FORM_TYPE_KEYWORDS)
        )
        
        if is_likely_name:
            return "advisor_1_on_1_session"
        
        # Filter out short parts and likely names
        meaningful_parts = [
            p for p in parts
            if len(p) > 10 and not re.match(r'^(free|paid|\$\d+)', p.lower())
        ]
        
        if meaningful_parts:
            return meaningful_parts[0]
        
        return appointment_type if appointment_type else "unknown_form_type"
    
    def _clean_form_name(self, form_name: str) -> str:
        """
        Clean up form name for use as filename.
        
        Args:
            form_name: Raw form name
            
        Returns:
            Cleaned filename-safe name
        """
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
        
        return cleaned

