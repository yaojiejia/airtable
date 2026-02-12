"""
CSV logging utilities for Acuity appointments.
Handles both the main log and form-specific CSV files.
"""
import csv
import os
import re
from collections import defaultdict
from datetime import datetime
from dateutil import parser as date_parser
import pytz
from typing import Dict, Optional, List
from pathlib import Path

from config import config


class CSVLogger:
    """Handles all CSV logging operations for Acuity appointments."""
    
    def __init__(
        self,
        form_type_keywords: Optional[List[str]] = None,
        fallback_form_name: Optional[str] = None
    ):
        """
        Initialize CSV logger and ensure directories/files exist.
        
        Args:
            form_type_keywords: Optional list of keywords to identify form types
                               in appointment names. If None, uses a generic approach.
            fallback_form_name: Optional fallback name for forms that can't be categorized.
                               If None, uses "unknown_form_type".
        """
        self.log_file = config.CSV_LOG_FILE
        self.forms_dir = config.FORMS_CSV_DIR
        
        # Form name extraction configuration
        self.form_type_keywords = form_type_keywords or []
        self.fallback_form_name = fallback_form_name or "unknown_form_type"
        
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
                    self._format_datetime_to_est(acuity_record.get('datetime', '')),
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
        
        # Check if this is a reschedule by comparing with existing records
        is_rescheduled = self._check_if_rescheduled(
            acuity_record.get('appointment_id', ''),
            self._format_datetime_to_est(acuity_record.get('datetime', '')),
            acuity_record.get('canceled', False)
        )
        
        form_data = {
            'Sync Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'Appointment ID': acuity_record.get('appointment_id', ''),
            'Client Name': acuity_record.get('client_name', ''),
            'Email': acuity_record.get('email', ''),
            'Phone': acuity_record.get('phone', ''),
            'Appointment DateTime': self._format_datetime_to_est(acuity_record.get('datetime', '')),
            'Canceled': 'Yes' if acuity_record.get('canceled', False) else 'No',
            'Rescheduled': 'Yes' if is_rescheduled else 'No'
        }
        
        # Add all form field Q&A
        for form in forms:
            for field in form.get('values', []):
                field_name = field.get('name', '').strip()
                field_value = field.get('value', '')
                if field_name:
                    form_data[field_name] = field_value
        
        return form_data
    
    def _check_if_rescheduled(self, appointment_id: str, current_datetime: str, is_canceled: bool) -> bool:
        """
        Check if an appointment has been rescheduled by comparing with existing CSV records.
        
        Args:
            appointment_id: Appointment ID to check
            current_datetime: Current appointment datetime (formatted)
            is_canceled: Whether appointment is currently canceled
            
        Returns:
            True if appointment was rescheduled or status changed
        """
        if not appointment_id:
            return False
        
        # Find the CSV file for this appointment type
        # We need to check all form CSV files since we don't know which one contains this appointment
        if not os.path.exists(self.forms_dir):
            return False
        
        try:
            # Check all CSV files in the forms directory
            for filename in os.listdir(self.forms_dir):
                if not filename.endswith('.csv'):
                    continue
                
                csv_filepath = os.path.join(self.forms_dir, filename)
                try:
                    with open(csv_filepath, 'r', newline='', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            if row.get('Appointment ID', '') == appointment_id:
                                # Found existing record - check if datetime or status changed
                                existing_datetime = row.get('Appointment DateTime', '')
                                existing_canceled = row.get('Canceled', 'No')
                                current_canceled = 'Yes' if is_canceled else 'No'
                                
                                # If datetime changed or canceled status changed, it's a reschedule/change
                                if existing_datetime != current_datetime or existing_canceled != current_canceled:
                                    return True
                except Exception:
                    continue
            
            return False
        except Exception:
            return False
    
    def _write_form_csv(self, filepath: str, form_data: Dict):
        """
        Write form data to CSV, handling dynamic headers.
        Always appends new records, never overwrites existing ones.
        
        Args:
            filepath: Path to CSV file
            form_data: Dictionary of form data
        """
        file_exists = os.path.exists(filepath)
        
        # Check if this exact record already exists (all fields identical)
        if file_exists:
            try:
                # Create signature for the new record
                new_signature = self._create_record_signature(form_data)
                
                with open(filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        existing_signature = self._create_record_signature(row)
                        if existing_signature == new_signature:
                            # Exact duplicate - skip it
                            return
            except Exception:
                pass
        
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
        
        # Fix rescheduled field after writing
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
                    # Sort by Export Timestamp or Sync Timestamp to find the first one
                    sorted_records = sorted(
                        record_list, 
                        key=lambda x: x[1].get('Export Timestamp', '') or x[1].get('Sync Timestamp', '')
                    )
                    
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
    
    def _rewrite_csv_with_new_headers(self, filepath: str, new_headers: list, new_row: Dict):
        """
        Rewrite CSV file with updated headers, preserving all existing records.
        
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
        
        # Rewrite file with new headers, preserving all existing records
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=new_headers)
                writer.writeheader()
                # Write all existing records (missing fields will be empty)
                for row in existing_data:
                    # Ensure all new headers are present in row
                    for header in new_headers:
                        if header not in row:
                            row[header] = ''
                    writer.writerow(row)
                # Add the new row
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
            
            # Check for parts with names in parentheses (likely advisor/instructor names)
            if '(' in part and ')' in part:
                before_paren = part.split('(')[0].strip()
                # If the text before parentheses contains form keywords, use it
                if self.form_type_keywords and any(keyword in before_paren.lower() for keyword in self.form_type_keywords):
                    form_name = before_paren
                    break
                # If it looks like a person's name (short, capitalized, no keywords), skip it
                elif self._looks_like_person_name(before_paren):
                    continue
            
            # Check if this part contains a session keyword (if keywords are configured)
            if self.form_type_keywords and any(keyword in part_lower for keyword in self.form_type_keywords):
                form_name = part
                break
        
        # Fallback logic if no keyword match
        if not form_name:
            form_name = self._fallback_form_name(parts)
        
        return form_name
    
    def _looks_like_person_name(self, text: str) -> bool:
        """
        Check if text looks like a person's name.
        
        Args:
            text: Text to check
            
        Returns:
            True if it looks like a person's name
        """
        # Short text (typically 2-4 words)
        word_count = len(text.split())
        if word_count > 5:
            return False
        
        # Starts with capital letter
        if not text or not text[0].isupper():
            return False
        
        # Doesn't contain form keywords
        if self.form_type_keywords and any(keyword in text.lower() for keyword in self.form_type_keywords):
            return False
        
        # Common patterns for advisor appointments
        if any(pattern in text.lower() for pattern in ['help desk', 'q&a', 'session', 'appointment', 'workshop']):
            return False
        
        return True
    
    def _fallback_form_name(self, parts: list) -> str:
        """
        Fallback logic for extracting form name when no keywords match.
        
        Args:
            parts: List of appointment type parts
            
        Returns:
            Fallback form name
        """
        appointment_type = ' | '.join(parts)
        
        # Check if the first part (before any parentheses) looks like a person's name
        first_part = parts[0] if parts else appointment_type
        if '(' in first_part:
            before_paren = first_part.split('(')[0].strip()
            if self._looks_like_person_name(before_paren):
                return self.fallback_form_name
        
        # Check if entire appointment type looks like a person's name
        is_likely_name = (
            len(parts) <= 2 and
            len(appointment_type.split()) <= 4 and
            appointment_type[0].isupper() and
            (not self.form_type_keywords or not any(keyword in appointment_type.lower() for keyword in self.form_type_keywords))
        )
        
        if is_likely_name:
            return self.fallback_form_name
        
        # Filter out short parts and likely names
        meaningful_parts = [
            p for p in parts
            if len(p) > 10 and not re.match(r'^(free|paid|\$\d+)', p.lower())
        ]
        
        if meaningful_parts:
            return meaningful_parts[0]
        
        return appointment_type if appointment_type else self.fallback_form_name
    
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
        Create a unique signature from all field values (excluding timestamp fields).
        Used for deduplication by comparing all columns.
        
        Args:
            row: Dictionary of record data
            
        Returns:
            String signature representing all field values
        """
        # Create a sorted tuple of all key-value pairs (excluding timestamp fields)
        fields = []
        timestamp_fields = {'Export Timestamp', 'Sync Timestamp', 'Timestamp'}
        
        for key, value in sorted(row.items()):
            if key not in timestamp_fields:
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
            # Map timezone abbreviations to timezone objects for parsing
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
            
            # Parse the datetime string with timezone info
            dt = date_parser.parse(datetime_str, tzinfos=tzinfos)
            
            # Convert to EST/EDT (handles daylight saving automatically)
            
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

