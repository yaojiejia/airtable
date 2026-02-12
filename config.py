"""
Centralized configuration for Acuity-Airtable SDK.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Configuration class for Acuity-Airtable SDK."""
    
    ACUITY_USER_ID = os.getenv("ACUITY_USER_ID")
    ACUITY_API_KEY = os.getenv("ACUITY_API_KEY")
    ACUITY_BASE_URL = "https://acuityscheduling.com/api/v1"
    
    AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
    AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
    AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
    
    CSV_LOG_FILE = "acuity_records.csv"
    FORMS_CSV_DIR = "csv_exports"
    
    CSV_LOG_HEADERS = [
        'Timestamp',
        'Appointment ID',
        'Client Name',
        'Email',
        'Phone',
        'Appointment DateTime',
        'Appointment Type',
        'Status',
        'Canceled',
        'Date Created',
        'Airtable Record ID',
        'Notes'
    ]
    
    FORM_CSV_BASE_HEADERS = [
        'Timestamp',
        'Appointment ID',
        'Client Name',
        'Email',
        'Phone',
        'Appointment DateTime',
        'Canceled',
        'Rescheduled'
    ]
    
    # Note: FORM_TYPE_KEYWORDS and form name extraction logic are now configurable
    # via CSVLogger initialization. See csv_logger.py for details.
    
    MULTI_SELECT_INDICATORS = ['check all that apply', 'select all']
    
    # Specific fields that should be treated as multi-select
    MULTI_SELECT_FIELDS = ['What is your current NYU status?']
    
    DEFAULT_LOOKBACK_HOURS = 24
    MAX_APPOINTMENTS = 100
    MAX_AIRTABLE_RECORDS_TO_SCAN = 100
    
    @classmethod
    def validate(cls):
        """Validate required configuration values."""
        missing = []
        if not cls.ACUITY_USER_ID:
            missing.append("ACUITY_USER_ID")
        if not cls.ACUITY_API_KEY:
            missing.append("ACUITY_API_KEY")
        if not cls.AIRTABLE_API_KEY:
            missing.append("AIRTABLE_API_KEY")
        if not cls.AIRTABLE_BASE_ID:
            missing.append("AIRTABLE_BASE_ID")
        if not cls.AIRTABLE_TABLE_NAME:
            missing.append("AIRTABLE_TABLE_NAME")
        return (len(missing) == 0, missing)
    
    @classmethod
    def get_acuity_auth(cls):
        """Get Acuity authentication tuple."""
        return (cls.ACUITY_USER_ID, cls.ACUITY_API_KEY)


config = Config()

