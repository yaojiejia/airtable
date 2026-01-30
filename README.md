# Airtable & Acuity Integration

Complete integration between Acuity Scheduling and Airtable:
- **Automatically** monitors Acuity for new intake form submissions
- **Automatically** injects new forms into Airtable with field mapping
- **Logs ALL records** to CSV (including cancelled/rescheduled)
- **Tracks status changes** (cancellations, reschedules)
- **Avoids duplicates** with persistent tracking
- **Updates** "Last Update" field with current date

## 🚀 Quick Start

### Full Integration (Recommended)

**Run the complete Acuity → Airtable integration:**

```powershell
# Check every 5 minutes (default)
python main.py

# Check every 10 minutes
python main.py 10

# Check every hour
python main.py 60
```

**This will:**
1. ✅ Monitor Acuity for new intake form submissions
2. ✅ Automatically map matching fields
3. ✅ Inject new forms into Airtable
4. ✅ Add "Last Update" timestamp
5. ✅ Avoid duplicates
6. ✅ Run continuously until stopped (Ctrl+C)

---

## Setup

1. **Create and activate virtual environment:**
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

2. **Install dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

3. **Configure credentials in `.env` file:**
   - Acuity: `ACUITY_USER_ID`, `ACUITY_API_KEY`
   - Airtable: `AIRTABLE_API_KEY`, `AIRTABLE_BASE_ID`, `AIRTABLE_TABLE_NAME`

## Individual Scripts

### Acuity - One-time Check

Fetch all intake forms from the last 24 hours:

```powershell
cd acuity
python acuity_intake_check.py

# Fetch ONE record as JSON
python acuity_intake_check.py --one
```

### Acuity - Continuous Monitoring

Monitor for new intake forms (without Airtable injection):

```powershell
cd acuity
python acuity_main.py        # Every 5 minutes
python acuity_main.py 60     # Every 60 minutes
```

### Airtable - Fetch Records

```powershell
cd airtable
python airtable_main.py
```

### Airtable - Manual Injection

Manually inject one Acuity record into Airtable:

```powershell
cd airtable
python airtable_main.py --inject
```

Compare fields between Acuity and Airtable:

```powershell
python airtable_main.py --compare
```

### Testing - Mock Data

Test with mock data (no real API calls):

```powershell
cd airtable

# Test mapping without injection
python test_injection.py 1

# Actually inject mock data into Airtable
python test_injection.py --inject 1   # TEST STUDENT
python test_injection.py --inject 2   # James Rodriguez
python test_injection.py --inject 3   # Emily Thompson

# Inject all 3 mock records
python test_injection.py --inject-all
```

## 📊 CSV Logging

### Main Log: `acuity_records_log.csv`
ALL Acuity records are logged:
- Sync timestamp, appointment details
- Status (Active/Cancelled), action taken
- Airtable injection status

### Form-Specific CSVs: `forms_csv/`
**NEW!** Each form type gets its own CSV with full Q&A data:
- `product_development_help_desk.csv`
- `legal_help_desk.csv`
- `financial_help_desk.csv`
- `advisor_1_on_1_session.csv`
- ... one file per form type

**See [`FORMS_CSV_README.md`](FORMS_CSV_README.md) for complete documentation.**

### Benefits
- ✅ Easy per-form analysis
- ✅ All intake questions and answers preserved
- ✅ Dynamic columns (auto-expands when forms change)
- ✅ Clean file naming (no instructor names)
- ✅ Ready for Excel/Google Sheets/Python analysis

---

## 🔄 How It Works

### Field Mapping

The integration automatically:
1. Fetches Airtable column names
2. Extracts Acuity form field names
3. Matches fields with identical names (case-sensitive, whitespace-stripped)
4. Maps only matching fields to avoid errors

**Currently matching 12-13 fields:**
- Name
- Email
- NYU Status, Net ID, School, Degree/Program
- MBA Status
- Graduation Year
- Venture Name, Stage, Description
- Founding Team Members
- First-time Founder (multi-select array)

### Auto-Generated Fields

- **Last Update**: Automatically set to current date (YYYY-MM-DD format)

### Duplicate Prevention

Tracks processed Acuity appointment IDs in memory to avoid re-injecting the same form during a session.

### Multi-Select Fields

Fields containing "check all that apply" are automatically converted to arrays for Airtable's multi-select field type.

## 📁 Project Structure

```
airtable/
├── main.py                          # 🚀 MAIN INTEGRATION SCRIPT
├── acuity/
│   ├── acuity_intake_check.py      # Fetch Acuity intake forms
│   ├── acuity_main.py              # Continuous Acuity monitoring
│   └── intake_form_example.json    # Sample form data
├── airtable/
│   ├── airtable_main.py            # Airtable operations
│   ├── update_students.py          # Mapping & injection functions
│   └── test_injection.py           # Mock data testing
├── requirements.txt
├── .env                            # Credentials (not in git)
├── .env.example                    # Template
└── README.md
```

## API Credentials

### Acuity Scheduling
- **Get credentials:** https://acuityscheduling.com/app.php?key=api&action=settings
- **User ID:** Your account User ID (number)
- **API Key:** Your API key (long string)

### Airtable
- **Create token:** https://airtable.com/create/tokens
- **Base ID:** Found in your base URL (starts with `app`)
- **Table Name:** The name of your table (e.g., "Student Profile")

## Dependencies

- `pyairtable>=2.3.0` - Airtable API client
- `requests>=2.31.0` - HTTP requests for Acuity API
- `flask>=3.0.0` - Web framework
- `python-dotenv>=1.0.0` - Environment variable management

## Notes

- All JSON files use UTF-8 encoding
- Acuity monitoring uses polling (no webhooks/ngrok needed)
- Scripts handle Windows console encoding issues
- Credentials stored securely in `.env` file (not committed to git)
- Press `Ctrl+C` to stop any running script
