@echo off
REM Daily sync script for Acuity -> Airtable integration
cd /d C:\Users\alexj\Documents\airtable
call venv\Scripts\activate.bat
python main.py

