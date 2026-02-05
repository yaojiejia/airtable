"""
Streamlit App for Acuity-Airtable Sync

Provides a web interface to:
- Run sync with user-defined hours
- View CSV forms and their contents
"""
import streamlit as st
import pandas as pd
import os
import traceback
from pathlib import Path
from datetime import datetime
from acuity_airtable_sdk import AcuityAirtableSDK
import io
import sys


# Page configuration
st.set_page_config(
    page_title="Acuity-Airtable Sync",
    page_icon="🔄",
    layout="wide"
)

# Title
st.title("🔄 Acuity-Airtable Sync Dashboard")

# Sidebar for sync controls
with st.sidebar:
    st.header("Sync Controls")
    
    hours = st.number_input(
        "Lookback Hours",
        min_value=1,
        max_value=168,  # 1 week
        value=24,
        step=1,
        help="Number of hours to look back for appointments"
    )
    
    if st.button("🚀 Run Sync", type="primary", use_container_width=True):
        with st.spinner("Running sync... This may take a moment."):
            try:
                # Run the sync
                # Business-specific configuration: form type keywords for extracting form names
                form_type_keywords = [
                    'help desk', 'helpdesk', 'q&a', 'q & a', 'session',
                    'essentials', 'advising', 'workshop', 'clinic', 'appointment'
                ]
                
                sdk = AcuityAirtableSDK(
                    form_type_keywords=form_type_keywords,
                    fallback_form_name="advisor_1_on_1_session"
                )
                sdk.airtable.use_table("Student Profile")
                
                # Step 1: Sync to Airtable
                results = sdk.sync(
                    hours=hours,
                    include_canceled=True,
                    verbose=False,  # Disable verbose to reduce output
                    timestamp_field="Last Update"
                )
                
                # Step 2: Export to CSV
                csv_files = sdk.export_to_csv(
                    hours=hours,
                    include_canceled=True,
                    group_by_appointment_type=True,
                    output_dir="forms_csv"
                )
                
                # Store results in session state
                st.session_state['last_sync_results'] = results
                st.session_state['last_sync_csv_files'] = csv_files
                st.session_state['last_sync_time'] = datetime.now()
                st.session_state['last_sync_hours'] = hours
                
                st.success("✅ Sync completed successfully!")
                st.rerun()
                
            except Exception as e:
                st.error(f"❌ Sync failed: {str(e)}")
                with st.expander("View Error Details"):
                    st.code(traceback.format_exc())

# Main content area
tab1, tab2 = st.tabs(["📊 Sync Results", "📁 CSV Forms"])

with tab1:
    st.header("Last Sync Results")
    
    if 'last_sync_results' in st.session_state:
        results = st.session_state['last_sync_results']
        csv_files = st.session_state['last_sync_csv_files']
        sync_time = st.session_state.get('last_sync_time', datetime.now())
        
        # Display summary
        sync_hours = st.session_state.get('last_sync_hours', hours)
        st.info(f"📅 Last sync: {sync_time.strftime('%Y-%m-%d %H:%M:%S')} | Lookback: {sync_hours} hours")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Forms Fetched", results['forms_fetched'])
        with col2:
            st.metric("Successfully Synced", results['successful'], delta=None)
        with col3:
            st.metric("Failed", results['failed'], delta=None, delta_color="inverse")
        with col4:
            st.metric("CSV Files Created", len(csv_files))
        
        # Display errors if any
        if results['failed'] > 0:
            st.warning(f"⚠️ {results['failed']} record(s) failed to sync")
            with st.expander("View Errors"):
                for error in results['errors']:
                    st.error(f"**{error['form'].get('client_name', 'Unknown')}**: {error['error']}")
        
        # Display CSV files created
        if csv_files:
            st.subheader("CSV Files Created")
            for form_type, filepath in csv_files.items():
                st.write(f"📄 **{form_type}**: `{filepath}`")
    else:
        st.info("👈 Run a sync from the sidebar to see results here")

with tab2:
    st.header("CSV Forms Viewer")
    
    # Get all CSV files
    csv_dir = Path("forms_csv")
    if not csv_dir.exists():
        st.warning("No CSV files directory found. Run a sync first.")
    else:
        csv_files = list(csv_dir.glob("*.csv"))
        
        if not csv_files:
            st.info("No CSV files found. Run a sync first.")
        else:
            # File selector
            selected_file = st.selectbox(
                "Select CSV File",
                options=csv_files,
                format_func=lambda x: x.name
            )
            
            if selected_file:
                try:
                    # Read and display CSV
                    df = pd.read_csv(selected_file)
                    
                    # Display file info
                    st.info(f"📄 **{selected_file.name}** - {len(df)} records")
                    
                    # Display dataframe
                    st.dataframe(
                        df,
                        use_container_width=True,
                        height=400
                    )
                    
                    # Download button
                    csv_data = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_data,
                        file_name=selected_file.name,
                        mime="text/csv"
                    )
                
                except Exception as e:
                    st.error(f"Error reading CSV file: {str(e)}")

