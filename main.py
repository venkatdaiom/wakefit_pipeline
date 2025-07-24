# --- THIS IS THE NEW, COMPLETE main.py SCRIPT (VERSION 3) ---

import pandas as pd
import numpy as np
import googlemaps
import gspread
import gspread_dataframe as gd
import json
import os
import re
from datetime import datetime
from google.oauth2.service_account import Credentials
from google.cloud import secretmanager

# --- HELPER FUNCTIONS ---
def extract_place_id_from_url(url):
    if not isinstance(url, str): return None
    match = re.search(r'place_id:([^&.?]+)', url)
    if match: return match.group(1)
    if 'place_id:' not in url and '/place/' in url:
        parts = url.split('/')
        for part in reversed(parts):
            potential_id = part.split('?')[0].split('!')[0]
            if (potential_id.startswith('ChIJ') or potential_id.startswith('GhIJ')) and len(potential_id) > 20:
                return potential_id
    return None

# --- MODIFIED HELPER FUNCTION ---
def get_store_details(place_id_or_url, gmaps_client):
    place_id = extract_place_id_from_url(place_id_or_url) if 'https://' in str(place_id_or_url) else place_id_or_url
    if not place_id:
        print(f"Could not extract Place ID from URL: {place_id_or_url}")
        return {'Original URL': place_id_or_url, 'Error': 'Could not extract Place ID'}
    try:
        # We now only request the fields we absolutely need
        fields = ['rating', 'user_ratings_total', 'place_id', 'business_status'] # Keep business_status to filter closed stores
        place_details = gmaps_client.place(place_id=place_id, fields=fields)
        if place_details.get('status') == 'OK':
            result = place_details['result']
            return {
                'Original URL': place_id_or_url,
                'Total Reviews': result.get('user_ratings_total'),
                'Rating': result.get('rating'),
                'Api Business Status': result.get('business_status'), # Use a unique name
                'Error': None
            }
        else:
            return {'Original URL': place_id_or_url, 'Error': f"API Error: {place_details.get('status')}"}
    except Exception as e:
        return {'Original URL': place_id_or_url, 'Error': str(e)}

def get_secret(project_id, secret_id, version_id="latest"):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# --- MAIN PIPELINE FUNCTION ---
def run_gmb_pipeline(request):
    try:
        # --- 1. CONFIGURATION & AUTHENTICATION ---
        GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
        PLACES_API_KEY = get_secret(GCP_PROJECT_ID, 'google-places-api-key')
        gmaps = googlemaps.Client(key=PLACES_API_KEY)
        
        gcp_creds_json_str = get_secret(GCP_PROJECT_ID, 'gcp-service-account-creds')
        gcp_creds_json = json.loads(gcp_creds_json_str)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_info(gcp_creds_json, scopes=scopes)
        gc = gspread.authorize(creds)
        
        INPUT_SHEET_NAME = 'Wakefit_GMB_Pipeline_Input'
        INPUT_WORKSHEET_NAME = 'store_data'
        OUTPUT_SHEET_NAME = 'Wakefit_GMB_Pipeline_Output'
        
        # --- 2. READ INPUT DATA ---
        print("Reading input data from Google Sheet...")
        workbook = gc.open(INPUT_SHEET_NAME)
        worksheet = workbook.worksheet(INPUT_WORKSHEET_NAME)
        df_input = pd.DataFrame(worksheet.get_all_records())
        print(f"Found {len(df_input)} stores in the input sheet.")

        # --- 3. FETCH DATA FROM GOOGLE PLACES API ---
        print("Fetching live data from Google Places API...")
        all_store_data = []
        store_urls = df_input['store locator'].dropna().tolist()
        for url in store_urls:
            details = get_store_details(url, gmaps)
            all_store_data.append(details)
        
        df_fetched = pd.DataFrame(all_store_data)

        # --- 4. MERGE AND PREPARE DATA ---
        print("Merging fetched data with input data...")
        df_input_renamed = df_input.rename(columns={'store locator': 'Original URL'})
        
        # Select ONLY the columns we need from the API results before merging
        cols_to_merge = ['Original URL', 'Total Reviews', 'Rating', 'Api Business Status']
        
        merged_df = pd.merge(
            df_input_renamed,
            df_fetched[cols_to_merge],
            on='Original URL',
            how='left'
        )
        merged_df['Total Reviews'] = pd.to_numeric(merged_df['Total Reviews']).fillna(0)
        merged_df['Rating'] = pd.to_numeric(merged_df['Rating']).fillna(0)

        # --- 5. CALCULATE KPIs ---
        print("Calculating aggregated KPIs...")
        # Use the new, uniquely named column from the API to filter
        df = merged_df[merged_df['Api Business Status'] != 'PERMANENTLY_CLOSED'].copy()
        df.rename(columns={'Rating': 'Visible Rating'}, inplace=True)
        
        snapshot_date = datetime.now().strftime('%Y-%m-%d')
        kpi_results = []
        
        segments_config = {
            'All Wakefit Stores': df,
            'COCO Stores': df[df['Experience Center'] == 1],
            'Non COCO Stores': df[df['Experience Center'] == 0],
            'North': df[df['Region'] == 'North'],
            'South': df[df['Region'] == 'South'],
            'East': df[df['Region'] == 'East'],
            'West': df[df['Region'] == 'West'],
        }

        for name, temp_df in segments_config.items():
            total_reviews = temp_df['Total Reviews'].sum()
            avg_rating = np.average(temp_df['Visible Rating'], weights=temp_df['Total Reviews']) if total_reviews > 0 else 0
            kpi_results.append({
                'Segment': name,
                'Total Reviews': total_reviews,
                'Visible Rating': avg_rating,
                'No. of Stores': len(temp_df),
                '≥4.9 Rating': len(temp_df[temp_df['Visible Rating'] >= 4.9]),
                '>4 Rating': len(temp_df[(temp_df['Visible Rating'] >= 4) & (temp_df['Visible Rating'] < 4.9)]),
                '<4 Rating': len(temp_df[temp_df['Visible Rating'] < 4]),
                'Snapshot date': snapshot_date
            })

        kpi_df = pd.DataFrame(kpi_results)

        # --- 6. GENERATE JSON OUTPUT ---
        print("Generating JSON output...")
        # For the final output, we can drop the temporary API status column if we want
        output_store_data = merged_df.drop(columns=['Api Business Status'])
        store_data_json = output_store_data.to_dict(orient='records')
        kpi_data_json = kpi_df.to_dict(orient='records')
        
        final_json_output = {
            'snapshot_timestamp': datetime.now().isoformat(),
            'store_level_data': store_data_json,
            'aggregated_kpi_data': kpi_data_json
        }
        
        print(json.dumps(final_json_output, indent=2))

        # --- 7. WRITE TO GOOGLE SHEET ---
        print("Writing data to output Google Sheet...")
        output_workbook = gc.open(OUTPUT_SHEET_NAME)
        
        store_worksheet_name = f"Store_Data_{snapshot_date}"
        try:
            store_worksheet = output_workbook.worksheet(store_worksheet_name)
            store_worksheet.clear()
        except gspread.WorksheetNotFound:
            store_worksheet = output_workbook.add_worksheet(title=store_worksheet_name, rows=1000, cols=30)
        # Write the data without the temporary column
        gd.set_with_dataframe(store_worksheet, output_store_data)

        kpi_worksheet_name = f"KPI_Data_{snapshot_date}"
        try:
            kpi_worksheet = output_workbook.worksheet(kpi_worksheet_name)
            kpi_worksheet.clear()
        except gspread.WorksheetNotFound:
            kpi_worksheet = output_workbook.add_worksheet(title=kpi_worksheet_name, rows=100, cols=20)
        gd.set_with_dataframe(kpi_worksheet, kpi_df)
        
        print("Successfully updated Google Sheet.")

        return "Pipeline executed successfully.", 200

    except Exception as e:
        print(f"!!!!!!!!!!!!!! CRITICAL ERROR IN PIPELINE !!!!!!!!!!!!!!")
        import traceback
        print(traceback.format_exc()) # Print the full traceback for better debugging
        return f"Pipeline failed: {e}", 500
