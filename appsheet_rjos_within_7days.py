# ================================================= Import SLI Apsheet =================================================

import requests
import warnings
from datetime import datetime
import pytz
import pandas as pd

# Suppress warnings
warnings.filterwarnings("ignore")

# AppSheet API credentials
app_id = '86cf92e7-c5be-435e-8366-25989bb00035'
application_access_key = 'V2-6rM8g-QR4Pi-xO6ea-8lwbN-3pcFF-DVPXM-3T5MS-Tijsl'


# ✅ Function to query AppSheet table and return DataFrame
def fetch_table_data(table_name):
    url = f"https://api.appsheet.com/api/v2/apps/{app_id}/tables/{table_name}/query"

    headers = {
        'ApplicationAccessKey': application_access_key,
        'Content-Type': 'application/json'
    }

    payload = {
        "Action": "Find",
        "Properties": {
            "Locale": "en-US",
            "Timezone": "UTC"
        },
        "Rows": []
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        try:
            data = response.json()
            if isinstance(data, list):
                print(f"{table_name}: {len(data):,} rows imported")
                return pd.DataFrame(data)
            else:
                print(f"Unexpected data format from {table_name}: {type(data)}")
                return pd.DataFrame()
        except ValueError:
            print(f"Failed to parse JSON from {table_name}")
            print(response.text)
            return pd.DataFrame()
    else:
        print(f"Failed to retrieve data from {table_name}. Status code: {response.status_code}")
        print(response.text)
        return pd.DataFrame()


# ✅ Import tables
df_s2d = fetch_table_data('S2D')
df_s2d['JO SOURCE'] = 'FS'

df_s2d_archive = fetch_table_data('S2D ARCHIVE1')
df_s2d_archive['JO SOURCE'] = 'FS'

df_cict = fetch_table_data('CICT')
df_cict['JO SOURCE'] = 'CICT-SDU'
df_cict['OMD STATUS'] = 'For Schedule of Installation'

df_cict_mdu = fetch_table_data('CICT MDU')
df_cict_mdu['JO SOURCE'] = 'CICT-MDU'
df_cict_mdu['OMD STATUS'] = 'For Schedule of Installation'

# ✅ Combine all DataFrames
df_appsheet = pd.concat([df_s2d, df_s2d_archive, df_cict, df_cict_mdu], ignore_index=True)
print(f"Total rows imported: {df_appsheet.shape[0]:,}")

# ✅ Get current date and time in the Philippines
philippines_tz = pytz.timezone('Asia/Manila')
current_time = datetime.now(philippines_tz)
formatted_time = current_time.strftime("%B %d, %Y %I:%M %p")
print(f"Imported as of: {formatted_time}")

# ✅ Copy DataFrame for processing
df = df_appsheet.copy()


# ✅ Timezone Conversion (safe handling)
def safe_localize(column):
    if column in df.columns:
        return pd.to_datetime(df[column], errors='coerce').dt.tz_localize(philippines_tz, ambiguous='NaT',
                                                                          nonexistent='shift_forward')
    return pd.NaT


df['ACCOUNT NUMBER TIMESTAMP'] = safe_localize('ACCOUNT NUMBER TIMESTAMP')
df['LAST TECH STATUS CHANGE DATE AND TIME'] = safe_localize('LAST TECH STATUS CHANGE DATE AND TIME')
df['OMD LAST STATUS CHANGE'] = safe_localize('OMD LAST STATUS CHANGE')

# ✅ Fill missing timestamps using other available columns
condition = df['ACCOUNT NUMBER TIMESTAMP'].isna() & df['ACCOUNT NUMBER'].notna()
df.loc[condition, 'ACCOUNT NUMBER TIMESTAMP'] = df.loc[condition, 'OMD LAST STATUS CHANGE']

# ✅ Create SDU IDENTIFIER with safe handling
df['SDU IDENTIFIER'] = (
        (df.get('JO SOURCE') == 'CICT-SDU') |
        ((df.get('JO SOURCE') == 'FS') & (df.get('TAG') == 'MYRIAD-SDU'))
)

# ✅ Default missing column to False if not created
if 'SDU IDENTIFIER' not in df.columns:
    df['SDU IDENTIFIER'] = False

# ✅ Status Mapping for OVERALL STATUS
status_map = {
    'INSTALLED AND ACTIVATED': 'ACTIVE',
    'FOR VISIT': 'OPEN',
    'FOR DISPATCH': 'OPEN'
}

df['OVERALL STATUS'] = df['TECH STATUS'].map(status_map).fillna(df['OVERALL STATUS'])

# ✅ Fix 'INSTALLABLE' status to 'OPEN'
df['OVERALL STATUS'] = df['OVERALL STATUS'].replace('INSTALLABLE', 'OPEN')

# ✅ Calculate Ageing (HRS)
current_datetime = datetime.now(philippines_tz)
mask = df['OVERALL STATUS'].isin(['OPEN', '']) & df['ACCOUNT NUMBER TIMESTAMP'].notna()

df.loc[mask, 'AGEING (HRS)'] = (
        (current_datetime - df['ACCOUNT NUMBER TIMESTAMP']).dt.total_seconds() / 3600
)

# ✅ Convert Ageing to DAYS
df['AGEING (DAYS)'] = df['AGEING (HRS)'] / 24


# ✅ Create Ageing Range
def determine_ageing_range(ageing_hrs):
    if pd.isna(ageing_hrs):
        return ''
    if ageing_hrs <= 4:
        return '1-4 Hrs'
    elif ageing_hrs <= 12:
        return '>4-12 Hrs'
    elif ageing_hrs <= 24:
        return '>12-24 Hrs'
    elif ageing_hrs <= 48:
        return '>24-48 Hrs'
    elif ageing_hrs <= 72:
        return '>2-3 Days'
    elif ageing_hrs <= 96:
        return '>3-4 Days'
    elif ageing_hrs <= 120:
        return '>4-5 Days'
    elif ageing_hrs <= 240:
        return '>5-10 Days'
    elif ageing_hrs <= 360:
        return '>10-15 Days'
    elif ageing_hrs <= 480:
        return '>15-20 Days'
    else:
        return '>20 Days'


df['AGEING (RANGE)'] = df['AGEING (HRS)'].apply(determine_ageing_range)

# ✅ Convert LAST TECH STATUS CHANGE DATE AND TIME to date only
df['LAST TECH STATUS CHANGE DATE'] = df['LAST TECH STATUS CHANGE DATE AND TIME'].dt.date

# ✅ Create JO TYPE based on JO SOURCE
df['JO TYPE'] = df['JO SOURCE'].map({
    'FS': 'Field Sales',
    'CICT-SDU': 'CICT',
    'CICT-MDU': 'CICT'
})

# ✅ Create SLI ENDORSED DATE from ACCOUNT NUMBER TIMESTAMP
df['SLI ENDORSED DATE'] = df['ACCOUNT NUMBER TIMESTAMP'].dt.date

# ✅ Safe Filter for CLUSTER 3
if 'SDU IDENTIFIER' in df.columns:
    df_c3 = df[
        (df['CLUSTER'] == 'CLUSTER 3') &
        (df['ACCOUNT NUMBER'] != '') &
        (df['SDU IDENTIFIER'] == True)
        ]
    print(f"Filtered CLUSTER 3 rows: {df_c3.shape[0]:,}")
else:
    print("'SDU IDENTIFIER' column missing — skipping filter.")

# ✅ Final Output Summary
print(f"Processed DataFrame size: {df.shape[0]:,} rows")

# ================================================= Add columns =================================================

df_c3['ACCOUNT NUMBER TIMESTAMP'] = pd.to_datetime(df_c3['ACCOUNT NUMBER TIMESTAMP']).dt.tz_localize(None)
df_c3['TECH TEAM ASSIGNED TO TIMESTAMP'] = pd.to_datetime(df_c3['TECH TEAM ASSIGNED TO TIMESTAMP']).dt.tz_localize(None)

df_c3['DISPATCH TAT (HOURS)'] = (df_c3['TECH TEAM ASSIGNED TO TIMESTAMP'] - df_c3['ACCOUNT NUMBER TIMESTAMP']).dt.total_seconds() / 3600

# ✅ Create Ageing Range
def determine_ageing_range(ageing_hrs):
    if pd.isna(ageing_hrs):
        return ''
    if ageing_hrs <= 4:
        return '1-4 Hrs'
    elif ageing_hrs <= 12:
        return '>4-12 Hrs'
    elif ageing_hrs <= 24:
        return '>12-24 Hrs'
    elif ageing_hrs <= 48:
        return '>24-48 Hrs'
    elif ageing_hrs <= 72:
        return '>2-3 Days'
    elif ageing_hrs <= 96:
        return '>3-4 Days'
    elif ageing_hrs <= 120:
        return '>4-5 Days'
    elif ageing_hrs <= 240:
        return '>5-10 Days'
    elif ageing_hrs <= 360:
        return '>10-15 Days'
    elif ageing_hrs <= 480:
        return '>15-20 Days'
    else:
        return '>20 Days'


df_c3['DISPATCH TAT (RANGE)'] = df_c3['DISPATCH TAT (HOURS)'].apply(determine_ageing_range)

df_c3_fs = df_c3[df_c3['JO TYPE'] == 'Field Sales']
print(f"Filtered C3 FS rows: {df_c3_fs.shape[0]:,}")

df_c3_fs = df_c3[df_c3['JO TYPE'] == 'Field Sales']
print(f"Filtered C3 FS rows: {df_c3_fs.shape[0]:,}")


# Round to the nearest hour
df_c3_fs["ACCOUNT CREATION TIME"] = df_c3_fs["ACCOUNT NUMBER TIMESTAMP"].dt.round("H").dt.strftime("%I%p")

# Remove leading zero from the hour
df_c3_fs["ACCOUNT CREATION TIME"] = df_c3_fs["ACCOUNT CREATION TIME"].str.lstrip("0")

# ================================== Filter by Status ==================================

valid_statuses = [
    'HIGH LOSS',
    'HOLD BY CLIENT',
    'CLIENT UNCONTACTED',
    'RE-SCHEDULE',
    'VISITED CLIENT UNCONTACTED',
    'CLIENT UNCONTACTED / UNKNOWN PERSON',
    'CLIENT UNCONTACTED / WRONG ADDRESS',
    'PORT ISSUE/FULL NAP'
]

df_rjos= df_c3_fs[df_c3_fs["TECH STATUS"].isin(valid_statuses)]

# ================================== Ageing ==================================

import pandas as pd
from datetime import datetime
import pytz

# Convert to datetime if needed
df_rjos["ACCOUNT NUMBER TIMESTAMP"] = pd.to_datetime(df_rjos["ACCOUNT NUMBER TIMESTAMP"], errors='coerce')

# Define PH timezone
ph_timezone = pytz.timezone("Asia/Manila")

# Localize timestamp column (assume they are in PH time but naive)
df_rjos["ACCOUNT NUMBER TIMESTAMP"] = df_rjos["ACCOUNT NUMBER TIMESTAMP"].dt.tz_localize(ph_timezone)

# Get current datetime in PH (timezone-aware)
now_ph = datetime.now(ph_timezone)

# Calculate ageing in days
df_rjos["rjo_ageing_days"] = (now_ph - df_rjos["ACCOUNT NUMBER TIMESTAMP"]).dt.days

# ================================== Arange Cols ==================================

# Define the desired column order
first_columns = ['TECH STATUS', 'REFERENCE NUMBER', 'TECHNICIAN REMARKS', 'SALES AGENT NAME', 'rjo_ageing_days']

# Reorder the DataFrame columns
df_rjos = df_rjos[first_columns + [col for col in df_rjos.columns if col not in first_columns]]

# ================================== Filter ==================================

df_rjos_7days = df_rjos[df_rjos["rjo_ageing_days"] <= 7]

# ================================== Download ==================================

import os
from datetime import datetime

# Optional: rename filtered DataFrame
df_rjos_7days = df_rjos[df_rjos["rjo_ageing_days"] <= 7]

# Get path to Downloads folder
downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")

# Get current date and time (formatted as YYYY-MM-DD_HHMM)
timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")

# Set filename with timestamp
filename = f"df_rjos_7days_{timestamp}.csv"
file_path = os.path.join(downloads_path, filename)

# Export to CSV
df_rjos_7days.to_csv(file_path, index=False)

print(f"File saved to: {file_path}")


