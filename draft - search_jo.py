# ================================================= Import SLI Appsheet =================================================

import requests
import warnings
from datetime import datetime
import pytz
import panel as pn
import pandas as pd
import matplotlib.pyplot as plt

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

# ✅ Final Output Summary
print(f"Processed DataFrame size: {df.shape[0]:,} rows")