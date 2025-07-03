
# ================================================= Import SLI Apsheet =================================================

import requests
import warnings
import pytz
import pandas as pd
import panel as pn
import datetime
from datetime import datetime

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

# ================================================= Generate SLI Open JOs Report =================================================

import pandas as pd
import panel as pn
import datetime

# Enable Panel extensions
pn.extension()

# Set timezone to Philippines (UTC+8)
pd.set_option('display.float_format', '{:.0f}'.format)

# Convert "LAST TECH STATUS CHANGE DATE" to datetime
df_c3["LAST TECH STATUS CHANGE DATE"] = pd.to_datetime(df_c3["LAST TECH STATUS CHANGE DATE"], format='%m/%d/%Y')

# Ensure JO TYPE and MUNICIPALITY are treated as strings (to avoid filtering issues)
df_c3["JO TYPE"] = df_c3["JO TYPE"].astype(str).str.strip().str.lower()
df_c3["MUNICIPALITY"] = df_c3["MUNICIPALITY"].astype(str).str.strip().str.lower()

# Define default start and end dates (Philippine time reference)
ph_time = datetime.datetime.utcnow() + datetime.timedelta(hours=8)
start_date_default = pd.to_datetime(ph_time.replace(day=1).date())
end_date_default = pd.to_datetime((ph_time - datetime.timedelta(days=1)).date())

# New Calendar Widgets for Start and End Dates
start_date_picker = pn.widgets.DatePicker(name="Start Date", value=start_date_default)
end_date_picker = pn.widgets.DatePicker(name="End Date", value=end_date_default)

# JO Type Filter
jo_types = ["All Type"] + sorted(df_c3["JO TYPE"].unique().tolist())
jo_filter = pn.widgets.Select(name='JO TYPE', options=jo_types, value="All Type")

# MUNICIPALITY Filter
municipalities = ["All Cities"] + sorted(df_c3["MUNICIPALITY"].unique().tolist())
municipality_filter = pn.widgets.Select(name='MUNICIPALITY', options=municipalities, value="All Cities")


# Function to update table
def update_table():
    filtered_df = df_c3.copy()

    # Ensure date selections are valid
    start_date = pd.to_datetime(start_date_picker.value)
    end_date = pd.to_datetime(end_date_picker.value)
    if start_date is None or end_date is None or start_date > end_date:
        return pd.DataFrame()  # Return empty table if invalid

    filtered_df = filtered_df[
        (filtered_df["LAST TECH STATUS CHANGE DATE"] >= start_date) &
        (filtered_df["LAST TECH STATUS CHANGE DATE"] <= end_date)
        ]

    # JO TYPE Filter
    if jo_filter.value.lower() != "all type":
        filtered_df = filtered_df[filtered_df["JO TYPE"].str.lower() == jo_filter.value.lower()]

    # MUNICIPALITY Filter
    if municipality_filter.value.lower() != "all cities":
        filtered_df = filtered_df[filtered_df["MUNICIPALITY"].str.lower() == municipality_filter.value.lower()]

    if filtered_df.empty:
        return pd.DataFrame()  # Return empty table if no data

    # Pivot table
    pivot_df = filtered_df.pivot_table(
        index="LAST TECH STATUS CHANGE DATE",
        columns="TECH STATUS",
        values="_RowNumber",
        aggfunc='count',
        fill_value=0
    )

    # Add Total column (sum of each row)
    pivot_df["Total"] = pivot_df.sum(axis=1)

    # Add Hit Rate (%) column
    if "INSTALLED AND ACTIVATED" in pivot_df.columns:
        pivot_df["Hit Rate (%)"] = ((pivot_df["INSTALLED AND ACTIVATED"] / pivot_df["Total"]) * 100).fillna(0).astype(
            int).map('{}%'.format)
        pivot_df["Total RJO"] = pivot_df["Total"] - pivot_df["INSTALLED AND ACTIVATED"]
    else:
        pivot_df["Hit Rate (%)"] = "0%"
        pivot_df["Total RJO"] = pivot_df["Total"]

    # Add RJO (%) column
    pivot_df["RJO (%)"] = (100 - pivot_df["Hit Rate (%)"].str.rstrip('%').astype(float)).astype(int).map('{}%'.format)

    # Define column order
    base_columns = ["Hit Rate (%)", "RJO (%)", "Total", "Total RJO"]
    optional_columns = ["INSTALLED AND ACTIVATED", "INSTALLED BUT NOT YET ACTIVATED"]
    existing_optional_columns = [col for col in optional_columns if col in pivot_df.columns]
    other_columns = [col for col in pivot_df.columns if col not in base_columns + existing_optional_columns]
    columns_order = base_columns + existing_optional_columns + other_columns

    # Apply column order
    pivot_df = pivot_df[columns_order]

    # Compute average row
    avg_row = pd.DataFrame(pivot_df.drop(columns=["Hit Rate (%)", "RJO (%)"]).mean().to_frame().T)
    avg_row["Hit Rate (%)"] = ((avg_row["INSTALLED AND ACTIVATED"] / avg_row["Total"]) * 100).fillna(0).astype(int).map(
        '{}%'.format)
    avg_row["RJO (%)"] = (100 - avg_row["Hit Rate (%)"].str.rstrip('%').astype(float)).astype(int).map('{}%'.format)
    avg_row.index = ["Average"]

    pivot_df = pd.concat([pivot_df, avg_row])

    return pivot_df.reset_index()  # Reset index for better table display


# Interactive Table
table = pn.pane.DataFrame(update_table(), max_height=600, sizing_mode="stretch_width")


# Callback to update table when filters change
def refresh(event=None):
    table.object = update_table()


# Ensure filters trigger updates
start_date_picker.param.watch(refresh, 'value')
end_date_picker.param.watch(refresh, 'value')
jo_filter.param.watch(refresh, 'value')
municipality_filter.param.watch(refresh, 'value')

# Dashboard layout
dashboard = pn.Column(
    pn.Row(start_date_picker, end_date_picker, jo_filter, municipality_filter),
    table
)

# Run Panel App (Using threaded=True to avoid issues)
if __name__ == "__main__":
    pn.serve(dashboard, port=5075, show=True, threaded=True)