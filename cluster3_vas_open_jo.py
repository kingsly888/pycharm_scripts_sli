import requests
import warnings
import numpy as np
import pandas as pd
import pytz
from datetime import datetime
import os
import io

import panel as pn
import seaborn as sns
import matplotlib
matplotlib.use('Agg')  # For EXE compatibility
import matplotlib.pyplot as plt

# ============ Panel Extension ============
pn.extension()

# ============ Password Section ============
correct_password = "cluster3@123!"
auth_status = pn.pane.Markdown("", width=300)
password_input = pn.widgets.PasswordInput(name="Enter Password", placeholder="••••••••••", width=300)

main_view = pn.Column()

def validate_password(event):
    if password_input.value == correct_password:
        auth_status.object = "✅ Access granted. Loading dashboard..."
        main_view[:] = [dashboard]
    else:
        auth_status.object = "❌ Incorrect password. Try again."

submit_button = pn.widgets.Button(name="Submit", button_type="primary", width=100)
submit_button.on_click(validate_password)

login_ui = pn.Column(
    pn.pane.Markdown("## 🔐 VAS Dashboard Login"),
    password_input,
    submit_button,
    auth_status
)

main_view[:] = [login_ui]

# ============ Data Import Section ============
warnings.filterwarnings("ignore")

# AppSheet API credentials
app_id = '86cf92e7-c5be-435e-8366-25989bb00035'
application_access_key = 'V2-6rM8g-QR4Pi-xO6ea-8lwbN-3pcFF-DVPXM-3T5MS-Tijsl'

def fetch_table_data(table_name):
    url = f"https://api.appsheet.com/api/v2/apps/{app_id}/tables/{table_name}/query"
    headers = {'ApplicationAccessKey': application_access_key, 'Content-Type': 'application/json'}
    payload = {"Action": "Find", "Properties": {"Locale": "en-US", "Timezone": "UTC"}, "Rows": []}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        try:
            data = response.json()
            return pd.DataFrame(data) if isinstance(data, list) else pd.DataFrame()
        except ValueError:
            print(f"❌ JSON decode error for table: {table_name}")
            return pd.DataFrame()
    else:
        print(f"❌ Failed to fetch data from AppSheet: {response.status_code} - {response.text}")
        return pd.DataFrame()

df_vas = fetch_table_data("CICT VAS SDU")
df_vas["TECH STATUS"] = df_vas["TECH STATUS"].replace("", "FOR VISIT")

# Define OVERALL STATUS
conditions = [
    df_vas["TECH STATUS"].isin(["FOR VISIT", "RE-SCHEDULE"]),
    df_vas["TECH STATUS"].isin(["DELIVERED / INSTALLED / ACTIVATED", "FOR ACTIVATION", "DELIVERED ONLY"]),
    df_vas["TECH STATUS"].isin([
        "FOR CANCELLATION / CUSTOMER INITIATED", "HOUSE CLOSED", "VISITED CANNOT LOCATE", "VISITED CLIENT UNCONTACTED",
        "VISITED UNKNOWN PERSON", "WAIT FOR CLIENT CALL", "WRONG PLAN / PACKAGE", "WRONG ADDRESS", "MISROUTED SDU OR MDU"
    ])
]
values = ["Open", "Closed", "Cancelled"]
df_vas["OVERALL STATUS"] = np.select(conditions, values, default="Unknown")

# Convert timestamps
df_vas["UPLOADED DATE AND TIME"] = pd.to_datetime(df_vas["UPLOADED DATE AND TIME"], errors='coerce')
df_vas["LAST UPDATED DATE AND TIME"] = pd.to_datetime(df_vas["LAST UPDATED DATE AND TIME"], errors='coerce')

# Philippine timezone
ph_tz = pytz.timezone("Asia/Manila")
current_time_ph = datetime.now(ph_tz)

def calculate_ageing(row):
    if pd.notna(row["UPLOADED DATE AND TIME"]):
        uploaded_dt = row["UPLOADED DATE AND TIME"].tz_localize(ph_tz) if row["UPLOADED DATE AND TIME"].tzinfo is None else row["UPLOADED DATE AND TIME"]
        if row["OVERALL STATUS"] == "Open":
            return (current_time_ph - uploaded_dt).total_seconds() / 3600
        elif row["OVERALL STATUS"] in ["Closed", "Cancelled"] and pd.notna(row["LAST UPDATED DATE AND TIME"]):
            updated_dt = row["LAST UPDATED DATE AND TIME"].tz_localize(ph_tz) if row["LAST UPDATED DATE AND TIME"].tzinfo is None else row["LAST UPDATED DATE AND TIME"]
            return (updated_dt - uploaded_dt).total_seconds() / 3600
    return None

df_vas["AGEING (HOURS)"] = df_vas.apply(calculate_ageing, axis=1)
df_vas["AGEING (DAYS)"] = df_vas["AGEING (HOURS)"] / 24

def determine_ageing_range(ageing_hrs):
    if pd.isna(ageing_hrs):
        return ''
    ranges = [(4, '1-4 Hrs'), (12, '>4-12 Hrs'), (24, '>12-24 Hrs'), (48, '>24-48 Hrs'), (72, '>2-3 Days'),
              (96, '>3-4 Days'), (120, '>4-5 Days'), (240, '>5-10 Days'), (360, '>10-15 Days'), (480, '>15-20 Days')]
    for limit, label in ranges:
        if ageing_hrs <= limit:
            return label
    return '>20 Days'

df_vas['AGEING (RANGE)'] = df_vas['AGEING (HOURS)'].apply(determine_ageing_range)

# Filter Cluster 5 and only Open
df_vas_c3 = df_vas[df_vas["CLUSTER"] == "CLUSTER 3"].copy()
df_vas_c3_Open = df_vas_c3[df_vas_c3["OVERALL STATUS"] == "Open"].copy()

# ============ Filters ============
classification_filter = pn.widgets.Select(
    name='Classification',
    options=['All', 'AFTERSALES VAS PRODUCTS', 'NEW SALES VAS PRODUCTS'],
    value='All'
)
vas_deployment_filter = pn.widgets.Select(
    name='VAS Deployment',
    options=['All', 'IPTV', 'WIFI 6', 'GAME CHANGER'],
    value='All'
)
municipality_options = ['All'] + sorted(df_vas_c3_Open["MUNICIPALITY CONVERTED"].dropna().unique())
municipality_filter = pn.widgets.Select(
    name='Municipality',
    options=municipality_options,
    value='All'
)

# ============ Download Button ============
def generate_csv():
    buffer = io.StringIO()
    df_vas_c3_Open.to_csv(buffer, index=False, encoding="utf-8-sig")
    buffer.seek(0)
    return buffer

download_button = pn.widgets.FileDownload(
    callback=generate_csv,
    filename="Open_VAS.csv",
    label="⬇️ Download Open VAS CSV",
    button_type="success"
)

# ============ Plot ============
@pn.depends(classification_filter, vas_deployment_filter, municipality_filter)
def plot_ageing(classification, vas_deployment, municipality):
    df_filtered = df_vas_c3_Open.copy()
    if classification != 'All':
        df_filtered = df_filtered[df_filtered["CLASSIFICATION"] == classification]
    if vas_deployment != 'All':
        df_filtered = df_filtered[df_filtered["VAS DEPLOYMENT"] == vas_deployment]
    if municipality != 'All':
        df_filtered = df_filtered[df_filtered["MUNICIPALITY CONVERTED"] == municipality]

    ageing_order = ['1-4 Hrs', '>4-12 Hrs', '>12-24 Hrs', '>24-48 Hrs',
                    '>2-3 Days', '>3-4 Days', '>4-5 Days', '>5-10 Days',
                    '>10-15 Days', '>15-20 Days', '>20 Days']

    ageing_counts = df_filtered["AGEING (RANGE)"].value_counts().reindex(ageing_order, fill_value=0)
    total_count = ageing_counts.sum()

    sns.set_theme(style="whitegrid")
    plt.close("all")
    fig, ax = plt.subplots(figsize=(12, 4))
    bars = sns.barplot(x=ageing_counts.index, y=ageing_counts.values,
                       palette=sns.color_palette("viridis", len(ageing_counts)), ax=ax)
    ax.set_xlabel("\nAgeing Range", fontsize=12)
    ax.set_ylabel("Count\n", fontsize=12)
    ax.set_title(f"VAS Ageing Distribution (Total: {total_count})", fontsize=14)
    for bar, count in zip(bars.patches, ageing_counts.values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(count),
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    plt.tight_layout()
    return fig

# ============ Dashboard ============
dashboard = pn.Column(
    pn.pane.Markdown("## 📊 Cluster 3 VAS Ageing Distribution Dashboard (Powered by Kingsly)"),
    classification_filter,
    vas_deployment_filter,
    municipality_filter,
    download_button,
    plot_ageing
)

# ============ Serve App ============
if __name__ == "__main__":
    pn.serve(main_view, show=True)
