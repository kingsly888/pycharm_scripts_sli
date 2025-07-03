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

# ================================================= Add Columns =================================================

# Ensure the column is in datetime format
df_c3["LAST TECH STATUS CHANGE DATE AND TIME"] = pd.to_datetime(df_c3["LAST TECH STATUS CHANGE DATE AND TIME"])

# Extract the date part only
df_c3["LAST TECH STATUS CHANGE DATE"] = df_c3["LAST TECH STATUS CHANGE DATE AND TIME"].dt.date

# Extract Month and Year in "Mon YYYY" format
df_c3["LAST TECH STATUS MONTH YEAR"] = df_c3["LAST TECH STATUS CHANGE DATE AND TIME"].dt.strftime("%b %Y")

# ================================================= Filter Completed =================================================

df_c3_comp = df_c3[df_c3["TECH STATUS"] == "INSTALLED AND ACTIVATED"].copy()
print(f"df_c3_comp: {len(df_c3_comp):,}")

# ================================================= Handled JO Dash v1 =================================================

# import panel as pn
# import pandas as pd
# import matplotlib.pyplot as plt
# from datetime import datetime, timedelta
#
# # Enable Panel extension
# pn.extension()
#
# # Ensure the column is in datetime format
# df_c3["LAST TECH STATUS CHANGE DATE"] = pd.to_datetime(
#     df_c3["LAST TECH STATUS CHANGE DATE"], format="%m/%d/%Y"
# )
#
# df_c3_comp = df_c3[df_c3["TECH STATUS"] == "INSTALLED AND ACTIVATED"].copy()
#
# # Get the first and last date of the current month
# current_month_start = datetime.today().replace(day=1)
# next_month_start = (current_month_start + timedelta(days=32)).replace(day=1)
# current_month_end = next_month_start - timedelta(days=1)
#
# # Date picker widgets for selecting start and end dates
# start_date_picker = pn.widgets.DatePicker(
#     name="Start Date", value=current_month_start
# )
# end_date_picker = pn.widgets.DatePicker(
#     name="End Date", value=current_month_end
# )
#
# # Generate sorted list of unique JO TYPE options
# jo_type_options = sorted(df_c3["JO TYPE"].dropna().unique().tolist())
# jo_type_options.insert(0, "All Types")  # Add an "All Types" option
#
# # Generate sorted list of unique MUNICIPALITY options
# municipality_options = sorted(df_c3["MUNICIPALITY"].dropna().unique().tolist())
# municipality_options.insert(0, "All Municipalities")  # Add an "All Municipalities" option
#
# jo_type_dropdown = pn.widgets.Select(
#     name="JO TYPE", options=jo_type_options, value="All Types"
# )
# municipality_dropdown = pn.widgets.Select(
#     name="MUNICIPALITY", options=municipality_options, value="All Municipalities"
# )
#
#
# def plot_combined_chart(start, end, jo_type, municipality):
#     # Convert selected values to datetime for filtering
#     start_date = pd.to_datetime(start)
#     end_date = pd.to_datetime(end)
#
#     # Filter data based on the selected range for df_c3_comp (bar chart)
#     df_filtered_comp = df_c3_comp[
#         (df_c3_comp["LAST TECH STATUS CHANGE DATE"] >= start_date) &
#         (df_c3_comp["LAST TECH STATUS CHANGE DATE"] <= end_date)
#         ]
#
#     # Apply JO TYPE filter if not "All Types"
#     if jo_type != "All Types":
#         df_filtered_comp = df_filtered_comp[df_filtered_comp["JO TYPE"] == jo_type]
#
#     # Apply MUNICIPALITY filter if not "All Municipalities"
#     if municipality != "All Municipalities":
#         df_filtered_comp = df_filtered_comp[df_filtered_comp["MUNICIPALITY"] == municipality]
#
#     # Filter data based on the selected range for df_c3 (line chart)
#     df_filtered_all = df_c3[
#         (df_c3["LAST TECH STATUS CHANGE DATE"] >= start_date) &
#         (df_c3["LAST TECH STATUS CHANGE DATE"] <= end_date)
#         ]
#
#     # Apply JO TYPE filter if not "All Types"
#     if jo_type != "All Types":
#         df_filtered_all = df_filtered_all[df_filtered_all["JO TYPE"] == jo_type]
#
#     # Apply MUNICIPALITY filter if not "All Municipalities"
#     if municipality != "All Municipalities":
#         df_filtered_all = df_filtered_all[df_filtered_all["MUNICIPALITY"] == municipality]
#
#     if df_filtered_comp.empty and df_filtered_all.empty:
#         return "No data available for the selected filters."
#
#     # Count occurrences for bar chart
#     date_counts_comp = df_filtered_comp["LAST TECH STATUS CHANGE DATE"].value_counts().sort_index()
#     average_count_comp = round(date_counts_comp.mean()) if not date_counts_comp.empty else 0
#
#     # Count occurrences for line chart
#     date_counts_all = df_filtered_all["LAST TECH STATUS CHANGE DATE"].value_counts().sort_index()
#     average_count_all = round(date_counts_all.mean()) if not date_counts_all.empty else 0
#
#     # Construct dynamic title
#     title = f"C3 Completed JO ({jo_type} | {municipality}) Date Range (Avg Installed & Activated: {average_count_comp} | Avg All JO: {average_count_all})"
#
#     # Create the figure
#     fig, ax = plt.subplots(figsize=(12, 6))
#
#     # Plot bar chart
#     if not date_counts_comp.empty:
#         bars = ax.bar(date_counts_comp.index, date_counts_comp.values, color="lightblue", alpha=0.7,
#                       label="Installed & Activated")
#         for bar in bars:
#             ax.text(
#                 bar.get_x() + bar.get_width() / 2,
#                 bar.get_height(),
#                 f"{int(bar.get_height()):,}",
#                 ha="center", va="bottom", fontsize=10, fontweight="bold"
#             )
#
#     # Plot line chart
#     if not date_counts_all.empty:
#         ax.plot(date_counts_all.index, date_counts_all.values, marker="o", linestyle="-", color="royalblue",
#                 label="All JO")
#         for i, txt in enumerate(date_counts_all.values):
#             ax.text(date_counts_all.index[i], txt, f"{txt:,}", ha="center", va="bottom", fontsize=10, fontweight="bold",
#                     color="black")
#
#     # Customize the plot
#     ax.set_xlabel("Date")
#     ax.set_ylabel("Count")
#     ax.set_title(title)
#     ax.grid(axis="y", linestyle="--", alpha=0.7)
#     ax.set_xticks(date_counts_comp.index)
#     ax.set_xticklabels(date_counts_comp.index.strftime("%m/%d/%Y"), rotation=45, ha="right")
#     ax.legend()
#
#     return fig
#
#
# # Bind function to widgets and wrap with Matplotlib pane
# interactive_plot = pn.pane.Matplotlib(
#     pn.bind(plot_combined_chart, start_date_picker, end_date_picker, jo_type_dropdown, municipality_dropdown),
#     tight=True
# )
#
# # Layout using Panel
# dashboard = pn.Column(
#     pn.Row(start_date_picker, end_date_picker, jo_type_dropdown, municipality_dropdown),
#     interactive_plot
# )
#
# # Serve and open in browser
# dashboard.show()

# ================================================= Handled JO Dash v2 =================================================

import panel as pn
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# Enable Panel extension
pn.extension()

# Ensure the column is in datetime format
df_c3["LAST TECH STATUS CHANGE DATE"] = pd.to_datetime(
    df_c3["LAST TECH STATUS CHANGE DATE"], format="%m/%d/%Y"
)

df_c3_comp = df_c3[df_c3["TECH STATUS"] == "INSTALLED AND ACTIVATED"].copy()

# Get the first and last date of the current month
current_month_start = datetime.today().replace(day=1)
next_month_start = (current_month_start + timedelta(days=32)).replace(day=1)
current_month_end = next_month_start - timedelta(days=1)

# Date picker widgets for selecting start and end dates
start_date_picker = pn.widgets.DatePicker(
    name="Start Date", value=current_month_start
)
end_date_picker = pn.widgets.DatePicker(
    name="End Date", value=current_month_end
)

# Generate sorted list of unique JO TYPE options
jo_type_options = sorted(df_c3["JO TYPE"].dropna().unique().tolist())
jo_type_options.insert(0, "All Types")  # Add an "All Types" option

# Generate sorted list of unique MUNICIPALITY options
municipality_options = sorted(df_c3["MUNICIPALITY"].dropna().unique().tolist())
municipality_options.insert(0, "All Municipalities")  # Add an "All Municipalities" option

jo_type_dropdown = pn.widgets.Select(
    name="JO TYPE", options=jo_type_options, value="All Types"
)
municipality_dropdown = pn.widgets.Select(
    name="MUNICIPALITY", options=municipality_options, value="All Municipalities"
)

def plot_combined_chart(start, end, jo_type, municipality):
    # Convert selected values to datetime for filtering
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)

    # Filter data based on the selected range for df_c3_comp (bar chart)
    df_filtered_comp = df_c3_comp[
        (df_c3_comp["LAST TECH STATUS CHANGE DATE"] >= start_date) &
        (df_c3_comp["LAST TECH STATUS CHANGE DATE"] <= end_date)
    ]

    # Apply JO TYPE filter if not "All Types"
    if jo_type != "All Types":
        df_filtered_comp = df_filtered_comp[df_filtered_comp["JO TYPE"] == jo_type]

    # Apply MUNICIPALITY filter if not "All Municipalities"
    if municipality != "All Municipalities":
        df_filtered_comp = df_filtered_comp[df_filtered_comp["MUNICIPALITY"] == municipality]

    # Filter data based on the selected range for df_c3 (line chart)
    df_filtered_all = df_c3[
        (df_c3["LAST TECH STATUS CHANGE DATE"] >= start_date) &
        (df_c3["LAST TECH STATUS CHANGE DATE"] <= end_date)
    ]

    # Apply JO TYPE filter if not "All Types"
    if jo_type != "All Types":
        df_filtered_all = df_filtered_all[df_filtered_all["JO TYPE"] == jo_type]

    # Apply MUNICIPALITY filter if not "All Municipalities"
    if municipality != "All Municipalities":
        df_filtered_all = df_filtered_all[df_filtered_all["MUNICIPALITY"] == municipality]

    if df_filtered_comp.empty and df_filtered_all.empty:
        return "No data available for the selected filters."

    # Count occurrences for bar chart
    date_counts_comp = df_filtered_comp["LAST TECH STATUS CHANGE DATE"].value_counts().sort_index()
    average_count_comp = round(date_counts_comp.mean()) if not date_counts_comp.empty else 0
    total_count_comp = date_counts_comp.sum() if not date_counts_comp.empty else 0

    # Count occurrences for line chart
    date_counts_all = df_filtered_all["LAST TECH STATUS CHANGE DATE"].value_counts().sort_index()
    average_count_all = round(date_counts_all.mean()) if not date_counts_all.empty else 0
    total_count_all = date_counts_all.sum() if not date_counts_all.empty else 0

    # Construct dynamic title
    title = (
        f"C3 Completed JO ({jo_type} | {municipality})\n"
        f"Date Range: {start_date.strftime('%m/%d/%Y')} - {end_date.strftime('%m/%d/%Y')}\n"
        f"(Installed & Activated - Avg: {average_count_comp}, Total: {total_count_comp} | "
        f"All JO - Avg: {average_count_all}, Total: {total_count_all})"
    )

    # Create the figure
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot bar chart
    if not date_counts_comp.empty:
        bars = ax.bar(date_counts_comp.index, date_counts_comp.values, color="lightblue", alpha=0.7,
                      label="Installed & Activated")
        for bar in bars:
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{int(bar.get_height()):,}",
                ha="center", va="bottom", fontsize=10, fontweight="bold"
            )

    # Plot line chart
    if not date_counts_all.empty:
        ax.plot(date_counts_all.index, date_counts_all.values, marker="o", linestyle="-", color="royalblue",
                label="All JO")
        for i, txt in enumerate(date_counts_all.values):
            ax.text(date_counts_all.index[i], txt, f"{txt:,}", ha="center", va="bottom", fontsize=10, fontweight="bold",
                    color="black")

    # Customize the plot
    ax.set_xlabel("Date")
    ax.set_ylabel("Count")
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.grid(axis="y", linestyle="--", alpha=0.7)
    ax.set_xticks(date_counts_all.index if not date_counts_all.empty else date_counts_comp.index)
    ax.set_xticklabels(
        (date_counts_all.index if not date_counts_all.empty else date_counts_comp.index).strftime("%m/%d/%Y"),
        rotation=45, ha="right"
    )
    ax.legend()

    return fig

# Bind function to widgets and wrap with Matplotlib pane
interactive_plot = pn.pane.Matplotlib(
    pn.bind(plot_combined_chart, start_date_picker, end_date_picker, jo_type_dropdown, municipality_dropdown),
    tight=True
)

# Layout using Panel
dashboard = pn.Column(
    pn.Row(start_date_picker, end_date_picker, jo_type_dropdown, municipality_dropdown),
    interactive_plot
)

# Serve and open in browser
dashboard.show()









