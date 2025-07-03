# ================================================= Import SLI Apsheet =================================================

import requests
import warnings
from datetime import datetime
import pytz
import dash
from dash import dcc, html, Input, Output, State, dash_table, ctx
import plotly.express as px
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

# ================================================= Generate SLI Open JOs Report =================================================


# Filter by 'OPEN' status
df_c3_open = df_c3[df_c3['OVERALL STATUS'] == 'OPEN'].copy()

# Custom order for AGEING (RANGE)
ageing_range_order = ['1-4 Hrs', '>4-12 Hrs', '>12-24 Hrs', '>24-48 Hrs',
                      '>2-3 Days', '>3-4 Days', '>4-5 Days', '>5-10 Days',
                      '>10-15 Days', '>15-20 Days', '>20 Days']

# ✅ Ensure AGEING (RANGE) column is properly handled
df_c3_open['AGEING (RANGE)'] = df_c3_open['AGEING (RANGE)'].fillna('N/A').astype(str)

# ✅ Columns to display in the table
columns_to_display = [
    'AGEING (RANGE)', 'MUNICIPALITY', 'JO TYPE', 'ACCOUNT NUMBER TIMESTAMP', 'TECH TEAM ASSIGNED TO TIMESTAMP',
    'AGEING (DAYS)', 'REFERENCE NUMBER', 'ACCOUNT NUMBER', 'TECH TEAM ASSIGNED TO TIMESTAMP',
    'DISPATCHER REMARKS', 'TECH TEAM ASSIGNED TO', 'TECH STATUS', 'RESCHEDULED DATE', 'TECHNICIAN REMARKS',
    'LAST TECH STATUS CHANGE DATE AND TIME', 'SUBSCRIBER NAME', 'BRGY'
]

# Get the unique municipalities and JO TYPES
municipalities = ['ALL'] + list(df_c3_open['MUNICIPALITY'].unique())
jo_types = ['ALL', 'Field Sales', 'CICT']

# Create the Dash app
app = dash.Dash(__name__)

# Define the layout
app.layout = html.Div([
    html.H1("Ageing Range Subscriber Count by Municipality"),

    # Municipality Dropdown
    dcc.Dropdown(
        id='municipality-dropdown',
        options=[{'label': m, 'value': m} for m in municipalities],
        value='ALL',
        clearable=False,
        style={'width': '50%', 'display': 'inline-block', 'margin-right': '10px'}
    ),

    # JO TYPE Dropdown
    dcc.Dropdown(
        id='jo-type-dropdown',
        options=[{'label': jo, 'value': jo} for jo in jo_types],
        value='ALL',
        clearable=False,
        style={'width': '40%', 'display': 'inline-block'}
    ),

    dcc.Graph(id='ageing-bar-chart'),

    # ✅ Download button
    html.Button(
        "Download CSV",
        id="download-button",
        style={'margin-top': '10px', 'background-color': '#4CAF50', 'color': 'white',
               'border': 'none', 'padding': '10px 20px', 'font-size': '16px', 'cursor': 'pointer'}
    ),

    dcc.Download(id="download-csv"),

    dash_table.DataTable(
        id='table-container',
        columns=[{'name': i, 'id': i} for i in columns_to_display],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'left', 'padding': '5px'},
        style_header={'backgroundColor': '#f4f4f4', 'fontWeight': 'bold'},
        page_size=10
    )
])


# ✅ Callback to update the bar chart and table
@app.callback(
    [Output('ageing-bar-chart', 'figure'),
     Output('table-container', 'data')],
    [Input('municipality-dropdown', 'value'),
     Input('jo-type-dropdown', 'value'),
     Input('ageing-bar-chart', 'clickData')]
)
def update_chart_and_table(selected_municipality, selected_jo_type, clickData):
    # Filter by selected municipality
    filtered_df_bc = df_c3_open.copy()
    if selected_municipality != 'ALL':
        filtered_df_bc = filtered_df_bc[filtered_df_bc['MUNICIPALITY'] == selected_municipality]

    # Filter by selected JO TYPE
    if selected_jo_type != 'ALL':
        filtered_df_bc = filtered_df_bc[filtered_df_bc['JO TYPE'] == selected_jo_type]

    # ✅ Ensure 'AGEING (RANGE)' column is not empty and properly formatted
    filtered_df_bc['AGEING (RANGE)'] = filtered_df_bc['AGEING (RANGE)'].fillna('N/A').astype(str)

    # Group by 'AGEING (RANGE)' and count subscribers
    ageing_count = filtered_df_bc.groupby('AGEING (RANGE)')['SUBSCRIBER NAME'].count().reset_index()
    ageing_count.columns = ['AGEING (RANGE)', 'Count of Subscribers']

    # ✅ Convert to categorical with the custom order and sort
    ageing_count['AGEING (RANGE)'] = pd.Categorical(ageing_count['AGEING (RANGE)'],
                                                    categories=ageing_range_order,
                                                    ordered=True)
    ageing_count = ageing_count.sort_values('AGEING (RANGE)')

    # Calculate total subscribers
    total_subscribers = ageing_count['Count of Subscribers'].sum()

    # ✅ Create a bar chart
    fig = px.bar(
        ageing_count,
        x='AGEING (RANGE)',
        y='Count of Subscribers',
        title=f"Count of Subscribers by Ageing (Range) - {selected_municipality} (Total: {total_subscribers})",
        labels={'AGEING (RANGE)': 'Ageing (Range)', 'Count of Subscribers': 'Count of Subscribers'},
        color='AGEING (RANGE)',
        text='Count of Subscribers'
    )

    fig.update_traces(texttemplate='%{text}', textposition='outside')
    fig.update_layout(
        xaxis_title='Ageing (Range)',
        yaxis_title='Count of Subscribers',
        xaxis=dict(type='category'),
        margin=dict(l=40, r=40, t=40, b=40)
    )

    # ✅ Handle click to filter table
    if clickData:
        selected_range = clickData['points'][0]['x']
        filtered_df_bc = filtered_df_bc[filtered_df_bc['AGEING (RANGE)'] == selected_range]

    # ✅ Select only the required columns for the table
    table_data = filtered_df_bc[columns_to_display].to_dict('records')

    return fig, table_data


# ✅ Callback to handle CSV download
@app.callback(
    Output("download-csv", "data"),
    Input("download-button", "n_clicks"),
    State('municipality-dropdown', 'value'),
    State('jo-type-dropdown', 'value'),
    prevent_initial_call=True
)
def download_csv(n_clicks, selected_municipality, selected_jo_type):
    filtered_df_bc = df_c3_open.copy()
    if selected_municipality != 'ALL':
        filtered_df_bc = filtered_df_bc[filtered_df_bc['MUNICIPALITY'] == selected_municipality]
    if selected_jo_type != 'ALL':
        filtered_df_bc = filtered_df_bc[filtered_df_bc['JO TYPE'] == selected_jo_type]

    # ✅ Fix: Ensure 'AGEING (RANGE)' is formatted properly
    filtered_df_bc['AGEING (RANGE)'] = filtered_df_bc['AGEING (RANGE)'].fillna('N/A').astype(str)

    # ✅ Strip column names of any hidden characters
    filtered_df_bc.columns = filtered_df_bc.columns.str.strip()

    # ✅ Export to CSV
    csv_string = filtered_df_bc.to_csv(index=False, encoding='utf-8')

    return dict(content=csv_string, filename="ageing_range_report.csv")


import webbrowser

# ✅ Run the app and open the browser automatically on port 8050
if __name__ == '__main__':
    webbrowser.open('http://127.0.0.1:8050')
    app.run(port=8050, use_reloader=False)  # Removed debug=True

