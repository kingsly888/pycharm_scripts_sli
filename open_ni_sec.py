import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import panel as pn
import socket
import os
from datetime import datetime

pn.extension("matplotlib", notifications=True)

# ===================== Load Data =====================
import pandas as pd
import requests
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

# AppSheet API credentials
app_id = '86cf92e7-c5be-435e-8366-25989bb00035'
application_access_key = 'V2-6rM8g-QR4Pi-xO6ea-8lwbN-3pcFF-DVPXM-3T5MS-Tijsl'

# Tables to fetch (with JO TYPE tags)
fs_tables = [
    "S2D",
    "S2D ARCHIVE1",
    "S2D APPSHEET ARCHIVE 1st HALF 2025",
    "S2D ARCHIVE 2024",
    "S2D APPSHEET ARCHIVE 2nd HALF 2025"
]

cict_tables = ["CICT"]

all_tables = fs_tables + cict_tables

# ✅ Function to query AppSheet table and return DataFrame with JO TYPE
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
                df = pd.DataFrame(data)
                # Assign JO TYPE based on source
                df["JO TYPE"] = "FS" if table_name in fs_tables else "CICT"
                print(f"{table_name}: {len(df):,} rows imported")
                return df
            else:
                print(f"{table_name}: Unexpected data format - {type(data)}")
                return pd.DataFrame()
        except ValueError:
            print(f"{table_name}: Failed to parse JSON")
            print(response.text)
            return pd.DataFrame()
    else:
        print(f"{table_name}: Failed to retrieve data - Status code {response.status_code}")
        print(response.text)
        return pd.DataFrame()

# ✅ Fetch all tables and combine
df_list = [fetch_table_data(tbl) for tbl in all_tables]
df = pd.concat(df_list, ignore_index=True)

# ✅ Final Output Summary
print(f"\n✅ Combined {len(all_tables)} tables into df with {df.shape[0]:,} rows and {df.shape[1]} columns.")

# ========================================================== Clean City ==========================================================

# List of keywords related to different cities
caloocan_keywords = ['CALOOCAN CITY', 'CITY OF CALOOCAN', 'CALOOCAN', 'CITY OF CALOOCN', 'CITY OF CALOOCN',
                     'caloocan city ', 'CITY OF CALOOCA', 'BAGUMBONG ', 'caloocan city ', 'Caloocan ',
                     'City Of Caloocan', 'Caloocan', 'City of Caloocan'

                     ]

las_pinas_keywords = [
    'CITY OF LAS PIÑAS', 'LAS PIÑAS', 'LAS PI-AS', 'LAS PIÑAS', 'LAS PINAS', 'METRO MANILA LAS PINAS',
    ' CITY OF LAS PIÃ?AS, ', ' LAS PIÃ?AS', ' LAS PIÃ?AS ', ',CITY OF LAS PIÃ?AS', 'CITY OF LAS PIÃ?AS',
    'LAS PIÃ?AS', 'LAS PIÃ?AS ', 'LAS PIÑAS', 'LAS PIÑAS', 'LAS PIÑAS', ' LAS PI?AS', 'LAS PI AS ', 'LAS PI?AS'
                                                                                                    'LAS PI?AS',
    'Las PiÃ±as', 'LASPINAS', 'LAS PI?AS', 'CITY OF LAS PISNAS ', 'las pinas', 'CITY OF LAS PIÃs', 'City Of Las Piñas',
    'Las PiÃ±as', "LAS PIï¿½AS", "LAS PIï¿½AS CITY", "PARAï¿½AQUE", "LAS PIï¿½AS CITY"

]

makati_keywords = ['MAKATI', 'CITY OF MAKATI', 'MAKAT', 'Makati city', 'COL. SANTOS ST. 7026 BSOUTH CEMBO',
                   'City Of Makati', 'City Of Makati 09562842956 0916511806', 'Makati']

malabon_keywords = ['MALABON', 'CITY OF MALABON', 'CITY OF MALABAON', 'MALABON CENTRAL', 'MALABON CITY',
                    'City Of Malabon', 'Malabon']

mandaluyong_keywords = ['CITY OF MANDALUYONG', 'MANDALUYONG', 'MAUWAY,CITY OF MANDALUYONG', 'Mandaluyong City',
                        'City Of Mandaluyong']

manila_keywords = [
    'MALATE', 'MALATE, CITY OF', 'PANDACAN', 'QUIAPO', 'SAMPALOC EAST', 'SAMPALOC EAST ',
    'SAN MIGUEL', 'SANTA ANA', 'SANTA CRUZ NORTH', 'TONDO', 'TONDO NORTH', 'TONDO NORTH ',
    ',TONDO I/II,', 'A TONDO NORTH', 'BARANGAY 100', 'BARANGAY 586', 'BINONDO', 'BINONDO ',
    'CITY OF CALOOCAN', 'GAGALANGIN, TONDO', 'INTRAMUROS', 'MALATE', 'MALATE ', 'NAGTAHAN',
    'PACO', 'PACO ', 'PANDACAN', 'PANDACAN   ', 'QUIAPO', 'QUIAPO ', 'SAMPALOC', 'SAMPALOC CITY',
    'SAMPALOC EAST', 'SAMPALOC EAST ', 'SAN MIGUEL', 'SAN NICOLAS', 'SAN PASCUAL', 'SANTA ANA',
    'SANTA ANA ', 'SANTA ANA CITY', 'SANTA CRUZ ', 'SANTA CRUZ NORTH', 'STA ANA', 'TONDO',
    'TONDO I/II,', 'TONDO NORTH', 'TONDO NORTH ', 'URMITA ',
    'CITY OF MANILA', 'CITY OF MANILA', 'LAS PINAS,METRO MANILA', 'MANILA', 'MANILA ',
    'MANILA CITY', 'TONDO NORTH MANILA', 'METRO MANILA QUEZON CITY', 'CITY OF MANDALUYONG',
    'MANILA CITY', 'CITY OF MANILA', 'CITY OF MANILA', 'CITY OF MANILA', 'CITY OF MANILATONDO I/II',
    'ERMITA, CITY OF MANILA', 'INTRAMUROS, CITY OF MANILA', 'MALATE, CITY OF MANILA',
    'MALATE, MANILA', 'MANILA', 'MANILA ', 'MANILA', 'MANILA BARANGAY', 'MANILA CITY',
    'MANILA CITY', 'MANILA CITY TONDO', 'MANILA CITY TONDO NORTH', 'MANILA CITY',
    'MANILA CPO - ERMITA', 'MANILA SAN ANDRES BUKID', 'MANILA SANTA ANA', 'MANILA',
    'METRO MANILA MANILA', 'METRO MANILA MANILA BARANGAY 232 Q. KATAMANAN SAN LORENZO ST 1969',
    'METRO MANILA SAMPALOC EAST BARANGAY 412 ZONE 6 LEGARDA ST. 2537 1008',
    'METRO MANILA SANTA ANA Barangay 775 ONYX STREET 2525 P2(PH4 B4)', 'PACO, CITY OF MANILA',
    'PANDACAN, CITY OF MANILA', 'PORT AREA, CITY OF MANILA', 'QUIAPO, CITY OF MANILA',
    'SAMPALOC EAST / MANILA CITY', 'SAMPALOC EAST MANILA', 'SAMPALOC MANILA CITY',
    'SAMPALOC, CITY OF MANILA', 'SAN MIGUEL, CITY OF MANILA', 'SAN NICOLAS, CITY OF MANILA',
    'SAN NICOLAS, MANILA', 'SANTA ANA, CITY OF MANILA', 'SANTA CRUZ, CITY OF MANILA',
    'SANTA MESA MANILA', 'SANTA MESA, CITY OF MANILA', 'STA. MESA,MANILA',
    'TONDO  MANILA CITY', 'TONDO ,MANIL', 'TONDO I/II, CITY OF MANILA', 'TONDO MANILA',
    'TONDO NORTH MANILA', 'TONDO, MANILA', 'SANTA CRUZ', 'STA. MESA ', ' QUIRICADA ST', 'SAN ANDRES', 'STA MESA',
    'SANTA MESA, ', 'ERMITA', 'CITY OF MINILA', 'STA. MESASTA', '--SANTA MESA  ', 'SANTA MESA', 'BACOOD ',
    'City of manila', 'Ermita, City Of Manila', 'Intramuros, City Of Manila', 'Malate, City Of Manila',
    'Paco, City Of Manila', 'Pandacan, City Of Manila', 'PORT AREA, CITY OF MANILA', 'Quiapo, City Of Manila',
    'Sampaloc, City Of Manila', 'San Miguel, City Of Manila', 'SAN NICOLAS, CITY OF MANILA',
    'Santa Ana, City Of Manila', 'Santa Cruz, City Of Manila', 'Santa Mesa, City Of Manila',
    'Tondo I/II, City Of Manila', 'Binondo, City Of Manila', 'City of Manila', 'Manila', 'Port Area, City Of Manila',
    'SAN NICOLAS, CITY OF MANILA'

]

pasig_keywords = [
    'CITY OF PASIG', 'PASIG', 'CITY OF PASIG ', 'CITY OF PASIG,', 'PASIG ', 'PASIG CITY', 'PASIG CITY ', 'PASIG CITY,',
    'PASIC CITY', 'PASIC', 'SIG CITY', 'City of Pasig', 'pasig city', 'Pasig City', 'Pasig'
]

pateros_keywords = ['PATEROS', 'City Of Pateros', 'Pateros']

quezon_city_keywords = [
    ',QUEZON CITY', 'QUEZON CITY', 'QUEZON CITY ', ' METRO MANILA QUEZON CITY', ' QUEZON CITY ',
    ', QUEZON CITY', ',QUEZON ', ',QUEZON CITY,', 'CITY OF QUEZON', 'METRO MANILA QUEZON CITY',
    'METRO MANILA QUEZON CITY ', 'NAQUEZON CITY', 'NOVALICHES QUEZON CITY', 'QUEZITY CITY', 'QUEZON', 'QUEZON ',
    'QUEZON CI', 'QUEZON CITY', 'QUEZON CITY ', 'QUEZON CITY LIBIS', 'QUEZON CITY PASONG TAMO PINGKIAN',
    'QUEZON CITY,', 'QUEZON CITY, ', 'SECOND DISTRICT QUEZON CITY', ' TANDANG SORA', 'NOVALICHES PROPER',
    ' MATANDANG BALARA', 'PAYATAS', 'Quezon City'
]

san_juan_keywords = [
    'CITY OF SAN JUAN', 'RIZAL TAYTAY SAN JUAN SITIO BATONG DALIG 1', 'SAN JUAN', 'SAN JUAN ', 'SAN JUAN CITY',
    'SAN JUA', 'City Of San Juan'
]

taguig_keywords = [
    'TAGUIG', 'TAGUIG ', 'CITY OF TAGUIG', '19 A, AGUILA STREET ISG, CITY OF TAGUIG, METRO MANILA, PHILIPPINES',
    '9, ROMANTIC IBAYO, SANTA ANA, CITY OF TAGUIG, METRO MANILA, PHILIPPINES', 'TAGUIG CITY', 'TAGAUIG',
    'City Of Taguig', 'Taguig City', 'Taguig', 'Taguig ', ' Taguig', 'taguig'
]

valenzuela_keywords = [
    'CITY OF VALENZUELA', 'VALENZUELA', 'VALENZUELA ', ',CITY OF VALENZUELA,', 'CITY OF VALENZUELA',
    'CITY OF VALENZUELA ', 'VALENZUELA CITY', 'VALENZUELA ', 'VALANZUELA CITY', 'VALENZUELA', 'VALENZUELA',
    'valenzuela',
    'Valenzuela City', 'ALENZUELA', 'City Of Valenzuela']

paranaque_keywords = [
    'PARANAQUE CITY',
    'CITY OF PARAÑAQUE',
    'CITY OF PARAÑAQUE,',
    'CITY OF PARAÑAQUE ',
    'CITY OF PARANAQUE',
    'CITY OF PARAÃ?AQUE',
    'CITY OF PARAÃ?AQUE,',
    'CITY OF PARAÑAQUE',
    'CITY OF PARAÑAQUE, ',
    'PARANAQUE',
    'PARAÑAQUE',
    'PARAÑAQUE ',
    'PARANAQUE ',
    'PARANAQUE CITY',
    'PARAÑAQUE CITY',
    'PUROK UNO SAN ISIDRO CITY OF PARAÑAQUE', 'PARAÃ?AQUE', 'PARANAQUE CITY', 'PARAÃ?AQUE', 'PARA?AQUE', 'PARANIAQUE',
    'City Of Parañaque', 'city of paranaque', 'CITY OF PARA�AQUE'
]

marikina_keywords = ['MARIKINA', 'CITY OF MARIKINA', 'CITYOF MARIKINA', 'MARIKINA', 'MARIKINA ', 'MARIKINA  CITY',
                     'MARIKINA CITY',
                     'METRO MANILA MARIKINA TUMANA OKRA BLK 18-21', 'City Of Marikina', 'City of Marikina'
                     ]

muntinlupa_keywords = ['MUNTINLUPA', 'CITY OF MUNTINLUPA', 'MUNTINLUPA', 'MUNTINLUPA', 'MUNTINLUPA, METRO MANILA, ',
                       ',ALABANG,CITY', 'MUNTILUPA', 'City Of Muntinlupa', 'Muntinlupa', 'Muntinlupa City']

navotas_keywords = ['NAVOTAS', 'CITY OF NAVOTAS', 'CITY OF NAVOTAS', 'NAVOTAS', 'NAVOTAS', 'NAVOTAS CITY',
                    'City Of Navotas']

pasay_keywords = ['PASAY', 'PASAY CITY', 'PASAY CITY', 'PASAY CITY', 'Pasay City']


# Function to check if CITY column contains specified keywords
def check_city(city):
    if isinstance(city, str):
        for keyword in caloocan_keywords:
            if keyword in city:
                return 'CALOOCAN'
        for keyword in las_pinas_keywords:
            if keyword in city:
                return 'LAS PIÑAS'
        for keyword in makati_keywords:
            if keyword in city:
                return 'MAKATI'
        for keyword in malabon_keywords:
            if keyword in city:
                return 'MALABON'
        for keyword in mandaluyong_keywords:
            if keyword in city:
                return 'MANDALUYONG'
        for keyword in manila_keywords:
            if keyword in city:
                return 'MANILA'
        for keyword in marikina_keywords:
            if keyword in city:
                return 'MARIKINA'
        for keyword in muntinlupa_keywords:
            if keyword in city:
                return 'MUNTINLUPA'
        for keyword in navotas_keywords:
            if keyword in city:
                return 'NAVOTAS'
        for keyword in paranaque_keywords:
            if keyword in city:
                return 'PARAÑAQUE'
        for keyword in pasay_keywords:
            if keyword in city:
                return 'PASAY'
        for keyword in pasig_keywords:
            if keyword in city:
                return 'PASIG'
        for keyword in pateros_keywords:
            if keyword in city:
                return 'PATEROS'
        for keyword in quezon_city_keywords:
            if keyword in city:
                return 'QUEZON CITY'
        for keyword in san_juan_keywords:
            if keyword in city:
                return 'SAN JUAN'
        for keyword in taguig_keywords:
            if keyword in city:
                return 'TAGUIG'
        for keyword in valenzuela_keywords:
            if keyword in city:
                return 'VALENZUELA'

    return 'OTHERS'


# Apply the function to create the FINAL CITY column
df['CITY'] = df['MUNICIPALITY'].apply(check_city)

# ========================================= Create Filter =========================================

df.loc[df["JO TYPE"] == "CICT", "OMD CLASSIFICATION"] = "SDU"
df = df[df["OMD CLASSIFICATION"] != "MDU"]

df = df[~(df["ACCOUNT NUMBER"].isna() | (df["ACCOUNT NUMBER"].astype(str).str.strip() == ""))]

df = df[df["CLUSTER"] == "CLUSTER 3"]

df = df[~df["SLI"].isin(["MYRIAD - MDU SOUTH", "MYRIAD - MDU NORTH"])]

# ========================================= Assign FOR VISIT value for blank TECH STATUS =========================================

df["TECH STATUS"] = df["TECH STATUS"].apply(lambda x: "FOR VISIT" if pd.isna(x) or str(x).strip() == "" else x)

# ========================================= Assign Values to Account Number TimeStamp Blank Values =========================================

import pandas as pd

# Ensure timestamp columns are treated as datetime (optional but recommended)
df["ACCOUNT NUMBER TIMESTAMP"] = pd.to_datetime(df["ACCOUNT NUMBER TIMESTAMP"], errors='coerce')
df["TECH TEAM ASSIGNED TO TIMESTAMP"] = pd.to_datetime(df["TECH TEAM ASSIGNED TO TIMESTAMP"], errors='coerce')
df["SUBMITTED IN S2D DATE AND TIME"] = pd.to_datetime(df["SUBMITTED IN S2D DATE AND TIME"], errors='coerce')

# Fill missing ACCOUNT NUMBER TIMESTAMP values
df["ACCOUNT NUMBER TIMESTAMP"] = df["ACCOUNT NUMBER TIMESTAMP"].fillna(
    df["TECH TEAM ASSIGNED TO TIMESTAMP"]
).fillna(
    df["SUBMITTED IN S2D DATE AND TIME"]
)

# ========================================= Assign Current DateTime =========================================

from datetime import datetime
import pytz

# Define timezone for the Philippines
ph_tz = pytz.timezone('Asia/Manila')

# Get current datetime in Philippines
current_dt = datetime.now(ph_tz).strftime("%m/%d/%Y %H:%M:%S")

# Assign to new column
df["CURRENT DATETIME"] = current_dt


# ==================================================== Add Ageing ====================================================

import pandas as pd

# Ensure datetime columns are in datetime format
df["ACCOUNT NUMBER TIMESTAMP"] = pd.to_datetime(df["ACCOUNT NUMBER TIMESTAMP"], errors='coerce')
df["CURRENT DATETIME"] = pd.to_datetime(df["CURRENT DATETIME"], errors='coerce')
df["LAST TECH STATUS CHANGE DATE AND TIME"] = pd.to_datetime(df["LAST TECH STATUS CHANGE DATE AND TIME"], errors='coerce')

# Define a function to calculate ageing in hours
def calculate_ageing(row):
    if row["TECH STATUS"] == "FOR VISIT":
        if pd.notnull(row["ACCOUNT NUMBER TIMESTAMP"]) and pd.notnull(row["CURRENT DATETIME"]):
            return (row["CURRENT DATETIME"] - row["ACCOUNT NUMBER TIMESTAMP"]).total_seconds() / 3600
    else:
        if pd.notnull(row["ACCOUNT NUMBER TIMESTAMP"]) and pd.notnull(row["LAST TECH STATUS CHANGE DATE AND TIME"]):
            return (row["LAST TECH STATUS CHANGE DATE AND TIME"] - row["ACCOUNT NUMBER TIMESTAMP"]).total_seconds() / 3600
    return None  # If required fields are missing

# Apply the function to the DataFrame
df["AGEING (HRS)"] = df.apply(calculate_ageing, axis=1)

# Convert AGEING (HRS) to AGEING (DAYS)
df["AGEING (DAYS)"] = df["AGEING (HRS)"] / 24


import numpy as np

# Define conditions
conditions = [
    (df["AGEING (HRS)"] <= 4),
    (df["AGEING (HRS)"] > 4) & (df["AGEING (HRS)"] <= 8),
    (df["AGEING (HRS)"] > 8) & (df["AGEING (HRS)"] <= 12),
    (df["AGEING (HRS)"] > 12) & (df["AGEING (HRS)"] <= 24),
    (df["AGEING (HRS)"] > 24) & (df["AGEING (HRS)"] <= 48),
    (df["AGEING (HRS)"] > 48) & (df["AGEING (HRS)"] <= 72),
    (df["AGEING (HRS)"] > 72) & (df["AGEING (HRS)"] <= 96),
    (df["AGEING (HRS)"] > 96) & (df["AGEING (HRS)"] <= 120),
    (df["AGEING (HRS)"] > 120) & (df["AGEING (HRS)"] <= 168),
    (df["AGEING (HRS)"] > 168)
]

# Define corresponding choices
choices = [
    "1-4Hrs",
    ">4-8Hrs",
    ">8-12Hrs",
    ">12Hrs-1Day",
    ">1Day-2Days",
    ">2Days-3Days",
    ">3Days-4Days",
    ">4Days-5Days",
    ">5Days-7Days",
    ">7Days"
]

# Assign new column
df["AGEING (RANGE)"] = np.select(conditions, choices, default="Invalid")

# ========================================== Assign Values ==========================================

import numpy as np

conditions = [
    df["TECH STATUS"] == "FOR VISIT",
    df["TECH STATUS"] == "RE-SCHEDULE"
]

choices = ["FOR VISIT", "RE-SCHEDULED"]

df["JO STATUS"] = np.select(conditions, choices, default="RJO")

df["TAG"] = df["TAG"].replace("", pd.NA)  # Treat empty strings as NA
df["TAG"] = df["TAG"].fillna("MYRIAD-SDU")

# ========================================== Add Logic to Sectorize ==========================================
# ========================================== Combine address ==========================================
import pandas as pd
import numpy as np

# Define a mask for rows where ADDRESS is empty or NaN (regardless of MUNICIPALITY)
address_mask = df["ADDRESS"].isna() | (df["ADDRESS"].str.strip() == "")

# Combine the columns, skipping any that are NaN or empty strings
def combine_address(row):
    parts = [
        str(row.get('HOUSE NO AND STREET NAME', '')).strip(),
        str(row.get('BUILDING SUBDIVISION', '')).strip(),
        str(row.get('BRGY', '')).strip(),
        str(row.get('MUNICIPALITY', '')).strip()
    ]
    return ', '.join([p for p in parts if p])

# Apply the combination only to the rows meeting the condition
df.loc[address_mask, "ADDRESS"] = df.loc[address_mask].apply(combine_address, axis=1)

# ========================================== Tag Manila Partition ==========================================

import pandas as pd
import numpy as np
import re

# Define Manila 1 and Manila 2 area keywords
manila1_keywords = [
    r"tondo", r"santa\s*cruz", r"binondo", r"san\s*nicolas", r"quiapo", r"sampaloc"
]
manila2_keywords = [
    r"baseco", r"intramuros", r"santa\s*mesa", r"san\s*miguel", r"san\s*andres\s*bukid",
    r"santa\s*ana", r"pandacan", r"paco", r"malate", r"ermita"
]

# Compile combined regex patterns
manila1_pattern = re.compile(r"|".join(manila1_keywords), flags=re.IGNORECASE)
manila2_pattern = re.compile(r"|".join(manila2_keywords), flags=re.IGNORECASE)

# Brgy number patterns
brgy_pattern = re.compile(r"(brgy|bgy|barangay)?\s*(\d{1,3})", flags=re.IGNORECASE)


# Function to determine MANILA PARTITION
def assign_manila_partition(row):
    if row["MUNICIPALITY"].strip().upper() != "MANILA CITY":
        return np.nan

    address = str(row["ADDRESS"]).lower().replace("\n", " ").replace("\r", " ").strip()

    # Check for area keywords
    if manila1_pattern.search(address):
        return "MANILA 1"
    elif manila2_pattern.search(address):
        return "MANILA 2"

    # Check for BRGY numbers
    brgy_matches = brgy_pattern.findall(address)
    for _, num_str in brgy_matches:
        try:
            num = int(num_str)
            if 1 <= num <= 594:
                return "MANILA 1"
            elif 595 <= num <= 897:
                return "MANILA 2"
        except ValueError:
            continue

    return np.nan  # If no match found


# Apply the function
df["MANILA PARTITION"] = df.apply(assign_manila_partition, axis=1)

# ========================================== add sectors ==========================================

import pandas as pd
import numpy as np

# Define function to assign sector
def assign_sector(row):
    mun = str(row["MUNICIPALITY"]).strip().upper()
    partition = str(row.get("MANILA PARTITION", "")).strip().upper()

    if mun == "MANDALUYONG CITY":
        return "Sector 3"
    elif mun == "MAKATI CITY":
        return "Sector 4"
    elif mun == "SAN JUAN CITY":
        return "Sector 2"
    elif mun == "MANILA CITY":
        if partition == "MANILA 1":
            return "Sector 1"
        elif partition == "MANILA 2":
            return "Sector 2"
    return np.nan  # Default if no condition is met

# Apply the function
df["SECTOR"] = df.apply(assign_sector, axis=1)

# ===================== AGEING Order =====================
ageing_order = [
    "1-4Hrs", ">4-8Hrs", ">8-12Hrs", ">12Hrs-1Day",
    ">1Day-2Days", ">2Days-3Days", ">3Days-4Days",
    ">4Days-5Days", ">5Days-7Days", ">7Days"
]

# ===================== Login Panel =====================
correct_password = "Cluster3@Power!"
auth_status = pn.pane.Markdown("", width=300)
password_input = pn.widgets.PasswordInput(name="Enter Password", placeholder="••••••••••", width=300)
submit_button = pn.widgets.Button(name="Submit", button_type="primary", width=100)

def check_password(event):
    if password_input.value == correct_password:
        auth_status.object = "✅ Login successful!"
        login_panel.visible = False
        main_view.visible = True
    else:
        auth_status.object = "❌ Incorrect password. Please try again."

submit_button.on_click(check_password)

login_panel = pn.Column(
    "## 🔐 Cluster 3 Access Login",
    password_input,
    submit_button,
    auth_status,
    width=350,
    margin=20,
)

# ===================== Filters =====================
jo_types = ["All"] + sorted(df["JO TYPE"].dropna().unique().tolist())
sectors = ["All"] + sorted(df["SECTOR"].dropna().unique().tolist())
cities = ["All"] + sorted(df["CITY"].dropna().unique().tolist())
tags = ["All"] + sorted(df["TAG"].dropna().unique().tolist())
jo_statuses = ["All"] + sorted(df["JO STATUS"].dropna().unique().tolist())
tech_statuses = ["All"] + sorted(df["TECH STATUS"].dropna().unique().tolist())

jo_filter = pn.widgets.Select(name="JO TYPE", options=jo_types)
sector_filter = pn.widgets.Select(name="SECTOR", options=sectors)
city_filter = pn.widgets.Select(name="CITY", options=cities)
tag_filter = pn.widgets.Select(name="TAG", options=tags)
jo_status_filter = pn.widgets.Select(name="JO STATUS", options=jo_statuses)
tech_status_filter = pn.widgets.Select(name="TECH STATUS", options=tech_statuses)
download_button = pn.widgets.Button(name="Download CSV", button_type="success")

# ===================== Plot Function =====================
def plot_ageing_bar(selected_jo_type, selected_sector, selected_city, selected_tag, selected_jo_status, selected_tech_status):
    df_filtered = df.copy()
    if selected_jo_type != "All":
        df_filtered = df_filtered[df_filtered["JO TYPE"] == selected_jo_type]
    if selected_sector != "All":
        df_filtered = df_filtered[df_filtered["SECTOR"] == selected_sector]
    if selected_city != "All":
        df_filtered = df_filtered[df_filtered["CITY"] == selected_city]
    if selected_tag != "All":
        df_filtered = df_filtered[df_filtered["TAG"] == selected_tag]
    if selected_jo_status != "All":
        df_filtered = df_filtered[df_filtered["JO STATUS"] == selected_jo_status]
    if selected_tech_status != "All":
        df_filtered = df_filtered[df_filtered["TECH STATUS"] == selected_tech_status]

    df_filtered["AGEING (RANGE)"] = pd.Categorical(
        df_filtered["AGEING (RANGE)"], categories=ageing_order, ordered=True
    )

    ageing_counts = (
        df_filtered.groupby("AGEING (RANGE)")["_RowNumber"]
        .count()
        .reindex(ageing_order)
        .fillna(0)
        .reset_index()
        .rename(columns={"_RowNumber": "Count"})
    )

    total = int(ageing_counts["Count"].sum())

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=ageing_counts, x="AGEING (RANGE)", y="Count", ax=ax,
                palette="Blues_d", edgecolor=None)

    for container in ax.containers:
        ax.bar_label(container, fmt=lambda x: f"{int(x):,}", label_type='edge', padding=3, fontsize=10)

    ax.set_title(f"Job Orders by Ageing Range (Total: {total:,})", fontsize=14)
    ax.set_xlabel("Ageing Range")
    ax.set_ylabel("Count of Job Orders")
    ax.set_xticklabels(ageing_counts["AGEING (RANGE)"], rotation=45)

    sns.despine(ax=ax, top=True, right=True)
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()

    return pn.pane.Matplotlib(fig, tight=True)

# ===================== Download Callback =====================
def download_csv(event):
    downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")

    df_filtered = df.copy()
    if jo_filter.value != "All":
        df_filtered = df_filtered[df_filtered["JO TYPE"] == jo_filter.value]
    if sector_filter.value != "All":
        df_filtered = df_filtered[df_filtered["SECTOR"] == sector_filter.value]
    if city_filter.value != "All":
        df_filtered = df_filtered[df_filtered["CITY"] == city_filter.value]
    if tag_filter.value != "All":
        df_filtered = df_filtered[df_filtered["TAG"] == tag_filter.value]
    if jo_status_filter.value != "All":
        df_filtered = df_filtered[df_filtered["JO STATUS"] == jo_status_filter.value]
    if tech_status_filter.value != "All":
        df_filtered = df_filtered[df_filtered["TECH STATUS"] == tech_status_filter.value]

    def clean(val):
        return val.replace(" ", "_").replace("/", "-") if val != "All" else "ALL"

    jo_val = clean(jo_filter.value)
    sector_val = clean(sector_filter.value)
    city_val = clean(city_filter.value)
    tag_val = clean(tag_filter.value)
    jo_status_val = clean(jo_status_filter.value)
    tech_status_val = clean(tech_status_filter.value)
    date_str = datetime.now().strftime("%Y%m%d")

    filename = f"c3_ni_{jo_val}_{sector_val}_{city_val}_{tag_val}_{jo_status_val}_{tech_status_val}_{date_str}.csv"
    save_path = os.path.join(downloads_path, filename)

    df_filtered.to_csv(save_path, index=False)
    pn.state.notifications.success(f"CSV saved as {filename}!")

download_button.on_click(download_csv)

# ===================== Main View =====================
main_view = pn.Column(
    "# Cluster3 New Install Dashboard (powered by kingsly👑)",
    pn.Row(jo_filter, sector_filter, city_filter, tag_filter, jo_status_filter, tech_status_filter),
    pn.bind(plot_ageing_bar, jo_filter, sector_filter, city_filter, tag_filter, jo_status_filter, tech_status_filter),
    pn.Row(download_button, sizing_mode="stretch_width")
)
main_view.visible = False

# ===================== Serve App =====================
def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('', 0))
    addr, port = s.getsockname()
    s.close()
    return port

if __name__ == "__main__":
    port = find_free_port()
    print(f"Launching on http://localhost:{port}")
    pn.serve(pn.Column(login_panel, main_view), port=port, show=True)
