import pandas as pd
import glob
import os
import numpy as np

# === CSV Import for df_da ===
csv_folder = r'C:\Users\kingsly.alovera\Documents\pycharm_scripts\subscriber_line_installation\data_analytics\import_files\data_analytics'
csv_files = glob.glob(os.path.join(csv_folder, '*.csv'))

if csv_files:
    df_da = pd.read_csv(csv_files[0])
    print(f"CSV Loaded (df_da): {os.path.basename(csv_files[0])}")
else:
    print("No CSV files found in the specified directory.")

# === Excel Import for df_ac ===
excel_folder = r'C:\Users\kingsly.alovera\Documents\pycharm_scripts\subscriber_line_installation\data_analytics\import_files\agent_codes'
excel_files = glob.glob(os.path.join(excel_folder, '*.xlsx'))

if excel_files:
    df_ac = pd.read_excel(excel_files[0])
    print(f"Excel Loaded (df_ac): {os.path.basename(excel_files[0])}")
else:
    print("No Excel files found in the specified directory.")

# ====================================================== Create Dates ======================================================

# Make sure the column is in datetime format
df_da["StatusUpdatedOn"] = pd.to_datetime(df_da["StatusUpdatedOn"], errors='coerce')

# Create the new column with date only
df_da["StatusUpdatedOn Date"] = df_da["StatusUpdatedOn"].dt.date

# ====================================================== CLEAN CITY ======================================================

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
    'Las PiÃ±as'

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

# Function to check city name based on keywords
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

# Apply city classification
df_da['Final City'] = df_da['Municipality'].apply(check_city)

# Define cluster conditions
conditions = [
    df_da['Final City'].isin(['CALOOCAN', 'MALABON', 'NAVOTAS', 'VALENZUELA']),
    df_da['Final City'] == 'QUEZON CITY',
    df_da['Final City'].isin(['MANILA', 'SAN JUAN', 'MAKATI', 'MANDALUYONG']),
    df_da['Final City'].isin(['MARIKINA', 'PASIG', 'TAGUIG', 'PATEROS']),
    df_da['Final City'].isin(['MUNTINLUPA', 'PASAY', 'PARAÑAQUE', 'LAS PIÑAS']),
    df_da['Final City'] == 'OTHERS'
]

choices = ['Cluster 1', 'Cluster 2', 'Cluster 3', 'Cluster 4', 'Cluster 5', 'MISROUTED']

# Assign Cluster based on Final City
df_da['Cluster'] = np.select(conditions, choices, default='Unknown')

# ====================================================== Create Partition ======================================================


import numpy as np

# Define the groups
manila_1 = [
    "TONDO I/II, CITY OF MANILA",
    "SANTA CRUZ, CITY OF MANILA",
    "BINONDO, CITY OF MANILA",
    "SAN NICOLAS, CITY OF MANILA",
    "QUIAPO, CITY OF MANILA",
    "SAMPALOC, CITY OF MANILA",
    "SAN MIGUEL, CITY OF MANILA"
]

manila_2 = [
    "PORT AREA, CITY OF MANILA",
    "INTRAMUROS, CITY OF MANILA",
    "ERMITA, CITY OF MANILA",
    "MALATE, CITY OF MANILA",
    "SAN ANDRES, CITY OF MANILA",
    "PACO, CITY OF MANILA",
    "SANTA MESA, CITY OF MANILA",
    "SANTA ANA, CITY OF MANILA",
    "PANDACAN, CITY OF MANILA"
]

# Apply logic row-wise
df_da['City Partition'] = df_da['Municipality'].apply(
    lambda x: 'MANILA 1' if x in manila_1 else (
        'MANILA 2' if x in manila_2 else np.nan)
)

# Fill fallback value with Final City if not tagged as MANILA 1 or 2
df_da['City Partition'] = df_da['City Partition'].fillna(df_da['Final City'])

# ====================================================== Bash Agent Codes ======================================================

# Ensure both columns are strings
df_da['AgentCode'] = df_da['AgentCode'].astype(str)
df_ac['AGENT CODE'] = df_ac['AGENT CODE'].astype(str)

# Create a mapping from AGENT CODE to AGENCY NAME
agent_map = df_ac.set_index('AGENT CODE')['AGENCY NAME'].to_dict()

# Create 'Matched AC' and 'AGENCY NAME' columns
df_da['Matched AC'] = df_da['AgentCode'].apply(lambda x: "Matched" if x in agent_map else "Not Matched")
df_da['AGENCY NAME'] = df_da['AgentCode'].map(agent_map)

# ====================================================== Filter ======================================================

df_da = df_da[
    (~df_da['AGENCY NAME'].str.contains('MYRIAD/NON TRAD|MYRIAD/SME|MYRIAD/SME AGENT', na=False)) &
    (df_da['Cluster'] == 'Cluster 3') &
    (df_da['ApplicationStatus'] == 'Active')
]

# ====================================================== Create Cluster ======================================================

import numpy as np

conditions = [
    df_da['City Partition'] == 'MANILA 1',
    df_da['City Partition'].isin(['MANILA 2', 'SAN JUAN']),
    df_da['City Partition'] == 'MANDALUYONG',
    df_da['City Partition'] == 'MAKATI'
]

choices = ['Sector 1', 'Sector 2', 'Sector 3', 'Sector 4']

df_da['C3 Sector'] = np.select(conditions, choices, default='Unknown')


# ====================================================== Create Dashboard ======================================================

import pandas as pd
import panel as pn
import numpy as np

pn.extension('tabulator')

# Ensure date is in datetime format
df_da['StatusUpdatedOn Date'] = pd.to_datetime(df_da['StatusUpdatedOn']).dt.date

# Date pickers
start_date_picker = pn.widgets.DatePicker(name='Start Date', value=None, width=200)
end_date_picker = pn.widgets.DatePicker(name='End Date', value=None, width=200)

# Update function with average calculation
@pn.depends(start_date_picker.param.value, end_date_picker.param.value)
def update_pivot_table(start_date, end_date):
    filtered_df = df_da.copy()

    if start_date and end_date:
        filtered_df = filtered_df[
            (filtered_df['StatusUpdatedOn Date'] >= start_date) &
            (filtered_df['StatusUpdatedOn Date'] <= end_date)
        ]
        num_days = (end_date - start_date).days + 1
    else:
        num_days = 1  # Avoid division by zero

    pivot_df = (
        filtered_df
        .groupby(['C3 Sector', 'City Partition'])['ApplicationRefID']
        .count()
        .reset_index(name='Count of ApplicationRefID')
    )

    # Whole number average
    pivot_df['Average Per Day'] = (pivot_df['Count of ApplicationRefID'] / num_days).round(0).astype(int)

    # Grand total row
    grand_total = pivot_df['Count of ApplicationRefID'].sum()
    avg_total = (grand_total / num_days) if num_days > 0 else 0
    pivot_df.loc[len(pivot_df.index)] = ['Grand Total', '', grand_total, int(round(avg_total, 0))]

    return pn.widgets.Tabulator(pivot_df, pagination='remote', page_size=10, layout='fit_columns')


# Layout
dashboard = pn.Column(
    '# Installed & Activated (Cluster 3)',
    pn.Row(start_date_picker, end_date_picker),
    update_pivot_table
)

dashboard.servable()
pn.serve(dashboard)