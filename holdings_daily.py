# -*- coding: utf-8 -*-
"""Holdings daily - refactored for GitHub Actions (headless, no Colab).

Reads inputs from Google Drive via a service account, runs transformations,
writes outputs back to Drive and updates Google Sheets.
"""

import os
import io
import json
import re
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import gspread
from gspread_dataframe import set_with_dataframe

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

_sa_env = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
if not _sa_env:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON env var is not set")

_sa_info = json.loads(_sa_env)
creds = Credentials.from_service_account_info(_sa_info, scopes=SCOPES)

drive_svc = build("drive", "v3", credentials=creds, cache_discovery=False)
gs_client = gspread.authorize(creds)

WORK = tempfile.mkdtemp(prefix="holdings_")
print(f"Working directory: {WORK}")

def _escape_q(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")

def find_folder_id(name, parent_id=None):
    q = f"mimeType='application/vnd.google-apps.folder' and name='{_escape_q(name)}' and trashed=false"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    res = drive_svc.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def resolve_path(path_parts):
    parent = None
    for part in path_parts:
        fid = find_folder_id(part, parent)
        if not fid:
            raise FileNotFoundError(
                f"Drive folder not found: {'/'.join(path_parts)} (missing: {part})"
            )
        parent = fid
    return parent

def find_file_id(name, parent_id):
    q = f"name='{_escape_q(name)}' and '{parent_id}' in parents and trashed=false"
    res = drive_svc.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def download_file(file_id, local_path):
    req = drive_svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(local_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = downloader.next_chunk()
    return local_path

def upload_file(local_path, parent_folder_id, drive_name=None, mime=None):
    drive_name = drive_name or os.path.basename(local_path)
    existing = find_file_id(drive_name, parent_folder_id)
    media = MediaFileUpload(local_path, mimetype=mime, resumable=False)
    if existing:
        drive_svc.files().update(
            fileId=existing, media_body=media, supportsAllDrives=True
        ).execute()
        print(f"Updated on Drive: {drive_name}")
    else:
        drive_svc.files().create(
            body={"name": drive_name, "parents": [parent_folder_id]},
            media_body=media, supportsAllDrives=True,
        ).execute()
        print(f"Created on Drive: {drive_name}")

def dl_by_path(path_parts, filename):
    folder_id = resolve_path(path_parts)
    fid = find_file_id(filename, folder_id)
    if not fid:
        raise FileNotFoundError(f"{'/'.join(path_parts)}/{filename}")
    local = os.path.join(WORK, filename)
    return download_file(fid, local)

BASE           = ["Pranamya Financial Services"]
INPUT_FOLDER   = BASE + ["DAILY DATA", "Input folder"]
HOLDINGS_IN    = INPUT_FOLDER + ["Holdings Daily"]
OUTPUT_FOLDER  = BASE + ["DAILY DATA", "Output folder"]
LEDGER_BASE    = BASE + ["DAILY DATA", "Yearly Ledger"]
CLIENTS_PARENT = BASE + ["Clients"]

OUTPUT_FOLDER_ID  = resolve_path(OUTPUT_FOLDER)
CLIENTS_PARENT_ID = resolve_path(CLIENTS_PARENT)

ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
yesterday = (ist_now - timedelta(days=1)).date()
holding_name = f"HOLDING {yesterday.strftime('%d %B %Y').upper()}.XLSX"
print(f"Looking for holding file: {holding_name}")

file_path      = dl_by_path(HOLDINGS_IN, holding_name)
EQUITY_L_CSV   = dl_by_path(INPUT_FOLDER, "EQUITY_L.csv")
MARKET_CAP_XLS = dl_by_path(INPUT_FOLDER, "market capitalisation.xlsx")
LED1_PATH = dl_by_path(LEDGER_BASE + ["2023-24"],   "LEDGER_01-01-24_to_31-03-24__7DI0XNNOZ.XLSX")
LED2_PATH = dl_by_path(LEDGER_BASE + ["2024-25"],   "LEDGER_01-04-24_to_31-03-25__7DI0URBHR.XLSX")
LED3_PATH = dl_by_path(LEDGER_BASE + ["2025-26"],   "LEDGER_01-04-25_to_31-03-26_.XLSX")
LED4_PATH = dl_by_path(LEDGER_BASE + ["2026-2027"], "LEDGER_01-04-26_to_31-03-27__7FX0LGNCB.XLSX")

def clean_name(name):
    return re.sub(r'[\\/*?:"<>|]', '', name).strip()

# ==================================================================
# Original business logic (unchanged) begins below
# ==================================================================

import pandas as pd

# file_path is set in the header (auto-built for yesterday in IST)


df_raw = pd.read_excel(file_path, header=1)

# 2. Extract data starting after header row
df = df_raw.copy()

# Columns are in row 0
#df.columns = df.iloc[0]
#df = df[1:].reset_index(drop=True)

# Add columns for client info
df['Client Code'] = None
df['Client Name'] = None

current_code = None
current_name = None

df.head()

df.head()

# Add columns for client info
df['Client Code'] = None
df['Client Name'] = None

current_code = None
current_name = None

df.head(10)

# 3. Iterate through all rows
for i, row in df_raw.iterrows():
    first_col = str(row[0]).strip() if not pd.isna(row[0]) else ""
    # Detect client header row
    if re.match(r'^\d{16}\s', first_col):
        m = re.match(r'^(\d{16})\s+(.+?)\s+\[(.+?)\]$', first_col)
        if m:
            current_code = m.group(1)
            current_name = f"{m.group(2).strip()} [{m.group(3).strip()}]"
        continue
    # Detect ISIN row and assign
    #if i > 0 and first_col.startswith("IN") and current_code and current_name:
    if i > 0 and first_col.startswith("IN") or first_col.startswith("*IN")and current_code and current_name:
        df.at[i, 'Client Code'] = current_code
        df.at[i, 'Client Name'] = current_name

df.head()

df.columns = df.columns.str.replace('_x000D_\\n', ' ', regex=True)



# 4. Keep only ISIN rows
#df_final = df[df.iloc[:, 0].astype(str).str.startswith('IN', na=False)]

df_final = df[df.iloc[:, 0].astype(str).str.match(r'^\*?IN')]

# 5. Reorder columns
cols = ['Client Code', 'Client Name'] + [c for c in df_final.columns if c not in ['Client Code', 'Client Name']]
df_final = df_final[cols]

df_final.head()

df_final["Client Name"] = (
    df_final["Client Name"]
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)

df_final["Client Code"] = df_final["Client Name"].str.extract(r"\[([A-Z]{2,5}\d+)\]")

df_final["Client Name"] = (
    df_final["Client Name"]
    .str.replace(r"\s*\[[A-Z]{2,5}\d+\]", "", regex=True)
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)

df3=pd.read_csv(EQUITY_L_CSV)
df3.head()

df3.columns = (
    df3.columns
        .str.strip()                # remove leading & trailing spaces
        .str.replace(r'\s+', ' ', regex=True)  # replace multiple spaces with single space
)

df3.columns

df_final = df_final.merge(
    df3[['ISIN NUMBER', 'SYMBOL']],
    left_on='ISIN Code',
    right_on='ISIN NUMBER',
    how='left'
)

# Optional: drop the extra ISIN NUMBER column after merge
df_final = df_final.drop(columns=['ISIN NUMBER'])

sheet_id = "1wwsN_YwPDUg1J1LMAseUqxd-KJyEhkOudS9aF8PIYwM"
sheet_name = "Sheet1"

url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"

df_coupon = pd.read_csv(url)
df_coupon.head()

df_final = df_final.merge(df_coupon[['ISIN Code', 'Dividend','Amount 1','Date 1','Amount 2','Date 2','Amount 3','Date 3']],
                         on='ISIN Code',
                         how='left')
df_final.head()

df_final['Free Balance']=df_final['Free Balance'].astype(int)
df_final['ISIN Description']=df_final['ISIN Description']
df_final['Total Balance']=df_final['Total Balance'].astype(int)
df_final['Closing Rate']=df_final['Closing Rate'].astype(int)
df_final['Holding Valuation']=df_final['Holding Valuation'].astype(int)

df_final['Total Dividend']=df_final['Dividend']*df_final['Free Balance']
#df_final['Total Dividend']=df_final['Total Dividend'].astype(int)
df_final.head()

df4=pd.read_excel(MARKET_CAP_XLS, header=1)
df4.head()

import pandas as pd

# ------------------------------------------------------------
# Merge SEBI market cap data from df4 into df_final via ISIN
# ------------------------------------------------------------

# 1. Pick the column you want from df4 (last column = SEBI category label)
sebi_col = df4.columns[-1]
print("Column being merged from df4:", sebi_col)

# 2. Clean ISIN values on both sides
df_final['ISIN Code'] = df_final['ISIN Code'].astype(str).str.strip().str.upper()
df4['ISIN']           = df4['ISIN'].astype(str).str.strip().str.upper()

# 3. Keep only ISIN + required column from df4, remove duplicates
df4_slim = df4[['ISIN', sebi_col]].drop_duplicates(subset='ISIN')

# 4. Left-join onto df_final
df_final = df_final.merge(
    df4_slim,
    left_on='ISIN Code',
    right_on='ISIN',
    how='left'
)

# 5. Drop the redundant ISIN column from df4
df_final = df_final.drop(columns=['ISIN'])

# 6. Rename the merged column to 'Category'
df_final = df_final.rename(columns={sebi_col: 'Category'})

# ------------------------------------------------------------
# Match report
# ------------------------------------------------------------
matched   = df_final['Category'].notna().sum()
total     = len(df_final)
unmatched = df_final.loc[df_final['Category'].isna(), 'ISIN Code'].unique()

print(f"Matched  : {matched} / {total} rows")
print(f"Unmatched: {len(unmatched)} unique ISINs")
print("Sample unmatched ISINs:", unmatched[:10])

df_final.head()

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# ============================================================
# STEP 1: Asset classifier for unmatched ISINs (NaN bucket)
# ============================================================
def classify_asset(isin_desc):
    isin_desc = str(isin_desc).upper()
    if 'SGB' in isin_desc or 'GLD' in isin_desc or 'GOLD' in isin_desc:
        return 'Gold'
    elif 'REIT' in isin_desc or 'INVIT' in isin_desc:
        return 'REIT/InvIT'
    elif 'GS' in isin_desc or 'GSEC' in isin_desc or 'GOVT' in isin_desc:
        return 'Govt Securities'
    elif 'LIQ' in isin_desc or 'RTLIQBEES' in isin_desc or 'LIQUID' in isin_desc:
        return 'Liquid'
    elif 'SILVER' in isin_desc or 'SILVERBEES' in isin_desc:
        return 'Silver'
    else:
        return 'Other Equity'   # residual equity not in SEBI list

# ============================================================
# STEP 2: Build final Category column
#   - If SEBI category exists (Large/Mid/Small Cap) -> use it
#   - If NaN -> fall back to classify_asset(ISIN Description)
# ============================================================
df_final['Category'] = df_final['Category'].where(
    df_final['Category'].notna(),
    df_final['ISIN Description'].apply(classify_asset)
)

# Clean up — sometimes blank strings or 'nan' text sneak in
df_final['Category'] = df_final['Category'].replace(
    {'': 'Other Equity', 'nan': 'Other Equity', 'NaN': 'Other Equity'}
)

# ============================================================
# STEP 3: Ensure Holding Valuation is numeric
# ============================================================
df_final['Holding Valuation'] = pd.to_numeric(
    df_final['Holding Valuation'], errors='coerce'
).fillna(0)

# ============================================================
# STEP 4: Client-wise totals (denominator)
# ============================================================
client_total = (
    df_final.groupby(['Client Code', 'Client Name'], as_index=False)['Holding Valuation']
            .sum()
            .rename(columns={'Holding Valuation': 'Total Holding'})
)

# ============================================================
# STEP 5: Client × Category aggregation + % allocation
# ============================================================
cat_value = (
    df_final.groupby(['Client Code', 'Client Name', 'Category'], as_index=False)['Holding Valuation']
            .sum()
)

cat_value = cat_value.merge(client_total, on=['Client Code', 'Client Name'])
cat_value['Allocation %'] = (
    cat_value['Holding Valuation'] / cat_value['Total Holding'] * 100
).round(2)

# ============================================================
# STEP 6: Pivot — wide format (one row per client)
# ============================================================
final_allocation = cat_value.pivot_table(
    index=['Client Code', 'Client Name'],
    columns='Category',
    values='Allocation %',
    fill_value=0
).reset_index()

# Preferred column order (only keeps those present)
preferred_order = [
    'Large Cap', 'Mid Cap', 'Small Cap',
    'REIT/InvIT', 'Govt Securities', 'Gold', 'Silver',
    'Liquid', 'Other Equity'
]
existing = [c for c in preferred_order if c in final_allocation.columns]
final_allocation = final_allocation[['Client Code', 'Client Name'] + existing]

# Attach Total Holding value + sanity check
final_allocation = final_allocation.merge(client_total, on=['Client Code', 'Client Name'])
final_allocation['Check Sum %'] = final_allocation[existing].sum(axis=1).round(2)

print(final_allocation.head(10))
print(f"\nTotal clients: {len(final_allocation)}")

# ============================================================
# STEP 7 (REVISED): Push to Google Sheets — prevent date coercion
# ============================================================
# gspread client already built in header; re-bind to 'client' for compatibility
from gspread.utils import rowcol_to_a1
client = gs_client
spreadsheet = client.open_by_key("1M9Hehl1Uyr-sUbCgw4qxMalRqZPMtDRql1Kv03fLSbk")

tab_name = "Asset Allocation %"
try:
    worksheet = spreadsheet.worksheet(tab_name)
    worksheet.clear()
except gspread.WorksheetNotFound:
    worksheet = spreadsheet.add_worksheet(
        title=tab_name,
        rows=len(final_allocation) + 5,
        cols=len(final_allocation.columns) + 2
    )

# ---- FIX 1: Force all numeric columns to float (not object) ----
numeric_cols = [c for c in final_allocation.columns
                if c not in ['Client Code', 'Client Name']]
for c in numeric_cols:
    final_allocation[c] = pd.to_numeric(final_allocation[c], errors='coerce').fillna(0).astype(float)

# Write data
set_with_dataframe(worksheet, final_allocation)

# ---- FIX 2: Force number format on numeric columns AFTER writing ----
# This prevents Sheets from auto-detecting as date
n_rows = len(final_allocation) + 1   # +1 for header
n_cols = len(final_allocation.columns)

# Get column letter range for numeric columns (cols 3 onwards = C onwards)
start_cell = rowcol_to_a1(2, 3)                    # e.g. C2
end_cell   = rowcol_to_a1(n_rows, n_cols)          # e.g. L250

worksheet.format(f"{start_cell}:{end_cell}", {
    "numberFormat": {
        "type": "NUMBER",
        "pattern": "0.00"
    }
})

print(f"✅ Google Sheet updated → tab: '{tab_name}' with number formatting applied")

# Select only required columns
df_required = df_final[['Client Name', 'ISIN Description', 'Total Balance']]

# Create pivot table
pivot_df = pd.pivot_table(
    df_required,
    index='Client Name',            # rows
    columns='ISIN Description',      # columns
    values='Total Balance',          # cell values
    aggfunc='sum',                   # sum in case of duplicates
    fill_value=0                     # replace NaN with 0
)

# Optional: reset index if you want Client Name as a column
pivot_df = pivot_df.reset_index()

# Open existing Google Sheet
spreadsheet = client.open_by_key("1M9Hehl1Uyr-sUbCgw4qxMalRqZPMtDRql1Kv03fLSbk")
#worksheet = spreadsheet.sheet1


# Select Sheet2
worksheet = spreadsheet.worksheet("Share Wise_details")


# Clear old data (optional but recommended)
worksheet.clear()

# Write dataframe
set_with_dataframe(worksheet, pivot_df)

print("Google Sheet updated successfully")

# Group and sum Total Dividend for each Client Name
df_dividend_summary = df_final.groupby('Client Name', as_index=False)['Total Dividend'].sum()

# Rename column for clarity (optional)
df_dividend_summary.rename(columns={'Total Dividend': 'Total Dividend Sum'}, inplace=True)

# Display result
df_dividend_summary.head()

df_dividend_summary['Client Name'] = (
    df_dividend_summary['Client Name']
    .str.replace(r'\s*\[.*?\]', '', regex=True)
    .str.strip()
)
df_dividend_summary.head()





# 6. Save cleaned output
output_path = os.path.join(WORK, "Holdings123.xlsx")
df_final.to_excel(output_path, index=False)
upload_file(output_path, OUTPUT_FOLDER_ID,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

print("File saved successfully at:", output_path)

# Open existing Google Sheet
spreadsheet = client.open_by_key("1M9Hehl1Uyr-sUbCgw4qxMalRqZPMtDRql1Kv03fLSbk")
#worksheet = spreadsheet.sheet1

# Select Sheet3
worksheet = spreadsheet.worksheet("dividend_summary")


# Clear old data (optional but recommended)
worksheet.clear()



# Write dataframe
set_with_dataframe(worksheet, df_dividend_summary)

print("Google Sheet updated successfully")

df_final["Client Code"] = df_final["Client Name"].str.extract(r"\[([A-Z]{2,5}\d+)\]")

df_final["Client Name"] = df_final["Client Name"].str.replace(
    r"\s*\[[A-Z]{2,5}\d+\]",
    "",
    regex=True
)

df_sharePrice=df_final[['ISIN Code','ISIN Description','Closing Rate']]

df_sharePrice['Date'] = pd.Timestamp.today().date()

df_sharePrice.head(10)

#6. Save cleaned output
output_path = os.path.join(WORK, "Share Price.xlsx")
df_sharePrice.to_excel(output_path, index=False)
upload_file(output_path, OUTPUT_FOLDER_ID,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

print("File saved successfully at:", output_path)

# Open existing Google Sheet
spreadsheet = client.open_by_key("1M9Hehl1Uyr-sUbCgw4qxMalRqZPMtDRql1Kv03fLSbk")
#worksheet = spreadsheet.sheet1

# Select Sheet4
worksheet = spreadsheet.worksheet("Share Price")

# Clear old data (optional but recommended)
worksheet.clear()

# Write dataframe
set_with_dataframe(worksheet, df_sharePrice)

print("Google Sheet updated successfully")

# Group and sum Total Dividend for each Client Name
df_Holding_summary = df_final.groupby('Client Name', as_index=False)['Holding Valuation'].sum()



# Display result
df_Holding_summary.head()

# Open existing Google Sheet
spreadsheet = client.open_by_key("1M9Hehl1Uyr-sUbCgw4qxMalRqZPMtDRql1Kv03fLSbk")
#worksheet = spreadsheet.sheet1

# Select Sheet5
worksheet = spreadsheet.worksheet("holding_details")

# Clear old data (optional but recommended)
worksheet.clear()

# Write dataframe
set_with_dataframe(worksheet, df_Holding_summary)

print("Google Sheet updated successfully")

df_Holding_summary.sum()



df_final.head()

import os
import re

BASE_DIR = WORK   # local workdir; per-client files uploaded to Drive after write

def clean_name(name):
    return re.sub(r'[\\/*?:"<>|]', '', name).strip()

import numpy as np

# ── Load Ledger files (for Cash Details sheet) ──────────────────────────
# ledger_base not used — per-file local paths set via LED*_PATH

df_led1 = pd.read_excel(LED1_PATH, header=1)
df_led2 = pd.read_excel(LED2_PATH, header=1)
df_led3 = pd.read_excel(LED3_PATH, header=1)
df_led4 = pd.read_excel(LED4_PATH, header=1)


# ── Standardize column names for each file before concat ────────────────
def standardize_ledger_columns(df):
    """Rename columns to a common format regardless of source file format."""
    rename_map = {}
    for col in df.columns:
        col_clean = col.replace("_x000D_", "").replace("\n", " ").replace("\r", " ").strip()
        col_lower = col_clean.lower()
        if "voucher" in col_lower and "date" in col_lower:
            rename_map[col] = "Date"
        elif "entry" in col_lower and "detail" in col_lower:
            rename_map[col] = "Details"
        elif "amount" in col_lower and "debit" in col_lower:
            rename_map[col] = "Debit"
        elif "amount" in col_lower and "credit" in col_lower:
            rename_map[col] = "Credit"
        elif "running" in col_lower and "balance" in col_lower:
            rename_map[col] = "Balance"
    df = df.rename(columns=rename_map)
    # Drop unwanted columns
    drop_cols = [c for c in df.columns if c in ['Branch', 'Sett#', 'DrCr'] or c.startswith('Unnamed')]
    df = df.drop(columns=drop_cols, errors='ignore')
    return df

df_led1 = standardize_ledger_columns(df_led1)
df_led2 = standardize_ledger_columns(df_led2)
df_led3 = standardize_ledger_columns(df_led3)
df_led4 = standardize_ledger_columns(df_led4)

df_ledger = pd.concat([df_led1, df_led2, df_led3, df_led4], axis=0, ignore_index=True)

# Extract and forward-fill client names
df_ledger["Client_Name"] = np.where(
    df_ledger["Date"].astype(str).str.contains(r"\[.*\]", na=False),
    df_ledger["Date"],
    np.nan
)
df_ledger["Client_Name"] = df_ledger["Client_Name"].ffill()

# Filter only BANK CLIENT A/C rows
df_cash = df_ledger[
    df_ledger["Details"].str.contains(
        r"\bBANK\b.*\bCLIENT\b.*\b(A\s*/\s*C|AC|ACCOUNT)\b",
        case=False, na=False, regex=True
    )
].copy()

# Extract Client Code and clean Client Name — strip + normalize spaces
df_cash["Client Code"] = df_cash["Client_Name"].str.extract(r"\[([A-Z]{2,5}\d+)\]")
df_cash["Client Name"] = (
    df_cash["Client_Name"]
    .str.replace(r"\s*\[[A-Z]{2,5}\d+\]", "", regex=True)
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)
df_cash = df_cash.drop(columns=["Client_Name"])

print(f"Cash details loaded: {len(df_cash)} rows for {df_cash['Client Name'].nunique()} clients")

import pandas as pd
import re
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe

# ======================================================================
# STEP 1: Ensure columns are correctly named
# ======================================================================
# If df_led4 was read with header=None, set column names explicitly
# Expected structure: Date | Branch | Sett# | Details | Debit | Credit | Balance | DrCr | Extra
if df_led4.columns[0] != 'Date':
    df_led4.columns = ['Date', 'Branch', 'Sett', 'Details', 'Debit', 'Credit', 'Balance', 'DrCr', 'Extra'][:len(df_led4.columns)]

# ======================================================================
# STEP 2: Identify client header rows (e.g. "ABHAY PREMDAS TEMBHURNE [NGPM071]")
# Client name appears in the 'Date' column in header rows
# ======================================================================
client_pattern = re.compile(r'^([A-Z][A-Z\s\.]+?\s*\[[A-Z0-9]+\])\s*$')

def extract_client(val):
    if pd.isna(val) or not isinstance(val, str):
        return None
    m = client_pattern.match(val.strip())
    return m.group(1).strip() if m else None

df_led4['_client_tag'] = df_led4['Date'].apply(extract_client)
df_led4['_client'] = df_led4['_client_tag'].ffill()   # propagate client name downward

# ======================================================================
# STEP 3: Clean numeric + date columns
# ======================================================================
df_led4['_date']   = pd.to_datetime(df_led4['Date'], format='%d/%m/%Y', errors='coerce')
df_led4['_debit']  = pd.to_numeric(df_led4['Debit'], errors='coerce').fillna(0)
df_led4['_credit'] = pd.to_numeric(df_led4['Credit'], errors='coerce').fillna(0)

# ======================================================================
# STEP 4: Filter last 1 month & identify PAYOUTS (money sent back to client's bank)
# Rule: dated row + Details contains "HDFC BANK" + amount in Debit column
# (Pay-ins like "HDFC BANK CLIENT ACCOUNT [F&O]" have amount in Credit, so auto-excluded)
# ======================================================================
today = pd.Timestamp(datetime.today().date())
one_month_ago = today - pd.DateOffset(months=1)

payout_mask = (
    df_led4['_date'].notna()
    & (df_led4['_debit'] > 0)
    & df_led4['Details'].astype(str).str.contains('HDFC BANK', case=False, na=False)
    & (df_led4['_date'] >= one_month_ago)
    & (df_led4['_date'] <= today)
)

payouts = df_led4[payout_mask].copy()

print(f"Period: {one_month_ago.strftime('%d/%m/%Y')} to {today.strftime('%d/%m/%Y')}")
print(f"Total payout transactions found: {len(payouts)}")
print(f"Grand total debit to bank: ₹{payouts['_debit'].sum():,.2f}\n")

# ======================================================================
# STEP 5: Build per-client summary
# ======================================================================
summary_df = (
    payouts.groupby('_client')
           .agg(**{
               'Total Debit to Bank': ('_debit', 'sum'),
               'No. of Transactions': ('_debit', 'count'),
               'Last Payout Date': ('_date', 'max')
           })
           .reset_index()
           .rename(columns={'_client': 'Client Name'})
)

summary_df['Last Payout Date'] = summary_df['Last Payout Date'].dt.strftime('%d/%m/%Y')
summary_df['Period From']  = one_month_ago.strftime('%d/%m/%Y')
summary_df['Period To']    = today.strftime('%d/%m/%Y')
summary_df['Last Updated'] = datetime.now().strftime('%d/%m/%Y %H:%M')

summary_df = summary_df[['Client Name', 'Period From', 'Period To',
                         'Total Debit to Bank', 'No. of Transactions',
                         'Last Payout Date', 'Last Updated']]

print(summary_df.to_string(index=False))

# ======================================================================
# STEP 6: Write to Google Sheet → "debit balance" worksheet
# ======================================================================
spreadsheet = client.open_by_key("1M9Hehl1Uyr-sUbCgw4qxMalRqZPMtDRql1Kv03fLSbk")

try:
    master_ws = spreadsheet.worksheet("debit balance")
    master_ws.clear()
except gspread.exceptions.WorksheetNotFound:
    master_ws = spreadsheet.add_worksheet(title="debit balance", rows=1000, cols=10)

set_with_dataframe(master_ws, summary_df)

# Clean up helper columns
df_led4.drop(columns=['_client_tag', '_client', '_date', '_debit', '_credit'],
             inplace=True, errors='ignore')

print(f"\n✅ {len(summary_df)} clients written to 'debit balance' sheet")

for client_name, client_df in df_final.groupby("Client Name"):

    folder_name = clean_name(client_name).strip()
    folder_name = re.sub(r"\s+", " ", folder_name)  # normalize multi-spaces

    client_folder_id = find_folder_id(folder_name, CLIENTS_PARENT_ID)
    if not client_folder_id:
        print(f"Folder not found for client: {client_name}")
        continue

    local_file = os.path.join(WORK, f"{folder_name}_HOLDINGS.xlsx")

    # Get this client's cash details
    client_cash = df_cash[df_cash["Client Name"] == client_name].copy()

    with pd.ExcelWriter(local_file, engine="openpyxl") as writer:
        client_df.to_excel(writer, sheet_name="Holdings", index=False)
        if not client_cash.empty:
            client_cash.to_excel(writer, sheet_name="Cash Details", index=False)

    upload_file(local_file, client_folder_id,
                drive_name=f"{folder_name}_HOLDINGS.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

df['Date'] = pd.Timestamp.today().date()
df.head()
