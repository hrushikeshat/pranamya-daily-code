# -*- coding: utf-8 -*-
"""Holdings daily - refactored for GitHub Actions (headless, no Colab).

Reads inputs from Google Drive via a service account, runs transformations,
writes outputs back to Drive and updates Google Sheets.

FIXES APPLIED (vs. previous version):
  1. Client header whitespace normalization BEFORE ffill — fixes duplicate
     "RENUKA PATIL" / "RENUKA  PATIL" type headers that caused half the
     cash rows to disappear during the per-client Excel writer's join.
  2. Cash filter broadened to include RAZORPAY pay-ins (previously missed
     ~60 rows across ~49 clients), plus inter-segment transfers and
     late-payment fees which are real ledger debits.
"""

import os
import io
import json
import re
import tempfile
import time
import socket
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from googleapiclient.errors import HttpError
import gspread
from gspread.utils import rowcol_to_a1
from gspread_dataframe import set_with_dataframe

# ============================================================================
#  AUTH / DRIVE / SHEETS CLIENTS
# ============================================================================

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
client = gs_client  # alias used later

WORK = tempfile.mkdtemp(prefix="holdings_")
print(f"Working directory: {WORK}")


# ============================================================================
#  DRIVE HELPERS
# ============================================================================

def _escape_q(s):
    return s.replace("\\", "\\\\").replace("'", "\\'")


def _retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs) retrying on transient errors (5xx, socket)."""
    delays = [0.5, 1, 2, 4, 8]
    last_exc = None
    for i, d in enumerate(delays + [None]):
        try:
            return fn(*args, **kwargs)
        except HttpError as e:
            status = getattr(e, "resp", None)
            code = getattr(status, "status", None)
            try:
                code = int(code) if code is not None else None
            except (TypeError, ValueError):
                code = None
            if code is not None and 500 <= code < 600:
                last_exc = e
                if d is None:
                    break
                print(f"  transient HttpError {code}; retrying in {d}s "
                      f"(attempt {i+1}/{len(delays)})")
                time.sleep(d)
                continue
            raise
        except (socket.error, TimeoutError, ConnectionError) as e:
            last_exc = e
            if d is None:
                break
            print(f"  transient network error {type(e).__name__}; "
                  f"retrying in {d}s (attempt {i+1}/{len(delays)})")
            time.sleep(d)
            continue
    raise last_exc


def find_folder_id(name, parent_id=None):
    q = (f"mimeType='application/vnd.google-apps.folder' "
         f"and name='{_escape_q(name)}' and trashed=false")
    if parent_id:
        q += f" and '{parent_id}' in parents"
    res = _retry(drive_svc.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute)
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
    res = _retry(drive_svc.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute)
    files = res.get("files", [])
    return files[0]["id"] if files else None


def download_file(file_id, local_path):
    req = drive_svc.files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(local_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, req)
        done = False
        while not done:
            _, done = _retry(downloader.next_chunk)
    return local_path


def upload_file(local_path, parent_folder_id, drive_name=None, mime=None):
    drive_name = drive_name or os.path.basename(local_path)
    existing = find_file_id(drive_name, parent_folder_id)
    media = MediaFileUpload(local_path, mimetype=mime, resumable=False)
    if existing:
        _retry(drive_svc.files().update(
            fileId=existing, media_body=media, supportsAllDrives=True
        ).execute)
        print(f"Updated on Drive: {drive_name}")
    else:
        _retry(drive_svc.files().create(
            body={"name": drive_name, "parents": [parent_folder_id]},
            media_body=media, supportsAllDrives=True,
        ).execute)
        print(f"Created on Drive: {drive_name}")


def dl_by_path(path_parts, filename):
    folder_id = resolve_path(path_parts)
    fid = find_file_id(filename, folder_id)
    if not fid:
        raise FileNotFoundError(f"{'/'.join(path_parts)}/{filename}")
    local = os.path.join(WORK, filename)
    return download_file(fid, local)


def clean_name(name):
    return re.sub(r'[\\/*?:"<>|]', '', name).strip()


# ============================================================================
#  RESOLVE DRIVE PATHS & DOWNLOAD INPUTS
# ============================================================================

BASE           = ["Pranamya Financial Services"]
INPUT_FOLDER   = BASE + ["DAILY DATA", "Input folder"]
HOLDINGS_IN    = INPUT_FOLDER + ["Holdings Daily"]
OUTPUT_FOLDER  = BASE + ["DAILY DATA", "Output folder"]
LEDGER_BASE    = BASE + ["DAILY DATA", "Yearly Ledger"]
CLIENTS_PARENT = BASE + ["Clients"]

OUTPUT_FOLDER_ID  = resolve_path(OUTPUT_FOLDER)
CLIENTS_PARENT_ID = resolve_path(CLIENTS_PARENT)

ist_now   = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
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


# ============================================================================
#  HOLDINGS FILE — PARSE RAW SHEET
# ============================================================================

df_raw = pd.read_excel(file_path, header=1)
df = df_raw.copy()

df['Client Code'] = None
df['Client Name'] = None

current_code = None
current_name = None

# Walk every row, detecting client-header rows and tagging the ISIN rows that
# follow with the right client code/name.
for i, row in df_raw.iterrows():
    first_col = str(row[0]).strip() if not pd.isna(row[0]) else ""

    # Client header row: starts with a 16-digit code
    if re.match(r'^\d{16}\s', first_col):
        m = re.match(r'^(\d{16})\s+(.+?)\s+\[(.+?)\]$', first_col)
        if m:
            current_code = m.group(1)
            current_name = f"{m.group(2).strip()} [{m.group(3).strip()}]"
        continue

    # ISIN rows (both "IN..." and "*IN..." variants)
    if (i > 0
        and (first_col.startswith("IN") or first_col.startswith("*IN"))
        and current_code and current_name):
        df.at[i, 'Client Code'] = current_code
        df.at[i, 'Client Name'] = current_name

df.columns = df.columns.str.replace('_x000D_\\n', ' ', regex=True)

# Keep only ISIN rows
df_final = df[df.iloc[:, 0].astype(str).str.match(r'^\*?IN')]

# Reorder columns
cols = ['Client Code', 'Client Name'] + [c for c in df_final.columns
                                         if c not in ['Client Code', 'Client Name']]
df_final = df_final[cols]

# Normalize client-name whitespace and re-extract code from the bracketed suffix
df_final["Client Name"] = (
    df_final["Client Name"].str.strip().str.replace(r"\s+", " ", regex=True)
)
df_final["Client Code"] = df_final["Client Name"].str.extract(r"\[([A-Z]{2,5}\d+)\]")
df_final["Client Name"] = (
    df_final["Client Name"]
    .str.replace(r"\s*\[[A-Z]{2,5}\d+\]", "", regex=True)
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)


# ============================================================================
#  MERGE: EQUITY SYMBOLS, DIVIDENDS, SEBI MARKET-CAP CATEGORY
# ============================================================================

df3 = pd.read_csv(EQUITY_L_CSV)
df3.columns = df3.columns.str.strip().str.replace(r'\s+', ' ', regex=True)

df_final = df_final.merge(
    df3[['ISIN NUMBER', 'SYMBOL']],
    left_on='ISIN Code', right_on='ISIN NUMBER', how='left'
)
df_final = df_final.drop(columns=['ISIN NUMBER'])

# Dividend sheet (external Google Sheet in CSV export mode)
sheet_id   = "1wwsN_YwPDUg1J1LMAseUqxd-KJyEhkOudS9aF8PIYwM"
sheet_name = "Sheet1"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
df_coupon = pd.read_csv(url)

df_final = df_final.merge(
    df_coupon[['ISIN Code', 'Dividend', 'Amount 1', 'Date 1',
               'Amount 2', 'Date 2', 'Amount 3', 'Date 3']],
    on='ISIN Code', how='left'
)

df_final['Free Balance']      = df_final['Free Balance'].astype(int)
df_final['Total Balance']     = df_final['Total Balance'].astype(int)
df_final['Closing Rate']      = df_final['Closing Rate'].astype(int)
df_final['Holding Valuation'] = df_final['Holding Valuation'].astype(int)
df_final['Total Dividend']    = df_final['Dividend'] * df_final['Free Balance']

# SEBI market-cap categorization
df4 = pd.read_excel(MARKET_CAP_XLS, header=1)
sebi_col = df4.columns[-1]
print("Column being merged from df4:", sebi_col)

df_final['ISIN Code'] = df_final['ISIN Code'].astype(str).str.strip().str.upper()
df4['ISIN']           = df4['ISIN'].astype(str).str.strip().str.upper()

df4_slim = df4[['ISIN', sebi_col]].drop_duplicates(subset='ISIN')
df_final = df_final.merge(df4_slim, left_on='ISIN Code', right_on='ISIN', how='left')
df_final = df_final.drop(columns=['ISIN']).rename(columns={sebi_col: 'Category'})

matched   = df_final['Category'].notna().sum()
total     = len(df_final)
unmatched = df_final.loc[df_final['Category'].isna(), 'ISIN Code'].unique()
print(f"Matched  : {matched} / {total} rows")
print(f"Unmatched: {len(unmatched)} unique ISINs")
print("Sample unmatched ISINs:", unmatched[:10])


# ============================================================================
#  ASSET ALLOCATION  (Large/Mid/Small Cap + buckets for non-SEBI items)
# ============================================================================

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
        return 'Other Equity'

df_final['Category'] = df_final['Category'].where(
    df_final['Category'].notna(),
    df_final['ISIN Description'].apply(classify_asset)
)
df_final['Category'] = df_final['Category'].replace(
    {'': 'Other Equity', 'nan': 'Other Equity', 'NaN': 'Other Equity'}
)
df_final['Holding Valuation'] = pd.to_numeric(
    df_final['Holding Valuation'], errors='coerce'
).fillna(0)

# Client-wise totals
client_total = (
    df_final.groupby(['Client Code', 'Client Name'], as_index=False)['Holding Valuation']
            .sum()
            .rename(columns={'Holding Valuation': 'Total Holding'})
)

# Client × Category
cat_value = (
    df_final.groupby(['Client Code', 'Client Name', 'Category'], as_index=False)['Holding Valuation']
            .sum()
            .merge(client_total, on=['Client Code', 'Client Name'])
)
cat_value['Allocation %'] = (
    cat_value['Holding Valuation'] / cat_value['Total Holding'] * 100
).round(2)

final_allocation = cat_value.pivot_table(
    index=['Client Code', 'Client Name'],
    columns='Category', values='Allocation %', fill_value=0
).reset_index()

preferred_order = ['Large Cap', 'Mid Cap', 'Small Cap',
                   'REIT/InvIT', 'Govt Securities', 'Gold', 'Silver',
                   'Liquid', 'Other Equity']
existing = [c for c in preferred_order if c in final_allocation.columns]
final_allocation = final_allocation[['Client Code', 'Client Name'] + existing]
final_allocation = final_allocation.merge(client_total, on=['Client Code', 'Client Name'])
final_allocation['Check Sum %'] = final_allocation[existing].sum(axis=1).round(2)

print(final_allocation.head(10))
print(f"\nTotal clients: {len(final_allocation)}")


# ============================================================================
#  WRITE: Asset Allocation % tab (with explicit number format — no date coercion)
# ============================================================================

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

numeric_cols = [c for c in final_allocation.columns
                if c not in ['Client Code', 'Client Name']]
for c in numeric_cols:
    final_allocation[c] = pd.to_numeric(
        final_allocation[c], errors='coerce'
    ).fillna(0).astype(float)

set_with_dataframe(worksheet, final_allocation)

n_rows = len(final_allocation) + 1
n_cols = len(final_allocation.columns)
start_cell = rowcol_to_a1(2, 3)
end_cell   = rowcol_to_a1(n_rows, n_cols)
worksheet.format(f"{start_cell}:{end_cell}",
                 {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}})
print(f"✅ Google Sheet updated → tab: '{tab_name}' with number formatting applied")


# ============================================================================
#  WRITE: Share-wise pivot, Dividend summary, Share price, Holding summary
# ============================================================================

# Share-wise pivot
df_required = df_final[['Client Name', 'ISIN Description', 'Total Balance']]
pivot_df = pd.pivot_table(
    df_required, index='Client Name', columns='ISIN Description',
    values='Total Balance', aggfunc='sum', fill_value=0
).reset_index()

worksheet = spreadsheet.worksheet("Share Wise_details")
worksheet.clear()
set_with_dataframe(worksheet, pivot_df)
print("Google Sheet updated: Share Wise_details")

# Dividend summary
df_dividend_summary = df_final.groupby('Client Name', as_index=False)['Total Dividend'].sum()
df_dividend_summary.rename(columns={'Total Dividend': 'Total Dividend Sum'}, inplace=True)
df_dividend_summary['Client Name'] = (
    df_dividend_summary['Client Name']
    .str.replace(r'\s*\[.*?\]', '', regex=True).str.strip()
)

# Save full Holdings123 Excel before the dividend tab write
output_path = os.path.join(WORK, "Holdings123.xlsx")
df_final.to_excel(output_path, index=False)
upload_file(output_path, OUTPUT_FOLDER_ID,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
print("File saved successfully at:", output_path)

worksheet = spreadsheet.worksheet("dividend_summary")
worksheet.clear()
set_with_dataframe(worksheet, df_dividend_summary)
print("Google Sheet updated: dividend_summary")

# Share Price
df_final["Client Code"] = df_final["Client Name"].str.extract(r"\[([A-Z]{2,5}\d+)\]")
df_final["Client Name"] = df_final["Client Name"].str.replace(
    r"\s*\[[A-Z]{2,5}\d+\]", "", regex=True
)

df_sharePrice = df_final[['ISIN Code', 'ISIN Description', 'Closing Rate']].copy()
df_sharePrice['Date'] = pd.Timestamp.today().date()

output_path = os.path.join(WORK, "Share Price.xlsx")
df_sharePrice.to_excel(output_path, index=False)
upload_file(output_path, OUTPUT_FOLDER_ID,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
print("File saved successfully at:", output_path)

worksheet = spreadsheet.worksheet("Share Price")
worksheet.clear()
set_with_dataframe(worksheet, df_sharePrice)
print("Google Sheet updated: Share Price")

# Holding summary
df_Holding_summary = df_final.groupby('Client Name', as_index=False)['Holding Valuation'].sum()
worksheet = spreadsheet.worksheet("holding_details")
worksheet.clear()
set_with_dataframe(worksheet, df_Holding_summary)
print("Google Sheet updated: holding_details")


# ============================================================================
#  LEDGER PROCESSING  (Cash Details)
#  ----------------------------------
#  FIX 1: Whitespace normalization on client headers BEFORE ffill — without
#         this, "RENUKA PATIL" and "RENUKA  PATIL" become two different
#         clients and the per-client Excel writer sees only half the rows.
#
#  FIX 2: Cash filter now includes RAZORPAY pay-ins, Inter-Segment transfers,
#         and Late-Payment charges — not just HDFC/ICICI "BANK CLIENT A/C"
#         entries. Previously ~60 Razorpay rows across ~49 clients were
#         silently dropped, so small-ticket SIP clients showed zero credits.
# ============================================================================

def standardize_ledger_columns(df):
    """Rename columns to a common format regardless of source file format."""
    rename_map = {}
    for col in df.columns:
        col_clean = (col.replace("_x000D_", "")
                        .replace("\n", " ").replace("\r", " ").strip())
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
    drop_cols = [c for c in df.columns
                 if c in ['Branch', 'Sett#', 'DrCr'] or c.startswith('Unnamed')]
    df = df.drop(columns=drop_cols, errors='ignore')
    return df


# Load and standardize all four ledger files
df_led1 = standardize_ledger_columns(pd.read_excel(LED1_PATH, header=1))
df_led2 = standardize_ledger_columns(pd.read_excel(LED2_PATH, header=1))
df_led3 = standardize_ledger_columns(pd.read_excel(LED3_PATH, header=1))
df_led4 = standardize_ledger_columns(pd.read_excel(LED4_PATH, header=1))

df_ledger = pd.concat([df_led1, df_led2, df_led3, df_led4],
                      axis=0, ignore_index=True)

# ─── FIX 1: Client header detection + WHITESPACE NORMALIZATION ───
# Previously: raw headers kept double-spaces → "RENUKA  PATIL" ≠ "RENUKA PATIL"
df_ledger["Client_Name"] = np.where(
    df_ledger["Date"].astype(str).str.contains(r"\[.*\]", na=False),
    df_ledger["Date"], np.nan
)
df_ledger["Client_Name"] = (
    df_ledger["Client_Name"]
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)   # collapse multi-space into single
)
df_ledger["Client_Name"] = df_ledger["Client_Name"].ffill()

# Ensure numeric types on amount columns for all downstream logic
df_ledger["Debit"]   = pd.to_numeric(df_ledger["Debit"],   errors='coerce').fillna(0)
df_ledger["Credit"]  = pd.to_numeric(df_ledger["Credit"],  errors='coerce').fillna(0)
df_ledger["Balance"] = pd.to_numeric(df_ledger["Balance"], errors='coerce')

# ─── FIX 2: Broadened cash filter ───
# Include: bank client accounts + Razorpay + inter-segment + late payment
cash_filter = (
    df_ledger["Details"].astype(str).str.contains(
        r"\bBANK\b.*\bCLIENT\b.*\b(?:A\s*/\s*C|AC|ACCOUNT)\b",
        case=False, na=False, regex=True
    )
    | df_ledger["Details"].astype(str).str.contains(r"RAZORPAY",
                                                     case=False, na=False)
    | df_ledger["Details"].astype(str).str.contains(r"Inter\s+Segment",
                                                     case=False, na=False)
    | df_ledger["Details"].astype(str).str.contains(r"Late\s+Payment",
                                                     case=False, na=False)
)
df_cash = df_ledger[cash_filter].copy()

# Extract Client Code and clean Client Name
df_cash["Client Code"] = df_cash["Client_Name"].str.extract(r"\[([A-Z0-9]+)\]")
df_cash["Client Name"] = (
    df_cash["Client_Name"]
    .str.replace(r"\s*\[[A-Z0-9]+\]", "", regex=True)
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)
df_cash = df_cash.drop(columns=["Client_Name"])

print(f"Cash details loaded: {len(df_cash)} rows for "
      f"{df_cash['Client Name'].nunique()} clients "
      f"(ledger has {df_ledger['Client_Name'].nunique()} unique clients total)")


# ============================================================================
#  DEBIT BALANCE SUMMARY  (payouts from df_led4 / FY 2026-27 only)
# ============================================================================

# If df_led4 somehow lost the original header, restore positional names
if df_led4.columns[0] != 'Date':
    df_led4.columns = (['Date', 'Branch', 'Sett', 'Details',
                        'Debit', 'Credit', 'Balance', 'DrCr', 'Extra']
                       [:len(df_led4.columns)])

client_pattern = re.compile(r'^([A-Z][A-Z\s\.]+?\s*\[[A-Z0-9]+\])\s*$')

def extract_client(val):
    if pd.isna(val) or not isinstance(val, str):
        return None
    m = client_pattern.match(val.strip())
    return m.group(1).strip() if m else None

df_led4['_client_tag'] = df_led4['Date'].apply(extract_client)
# Normalize whitespace on the raw header too, to stay consistent with FIX 1
df_led4['_client_tag'] = (
    df_led4['_client_tag']
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)
df_led4['_client'] = df_led4['_client_tag'].ffill()

df_led4['_date']   = pd.to_datetime(df_led4['Date'], format='%d/%m/%Y', errors='coerce')
df_led4['_debit']  = pd.to_numeric(df_led4['Debit'],  errors='coerce').fillna(0)
df_led4['_credit'] = pd.to_numeric(df_led4['Credit'], errors='coerce').fillna(0)

today         = pd.Timestamp(datetime.today().date())
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

summary_df = (
    payouts.groupby('_client')
           .agg(**{
               'Total Debit to Bank': ('_debit', 'sum'),
               'No. of Transactions': ('_debit', 'count'),
               'Last Payout Date':    ('_date',  'max')
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

try:
    master_ws = spreadsheet.worksheet("debit balance")
    master_ws.clear()
except gspread.exceptions.WorksheetNotFound:
    master_ws = spreadsheet.add_worksheet(title="debit balance", rows=1000, cols=10)
set_with_dataframe(master_ws, summary_df)

df_led4.drop(columns=['_client_tag', '_client', '_date', '_debit', '_credit'],
             inplace=True, errors='ignore')
print(f"\n✅ {len(summary_df)} clients written to 'debit balance' sheet")


# ============================================================================
#  PER-CLIENT EXCEL:  Holdings + Cash Details
# ============================================================================

for client_name, client_df in df_final.groupby("Client Name"):

    folder_name = re.sub(r"\s+", " ", clean_name(client_name).strip())

    client_folder_id = find_folder_id(folder_name, CLIENTS_PARENT_ID)
    if not client_folder_id:
        print(f"Folder not found for client: {client_name}")
        continue

    local_file = os.path.join(WORK, f"{folder_name}_HOLDINGS.xlsx")

    # This client's cash rows (from our filtered, fixed df_cash)
    client_cash = df_cash[df_cash["Client Name"] == client_name].copy()

    with pd.ExcelWriter(local_file, engine="openpyxl") as writer:
        client_df.to_excel(writer, sheet_name="Holdings", index=False)
        if not client_cash.empty:
            client_cash.to_excel(writer, sheet_name="Cash Details", index=False)

    upload_file(local_file, client_folder_id,
                drive_name=f"{folder_name}_HOLDINGS.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Sanity log: how many cash rows, net credit-debit captured
    if not client_cash.empty:
        net = float(client_cash["Credit"].sum()) - float(client_cash["Debit"].sum())
        print(f"  {client_name}: {len(client_cash)} cash rows, net ₹{net:+,.2f}")
    else:
        print(f"  {client_name}: 0 cash rows (no bank/razorpay/inter-seg/late-fee activity)")

print("\nAll client files written.")
