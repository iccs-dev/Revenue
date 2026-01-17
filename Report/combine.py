import os
import pandas as pd
from datetime import datetime

# ===== CONFIG =====
MAP_FILE = r"D:\Revenue\media\map\map.csv"
SOURCE_BASE = r"C:\Users\ICCSADMIN\Documents\GitHub\Dialer\media"
DEST_DIR = r"D:\Revenue\media\combined"
DOWNTIME_SOURCE = r"D:\Revenue\media\Downtime"
DOWNTIME_DEST = r"D:\Revenue\media\downtime_combined"
PROCESSED_BASE = r"D:\auto\media\processed"
PROCESSED_DEST = r"D:\Revenue\media\processed_combined"

# ===== GET PREVIOUS MONTH RANGE =====
# today = datetime.today()
# first_day_this_month = today.replace(day=1)
# last_month_date = first_day_this_month - pd.Timedelta(days=1)

# month_str = last_month_date.strftime("%m")
# month_name_short = last_month_date.strftime("%b").lower()
# year_str = last_month_date.strftime("%Y")

# start_prev_month = last_month_date.replace(day=1)
# end_prev_month = last_month_date

# ===== GET YESTERDAY'S MONTH RANGE =====
today = datetime.today()
yesterday = today - pd.Timedelta(days=0)

month_str = yesterday.strftime("%m")        # Month number (01–12)
month_name_short = yesterday.strftime("%b").lower()  # Short month (jan, feb, mar..)
year_str = yesterday.strftime("%Y")         # Year (2024)

# Range of entire month of yesterday
start_prev_month = yesterday.replace(day=1)
end_prev_month = yesterday.replace(day=yesterday.day)


print(f"\n📌 Processing data for: {month_name_short.upper()}-{year_str} ({start_prev_month} → {end_prev_month})\n")

# ===== READ PROCESSES =====
# df_map = pd.read_csv(MAP_FILE)
# processes = df_map["Process"].dropna().unique()

# ===== READ PROCESSES =====
df_map = pd.read_csv(MAP_FILE)

map_processes = df_map["Process"].dropna().unique().tolist()

# Extra processes not present in map.csv
extra_processes = [
    "ZET_Inbound",
    "ZET_Inbound_Partner",
    "ZET_Outbound",
    "Go_Noise_IB",
    "Go_Noise_OB"
]

# Merge + remove duplicates
processes = sorted(set(map_processes + extra_processes))

print(f"📌 Total processes considered: {len(processes)}")

# Function to extract latest date from processed file
def extract_date_from_processed(df, file_path):
    """Extracts latest valid date from the 'Date' column (flexible format)."""
    if "Date" not in df.columns:
        print(f"⚠️ Missing 'Date' column → Skipping {file_path}")
        return None

    try:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", dayfirst=True)
    except:
        print(f"❌ Could not convert Date format in: {file_path}")
        return None

    valid_dates = df["Date"].dropna()

    if valid_dates.empty:
        print(f"⚠️ No valid dates found in {file_path}")
        return None

    return valid_dates.max().date()


# ==============================================================
# ========== STEP 1: COMBINE DIALER FILES =======================
# ==============================================================

print("\n===== Step 1: Processing Dialer Data =====\n")

combined_data = []

for process in processes:
    process_dir = os.path.join(SOURCE_BASE, process, "dialer_data")

    if not os.path.exists(process_dir):
        print(f"⚠️ Dialer folder not found for {process} → Skipping")
        continue

    for file in os.listdir(process_dir):
        if file.endswith("_APR.csv") and file.startswith(f"{year_str}-{month_str}"):
            file_path = os.path.join(process_dir, file)
            try:
                df = pd.read_csv(file_path)
                df["Process"] = process
                df["Source"] = "Dialer"
                combined_data.append(df)
                print(f"✅ Dialer file added: {file_path}")
            except Exception as e:
                print(f"❌ Error reading {file_path}: {e}")

if combined_data:
    combined_df = pd.concat(combined_data, ignore_index=True).drop_duplicates()
    os.makedirs(DEST_DIR, exist_ok=True)
    output_file = os.path.join(DEST_DIR, f"combine_{month_name_short}{year_str}.csv")
    combined_df.to_csv(output_file, index=False)
    print(f"\n🎉 Dialer Combined File Saved: {output_file}")
else:
    print("\n⚠️ No Dialer data found for previous month.")


# ==============================================================
# ========== STEP 2: COMBINE DOWNTIME FILES =====================
# ==============================================================

print("\n===== Step 2: Processing Downtime Data =====\n")

downtime_files = [os.path.join(DOWNTIME_SOURCE, f) for f in os.listdir(DOWNTIME_SOURCE) if f.endswith(".csv")]
downtime_data = []

for file in downtime_files:
    try:
        df = pd.read_csv(file)

        if "Date" not in df.columns:
            print(f"⚠️ Missing 'Date' column → Skipping {file}")
            continue

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dropna()
        mask = (df["Date"] >= start_prev_month) & (df["Date"] <= end_prev_month)

        df_filtered = df.loc[mask].copy()

        if not df_filtered.empty:
            df_filtered["Date"] = df_filtered["Date"].dt.strftime("%m-%d-%Y")
            df_filtered["Source"] = "Downtime"
            downtime_data.append(df_filtered)
            print(f"🟢 Downtime Added: {file}")

    except Exception as e:
        print(f"❌ Error reading {file}: {e}")

if downtime_data:
    downtime_combined = pd.concat(downtime_data, ignore_index=True).drop_duplicates()
    os.makedirs(DOWNTIME_DEST, exist_ok=True)
    downtime_output = os.path.join(DOWNTIME_DEST, f"downtime_{month_name_short}{year_str}.csv")
    downtime_combined.to_csv(downtime_output, index=False)
    print(f"\n🎉 Downtime Combined Saved: {downtime_output}")
else:
    print("\n⚠️ No downtime files found for previous month.")


# ==============================================================
# ========== STEP 3: COMBINE PROCESSED FILES ====================
# ==============================================================

print("\n===== Step 3: Processing PROCESSED Files =====\n")

processed_data = []

for process in processes:
    process_dir = os.path.join(PROCESSED_BASE, process)

    if not os.path.exists(process_dir):
        print(f"⚠️ Processed folder missing for {process} → Skipping")
        continue

    print(f"📂 Checking: {process_dir}")

    for file in os.listdir(process_dir):
        if not file.lower().endswith((".csv", ".xlsx", ".xls")):
            continue

        file_path = os.path.join(process_dir, file)

        try:
            df = pd.read_csv(file_path) if file.endswith(".csv") else pd.read_excel(file_path)

            # Extract latest date from file
            extracted_date = extract_date_from_processed(df, file_path)

            if not extracted_date:
                continue

            # Check if extracted date falls in previous month
            if start_prev_month.date() <= extracted_date <= end_prev_month.date():
                df["Date"] = pd.to_datetime(df["Date"], errors="ignore", dayfirst=True)
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df["Date"] = df["Date"].dt.strftime("%m-%d-%Y")
                
                df["Process"] = process
                df["Source"] = "Processed"
                processed_data.append(df)

                print(f"📌 Added (based on inside date): {file_path}")
            else:
                print(f"⏭ Skipped (date not in range): {file_path} → {extracted_date}")

        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")

if processed_data:
    processed_combined = pd.concat(processed_data, ignore_index=True).drop_duplicates()
    os.makedirs(PROCESSED_DEST, exist_ok=True)
    processed_output = os.path.join(PROCESSED_DEST, f"processed_{month_name_short}{year_str}.csv")
    processed_combined.to_csv(processed_output, index=False)
    print(f"\n🎉 Processed Combined Saved: {processed_output}")
else:
    print("\n⚠️ No processed files found for previous month.")

print("\n✔️ Script completed successfully.\n")
