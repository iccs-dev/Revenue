import pandas as pd
import os
from datetime import datetime

# === Determine dynamic month and year ===
today = datetime.today()
month_abbr = today.strftime("%b").lower()
year_full = today.strftime("%Y")
month_year = f"{month_abbr}{year_full}"

# === Input file paths ===
combine_files = [
    fr"D:\Revenue\media\combined\combine_{month_year}.csv",
    fr"D:\Revenue\media\processed_combined\processed_{month_year}.csv"
]

downtime_path = fr"D:\Revenue\media\downtime_combined\downtime_{month_year}.csv"

# === Output path ===
output_dir = r"D:\Revenue\media\final_login"
output_file = os.path.join(output_dir, f"logins_{month_year}.csv")

os.makedirs(output_dir, exist_ok=True)

# === Read and merge combine + processed_combined ===
combined_df_list = []

for file in combine_files:
    if os.path.exists(file):
        print(f"📁 Reading source: {file}")
        df = pd.read_csv(file)
        df.columns = df.columns.str.strip()
        combined_df_list.append(df)
    else:
        print(f"⚠️ Source missing: {file}")

if not combined_df_list:
    raise FileNotFoundError("❌ No main source files available. Cannot continue.")

combine_df = pd.concat(combined_df_list, ignore_index=True)

# === Check downtime file ===
if not os.path.exists(downtime_path):
    print(f"⚠️ No downtime found for month {month_year}. Proceeding without downtime merge.")
    combine_df.to_csv(output_file, index=False)
    print(f"✅ Final file generated: {output_file}")
    exit()

# === Load downtime ===
print(f"🕒 Reading downtime: {downtime_path}")
downtime_df = pd.read_csv(downtime_path)
downtime_df.columns = downtime_df.columns.str.strip()
downtime_df = downtime_df.rename(columns={"my_process": "Process"})

# === Create lookup map ===
downtime_map = (
    downtime_df.groupby(["EmpCode", "Date", "Process"])["Minutes"]
    .sum()
    .to_dict()
)

# === Apply downtime to combined data ===
updated_rows = []
for idx, row in combine_df.iterrows():
    key = (row["EmpCode"], row["Date"], row["Process"])
    extra_minutes = downtime_map.get(key, 0)
    row["Minutes"] = row["Minutes"] + extra_minutes
    updated_rows.append(row)

updated_df = pd.DataFrame(updated_rows)

# === Add downtime rows missing in combine ===
combine_keys = set(zip(updated_df["EmpCode"], updated_df["Date"], updated_df["Process"]))

extra_rows = [
    row for idx, row in downtime_df.iterrows()
    if (row["EmpCode"], row["Date"], row["Process"]) not in combine_keys
]

if extra_rows:
    extra_df = pd.DataFrame(extra_rows)[["EmpCode", "Date", "Minutes", "Process"]]
    final_df = pd.concat(
        [updated_df[["EmpCode", "Date", "Minutes", "Process"]], extra_df],
        ignore_index=True
    )
else:
    final_df = updated_df



# ============================================================
# STEP: MERGE LOGICAL PROCESSES (Go Noise & ZET)
# ============================================================

# --- Define process groups ---
go_noise_processes = ["Go_Noise_IB", "Go_Noise_OB"]
kpn_processes = ["KPN_Fresh_CC", "KPN"]
zet_processes = ["ZET_Inbound", "ZET_Inbound_Partner", "ZET_Outbound"]
mpokket_processes = ["Mpokket_Onboarding_SE", "Mpokket_Onboarding_ST"]
saving_processes = ["I_Pru_Saving", "I_Pru_Saving_RM"]
protection_processes = ["I_Pru_Protection", "I_Pru_Protection_RM"]
ipru_processes = ["I-PRU_Mumbai_APR"]

# --- Create a copy to avoid side effects ---
df = final_df.copy()

# --- Normalize Go Noise ---
df.loc[df["Process"].isin(go_noise_processes), "Process"] = "GO_NOISE"

# --- Normalize ZET ---
df.loc[df["Process"].isin(zet_processes), "Process"] = "ZET"

# --- Normalize Mpokket ---
df.loc[df["Process"].isin(mpokket_processes), "Process"] = "Mpokket"

# --- Normalize KPN ---
df.loc[df["Process"].isin(kpn_processes), "Process"] = "KPN"

# --- Normalize I_Pru_Saving ---
df.loc[df["Process"].isin(saving_processes), "Process"] = "I-PRU Noida"

# --- Normalize I_Pru_Protection ---
df.loc[df["Process"].isin(protection_processes), "Process"] = "I-PRU Protection"

# --- Normalize I_Pru_Mumbai ---
df.loc[df["Process"].isin(ipru_processes), "Process"] = "I-PRU Mumbai"


# --- Aggregate minutes by EmpCode + Date + Process ---
final_df = (
    df.groupby(["EmpCode", "Date", "Process"], as_index=False)["Minutes"]
      .sum()
)

print("✅ Go_Noise, ZET, KPN, I_Pru_Saving, I_Pru_Protection and Mpokket processes merged successfully")

# ============================================================
# STEP: MAP PROCESS PER EMPCODE FROM proc.csv
# ============================================================

proc_file = r"D:\Revenue\media\map\proc.csv"

if os.path.exists(proc_file):
    print("🔄 Applying Process mapping from proc.csv")

    # Read proc.csv (no header)
    proc_df = pd.read_csv(proc_file, header=None, names=["EmpCode", "Mapped_Process"])
    proc_df["EmpCode"] = proc_df["EmpCode"].str.strip()
    proc_df["Mapped_Process"] = proc_df["Mapped_Process"].str.strip()

    # Create lookup dictionary
    proc_map = dict(zip(proc_df["EmpCode"], proc_df["Mapped_Process"]))

    # Apply mapping ONLY where Process == ICAI
    # mask = final_df["Process"] == "ICAI"
    mask = final_df["Process"].str.upper().isin(["ICAI", "POSHAN_HELPLINE", "L&T SME", "NAMMA_YATRI", "BAJAJ_ALLIANZ-NOIDA", "MAX_LIFE-SDPL"])

    final_df.loc[mask, "Process"] = final_df.loc[mask, "EmpCode"].map(proc_map)\
        .fillna(final_df.loc[mask, "Process"])

    print("✅ Process mapping applied successfully")

else:
    print("⚠️ proc.csv not found. Mapping skipped.")

# === Save output ===
final_df.to_csv(output_file, index=False)

print(f"✅ Final merged login file saved at: {output_file}")
