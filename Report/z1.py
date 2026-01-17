import os
import math
import json
import pandas as pd
from datetime import datetime, timedelta
from calendar import monthrange

# --- Step 1: Determine previous month ---
today = datetime.today()
first_day_current_month = today.replace(day=1)
last_day_previous_month = first_day_current_month
month_name = last_day_previous_month.strftime("%b").lower()
year_full = last_day_previous_month.strftime("%Y")
month_label = last_day_previous_month.strftime("%b")

# Correct days in month
year = last_day_previous_month.year
month = last_day_previous_month.month
_, days_in_month = monthrange(year, month)

print(f"📅 Processing for month: {month_name} ({days_in_month} days)\n")

# --- Input paths ---
map_path = r"D:\Revenue\media\map\map.csv"
meta_path = r"D:\Revenue\media\map\meta.csv"
cost_path = r"D:\Revenue\media\map\cost.csv"

login_file = fr"D:\Revenue\media\final_login\logins_{month_name}{year_full}.csv"
output_path = fr"D:\Revenue\media\report\revenue_{month_name}{year_full}.csv"

# --- Step 2: Read mapping and meta data ---
map_df = pd.read_csv(map_path)
meta_df = pd.read_csv(meta_path)
cost_df = pd.read_csv(cost_path)

required_map_cols = {'Process', 'Location', 'Billable', 'Cost1', 'ExtraBilling'}
required_meta_cols = {'Process', 'Month', 'FTE Cap', 'Mandays'}
required_cost_cols = {'EmpCode', 'Category', 'Process', 'Month', 'Cost'}

if not required_map_cols.issubset(map_df.columns):
    raise ValueError(f"map.csv must contain columns: {required_map_cols}")
if not required_meta_cols.issubset(meta_df.columns):
    raise ValueError(f"meta.csv must contain columns: {required_meta_cols}")
if not required_cost_cols.issubset(cost_df.columns):
    raise ValueError(f"cost.csv must contain columns: {required_cost_cols}")

# Normalize
cost_df['Category'] = cost_df['Category'].str.strip()
cost_df['Process'] = cost_df['Process'].str.strip()
cost_df['Month'] = cost_df['Month'].str.strip().str.lower()

meta_df['Process'] = meta_df['Process'].str.strip()

# Filter meta for this month
meta_df = meta_df[meta_df['Month'].str.lower() == month_label.lower()]

# --- Step 3: Read login data ---
if not os.path.exists(login_file):
    raise FileNotFoundError(f"Login file not found: {login_file}")

login_df = pd.read_csv(login_file)
required_login_cols = {'EmpCode', 'Date', 'Process', 'Minutes'}
if not required_login_cols.issubset(login_df.columns):
    raise ValueError(f"logins file must contain columns: {required_login_cols}")

# Convert date mm-dd-yyyy → yyyy-mm-dd
login_df['Date'] = pd.to_datetime(login_df['Date'], format='%m-%d-%Y', errors='coerce')
# login_df.dropna(subset=['Date'], inplace=True)
# login_df['Date'] = login_df['Date'].dt.strftime('%Y-%m-%d')

# --- Step 3A: Robust Date Parsing ---
# login_df['Date'] = pd.to_datetime(
#     login_df['Date'],
#     dayfirst=True,      # handles dd-mm & mm-dd safely
#     errors='coerce'
# )

# ❌ Hard fail if invalid dates exist
# bad_dates = login_df[login_df['Date'].isna()]
# if not bad_dates.empty:
#     raise ValueError(
#         f"❌ Invalid Date values detected in login file:\n"
#         f"{bad_dates[['EmpCode', 'Date']].head()}"
#     )
# --- Step 3A: Handle invalid dates (log & continue) ---
bad_dates = login_df[login_df['Date'].isna()]

if not bad_dates.empty:
    fail_path = fr"D:\Revenue\media\fail_login\fail_logins_{month_name}{year_full}.csv"
    os.makedirs(os.path.dirname(fail_path), exist_ok=True)

    bad_dates.to_csv(fail_path, index=False)
    print(f"⚠️ Invalid dates found. Logged to: {fail_path}")

# Remove invalid rows and continue
login_df = login_df[login_df['Date'].notna()]


# --- Step 3B: Restrict strictly to processing month ---
login_df = login_df[
    (login_df['Date'].dt.month == month) &
    (login_df['Date'].dt.year == year)
]

if login_df.empty:
    raise ValueError(
        f"❌ No login data found for {month_label}-{year_full} after date filtering"
    )

# ❌ Guard against epoch leakage
if (login_df['Date'] < pd.Timestamp('2000-01-01')).any():
    raise ValueError("❌ Epoch / corrupted dates detected in login data")

# Normalize format AFTER validation
login_df['Date'] = login_df['Date'].dt.strftime('%Y-%m-%d')




# --- Step 4: Unique dates ---
date_list = sorted(login_df['Date'].dropna().unique())

# --- Step 5: Process loop ---
rows = []

for _, row in map_df.iterrows():

    process = str(row['Process']).strip()

    # --- UGVCL uplift ---
    cost_multiplier = 1.075 if process.upper() == "UGVCL" else 1.0

    location = row['Location']
    cluster_head = row['Cluster Head'] 
    billable = float(row['Billable'])
    # cost1 = float(row['Cost1'])

    raw_cost1 = str(row['Cost1']).strip()

    # --- NEW: Parse ExtraBilling JSON ---
    extra_list = []
    raw_extra = row.get('ExtraBilling', '')

    try:
        if pd.notna(raw_extra) and str(raw_extra).strip() != "":
            extra_list = json.loads(raw_extra)
    except Exception as e:
        print(f"⚠️ Invalid ExtraBilling format for {process}: {e}")

    # Calculate total extra billing
    extra_total = sum(item.get("count", 0) * item.get("cost", 0)  * cost_multiplier for item in extra_list)

    # # Meta
    # meta_row = meta_df[meta_df['Process'].str.strip() == process]
    # if meta_row.empty:
    #     print(f"⚠️ No metadata found for process '{process}' for month {month_label}.")
    #     continue

    # --- MULTI COST MODE ---
    if '$' in raw_cost1:
        # allowed_costs = [float(c) for c in raw_cost1.split('$')]
        allowed_costs = [float(c) * cost_multiplier for c in raw_cost1.split('$')]


        process_cost_df = cost_df[
            (cost_df['Process'] == process) &
            (cost_df['Month'] == month_label.lower()) &
            (cost_df['Cost'].isin(allowed_costs))
        ]

        category_map = {}
        for cat, grp in process_cost_df.groupby('Category'):
            category_map[cat] = {
                "cost": grp['Cost'].iloc[0] * cost_multiplier,
                "empcodes": set(grp['EmpCode'])
            }
        
        # Display-only values
        if len(category_map):
            display_pay = sum(info['cost'] for info in category_map.values()) / len(category_map)
        else : 
            display_pay = 0
        display_fte_cap = 0

        # target = 0
        target = extra_total
        for cat, info in category_map.items():
            meta_row = meta_df[meta_df['Process'] == cat]
            if not meta_row.empty:
                fte_cap_cat = float(meta_row['FTE Cap'].values[0])
                display_fte_cap += fte_cap_cat
                target += fte_cap_cat * info['cost']

        multi_cost_mode = True

        # # Target = sum(category FTE × cost)
        # target = 0
        # for cat, info in category_map.items():
        #     meta_row = meta_df[meta_df['Process'] == cat]
        #     if not meta_row.empty:
        #         target += float(meta_row['FTE Cap'].values[0]) * info['cost']

    else:
        cost1 = float(raw_cost1) * cost_multiplier
        meta_row = meta_df[meta_df['Process'] == process]
        if meta_row.empty:
            continue
        fte_cap = float(meta_row['FTE Cap'].values[0])
        mandays = float(meta_row['Mandays'].values[0])
        # target = fte_cap * cost1
        target = (fte_cap * cost1) + extra_total
        display_pay = cost1
        display_fte_cap = fte_cap
        multi_cost_mode = False

    process_df = login_df[login_df['Process'].str.strip() == process]
    if process_df.empty:
        continue

    cumulative_billable_revenue = 0

    for date_str in date_list:
        daily_df = process_df[process_df['Date'] == date_str]

        revenue = 0
        billable_revenue = 0

        if multi_cost_mode:

            for cat, info in category_map.items():
                cat_df = daily_df[daily_df['EmpCode'].isin(info['empcodes'])]

                empcount = 0
                for _, emp in cat_df.iterrows():
                    if emp['Minutes'] >= billable:
                        empcount += 1
                    elif emp['Minutes'] >= billable / 2:
                        empcount += 0.5

                empcount = round(empcount, 2)

                meta_cat = meta_df[meta_df['Process'] == cat]
                if meta_cat.empty:
                    continue

                fte_cap_cat = float(meta_cat['FTE Cap'].values[0])
                mandays = float(meta_cat['Mandays'].values[0])
                billable_count = min(empcount, fte_cap_cat)

                revenue += math.ceil((empcount * info['cost']) / mandays) if empcount else 0
                billable_revenue += math.ceil((billable_count * info['cost']) / mandays) if billable_count else 0

        else:
            empcount = 0
            for _, emp in daily_df.iterrows():
                if emp['Minutes'] >= billable:
                    empcount += 1
                elif emp['Minutes'] >= billable / 2:
                    empcount += 0.5

            empcount = round(empcount, 2)
            revenue = math.ceil((empcount * cost1) / mandays) if empcount else 0
            billable_revenue = math.ceil((min(empcount, fte_cap) * cost1) / mandays) if empcount else 0

        cumulative_billable_revenue += billable_revenue
        daily_target = target / days_in_month
        deficit = round((daily_target - revenue) / daily_target, 3) if daily_target else 0

        rows.append({
            "Date": datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y"),
            "Process": process,
            "Location": location,
            "Cluster Head": cluster_head,
            "Pay": round(display_pay, 2),
            "Revenue": revenue,
            "Billable Minutes": billable,
            "Billable FTE cap": display_fte_cap,
            "Billable Revenue": billable_revenue,
            "Mandays": mandays,
            "MTD": cumulative_billable_revenue,
            "Target Revenue": target,
            "Defecit": deficit
        })


# --- Step 6: Output DF ---
df_out = pd.DataFrame(rows)

# --- Step 7: Save CSV ---
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df_out.to_csv(output_path, index=False)

print(f"\n✅ Revenue report generated successfully at:\n{output_path}")
