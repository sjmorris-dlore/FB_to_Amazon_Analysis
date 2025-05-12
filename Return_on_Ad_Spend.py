import pandas as pd
import os
import glob
from datetime import datetime, timedelta

# Compute analysis window: prior Saturday to Saturday
today = datetime.today()
last_saturday = today - timedelta(days=(today.weekday() + 2) % 7 + 7)
start_date = last_saturday - timedelta(days=7)

week_str = f"{start_date.date()}_to_{last_saturday.date()}"
year_str = str(last_saturday.year)
month_str = f"{last_saturday.month:02d}"

print(f"Analyzing week: {week_str}")

# Folder structure: ./ad_data/YYYY/MM/
DATA_FOLDER = f'./ad_data/{year_str}/{month_str}/'

# Build file paths dynamically
fb_file = f"{DATA_FOLDER}/298049981597293-Ads_{week_str}.csv"
sales_file = f"{DATA_FOLDER}/KDP_Royalties_Estimator_{week_str}.xlsx"

# Amazon Attribution file pattern (wildcard match)
attr_file_pattern = f"{DATA_FOLDER}/Amazon_Attribution_campaign_adgroups_*_{week_str}.csv"

# Print file search paths
print("Looking for these files:")
print(f" - Facebook Ads: {fb_file}")
print(f" - Amazon Attribution pattern: {attr_file_pattern}")
print(f" - Amazon Sales: {sales_file}")

# File existence checks
missing_files = []
if not os.path.exists(fb_file):
    missing_files.append(fb_file)
if not os.path.exists(sales_file):
    missing_files.append(sales_file)

attr_files = glob.glob(attr_file_pattern)
if not attr_files:
    missing_files.append(f"Amazon Attribution files matching: {attr_file_pattern}")

if missing_files:
    print("\nERROR: The following required files were not found:")
    for mf in missing_files:
        print(f" - {mf}")
    exit(1)

# Load and process FB data
fb_data = pd.read_csv(fb_file)
fb_clicks = fb_data['Results'].sum()
fb_spend = fb_data['Amount spent (USD)'].sum()

# Load and aggregate Amazon Attribution data
attr_clicks_total = 0
attr_sales_total = 0
attr_kenp_total = 0
attr_kenp_royalties = 0.0

for file in attr_files:
    print(f"Processing Amazon Attribution file: {file}")
    attr_data = pd.read_csv(file)
    attr_data['Click-throughs'] = attr_data['Click-throughs'].replace(',', '', regex=True).astype(float)
    attr_data['Purchases'] = attr_data['Purchases'].replace(',', '', regex=True).fillna(0).astype(int)
    attr_data['KENP read'] = attr_data['KENP read'].replace(',', '', regex=True).fillna(0).astype(int)
    attr_data['Estimated KENP royalties'] = attr_data['Estimated KENP royalties'].replace('[\$,]', '', regex=True).replace(',', '', regex=True).astype(float)
    attr_clicks_total += attr_data['Click-throughs'].sum()
    attr_sales_total += attr_data['Purchases'].sum()
    attr_kenp_total += attr_data['KENP read'].sum()
    attr_kenp_royalties += attr_data['Estimated KENP royalties'].sum()

# Estimate books read from KENP (assuming 400 pages per book)
attr_kenp_books = attr_kenp_total / 400

# Load and process Amazon Sales data
sales_data = pd.read_excel(sales_file, sheet_name='Combined Sales')
sales_data['Royalty Date'] = pd.to_datetime(sales_data['Royalty Date'], errors='coerce')
sales_data = sales_data[(sales_data['Royalty Date'] >= start_date) & (sales_data['Royalty Date'] <= last_saturday)]
sales_units = sales_data['Net Units Sold'].sum()
sales_royalties = sales_data['Royalty'].sum()

# Compute ROAS
roas_attr = attr_sales_total / fb_spend if fb_spend else 0
roas_total = (sales_royalties + attr_kenp_royalties) / fb_spend if fb_spend else 0

# Compute blended ROAS (including KENP books as equivalent to sales)
kenp_book_sales_multiplier = 0.5  # Example: treat each KENP book read as half a sale
blended_sales = attr_sales_total + (attr_kenp_books * kenp_book_sales_multiplier)
roas_blended = blended_sales / fb_spend if fb_spend else 0

# Output summary
summary = pd.DataFrame([{
    'Week': f"{start_date.date()} to {last_saturday.date()}",
    'FB_Clicks': fb_clicks,
    'Attributed_Sales': attr_sales_total,
    'Attributed_KENP_Pages': attr_kenp_total,
    'Attributed_KENP_Books': attr_kenp_books,
    'Attributed_KENP_Royalties': attr_kenp_royalties,
    'Total_Units_Sold': sales_units,
    'Total_Royalties_USD': sales_royalties,
    'Spend_USD': fb_spend,
    'ROAS_Attributed': roas_attr,
    'ROAS_Total': roas_total,
    'ROAS_Blended': roas_blended
}])

# Save to Excel (append or create)
output_file = './Weekly_Ad_Performance_Tracker.xlsx'
try:
    existing = pd.read_excel(output_file)
    combined = pd.concat([existing, summary], ignore_index=True)
except FileNotFoundError:
    combined = summary

combined.to_excel(output_file, index=False)
print(f"Summary appended to {output_file}")
