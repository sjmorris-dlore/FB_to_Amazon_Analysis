import pandas as pd
import os
import glob
from datetime import datetime, timedelta

kenp_book_sales_multiplier = 1.97  # Example: treat each KENP book read as half a sale
profit_per_ebook = 2.71
kenp_pages_per_book = 450

# User-defined list of date ranges
analysis_ranges = [
    ('2025-04-13', '2025-04-19'),
    ('2025-04-20', '2025-04-26'),
    ('2025-04-27', '2025-05-03'),
    ('2025-05-02', '2025-05-09')  # <-- Add more date ranges here
]

# Clean out old Per_Ad_Performance_Tracker data
per_ad_output_file = './Per_Ad_Performance_Tracker.xlsx'
if os.path.exists(per_ad_output_file):
    os.remove(per_ad_output_file)
    print(f"Deleted old {per_ad_output_file} to start fresh.")

for start_date_input, end_date_input in analysis_ranges:
    try:
        start_date = datetime.strptime(start_date_input, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_input, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: Invalid date format for range {start_date_input} to {end_date_input}. Please use YYYY-MM-DD.")
        continue

    week_str = f"{start_date.date()}_to_{end_date.date()}"
    year_str = str(end_date.year)
    month_str = f"{end_date.month:02d}"

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

    # Load and process FB per-ad data
    fb_data = pd.read_csv(fb_file)
    fb_data['Results'] = fb_data['Results'].replace(',', '', regex=True).fillna(0).astype(int)
    fb_data['Amount spent (USD)'] = fb_data['Amount spent (USD)'].replace(',', '', regex=True).fillna(0).astype(float)
    if 'Reach' in fb_data.columns:
        fb_data['Reach'] = fb_data['Reach'].replace(',', '', regex=True).fillna(0).astype(int)
    if 'Impressions' in fb_data.columns:
        fb_data['Impressions'] = fb_data['Impressions'].replace(',', '', regex=True).fillna(0).astype(int)
    if 'Cost per result' in fb_data.columns:
        fb_data['Cost per result'] = fb_data['Cost per result'].replace('[\$,]', '', regex=True).replace(',', '', regex=True).fillna(0).astype(float)
    fb_clicks = fb_data['Results'].sum()
    fb_spend = fb_data['Amount spent (USD)'].sum()

    # Load mapping file for Attribution Ad group to FB Ad Name
    mapping_file = './ad_data/Attribution_to_FB_Ad_Mapping.csv'
    if not os.path.exists(mapping_file):
        print(f"ERROR: Mapping file not found: {mapping_file}")  # Fixed newline escape
        exit(1)

    mapping_data = pd.read_csv(mapping_file)

    # Process Amazon Attribution data per Ad group
    attr_data_list = []
    for file in attr_files:
        print(f"Processing Amazon Attribution file: {file}")
        attr_data = pd.read_csv(file)
        attr_data['Click-throughs'] = attr_data['Click-throughs'].replace(',', '', regex=True).astype(float)
        attr_data['Purchases'] = attr_data['Purchases'].replace(',', '', regex=True).fillna(0).astype(int)
        attr_data['KENP read'] = attr_data['KENP read'].replace(',', '', regex=True).fillna(0).astype(int)
        attr_data['Estimated KENP royalties'] = attr_data['Estimated KENP royalties'].replace('[\$,]', '', regex=True).replace(',', '', regex=True).astype(float)
        attr_data_list.append(attr_data)

    attr_all_data = pd.concat(attr_data_list, ignore_index=True)
    print("Attribution data columns:", attr_all_data.columns.tolist())

    # Merge with mapping to FB Ad Names
    try:
        attr_mapped = attr_all_data.merge(mapping_data, left_on='Ad group', right_on='Ad group', how='left')

    except KeyError as e:
        print(f"ERROR: Missing expected column in Attribution data: {e}")
        print("Attribution data columns:", attr_all_data.columns.tolist())
        exit(1)

    if (0):
        # Warn about unmapped Ad groups
        unmapped = attr_all_data[~attr_all_data['Ad group'].isin(mapping_data['Ad Name (FB)'])]['Ad group'].unique()
        if len(unmapped) > 0:
            print("WARNING: The following Ad groups from Attribution did not map to any FB Ad Name:")
            for ad in unmapped:
                print(f" - {ad}")

    # Aggregate per FB Ad Name
    attr_per_ad = attr_mapped.groupby('Ad Name (FB)').agg({
        'Click-throughs': 'sum',
        'Purchases': 'sum',
        'KENP read': 'sum',
        'Estimated KENP royalties': 'sum'
    }).reset_index()

    # Warn about FB Ads with no Attribution data
    fb_names_with_attr = attr_per_ad['Ad Name (FB)'].dropna().unique()
    unmatched_fb_ads = fb_data[~fb_data['Ad name'].isin(fb_names_with_attr)]['Ad name'].unique()
    if len(unmatched_fb_ads) > 0:
        print("WARNING: The following FB Ads have no matching Attribution data:")
        for ad in unmatched_fb_ads:
            print(f" - {ad}")

    # Also aggregate totals for global metrics
    attr_clicks_total = attr_mapped['Click-throughs'].sum()
    attr_sales_total = attr_mapped['Purchases'].sum()
    attr_kenp_total = attr_mapped['KENP read'].sum()
    attr_kenp_royalties = attr_mapped['Estimated KENP royalties'].sum()

    # Load and process Amazon Sales data
    sales_data = pd.read_excel(sales_file, sheet_name='Combined Sales')
    sales_data['Royalty Date'] = pd.to_datetime(sales_data['Royalty Date'], errors='coerce')
    sales_data = sales_data[(sales_data['Royalty Date'] >= start_date) & (sales_data['Royalty Date'] <= end_date)]
    sales_units = sales_data['Net Units Sold'].sum()
    sales_royalties = sales_data['Royalty'].sum()

    # Merge with FB per-ad spend and clicks
    fb_per_ad = fb_data[['Ad name', 'Results', 'Amount spent (USD)']]
    per_ad_summary = fb_per_ad.merge(attr_per_ad, left_on='Ad name', right_on='Ad Name (FB)', how='left')

    # Compute per-ad ROAS metrics
    per_ad_summary['KENP Books'] = per_ad_summary['KENP read'] / kenp_pages_per_book
    per_ad_summary['ROAS_Attributed'] = per_ad_summary['Purchases'] / per_ad_summary['Amount spent (USD)']
    per_ad_summary['ROAS_Blended'] = (per_ad_summary['Purchases'] + (per_ad_summary['KENP Books'] * kenp_book_sales_multiplier)) / per_ad_summary['Amount spent (USD)']
    per_ad_summary['ROAS_Total'] = (per_ad_summary['Estimated KENP royalties']) / per_ad_summary['Amount spent (USD)']

    # Compute sum of attributed royalties (ebook + KENP)
    per_ad_summary['Attributed_Royalties'] = per_ad_summary['Purchases'] * profit_per_ebook + (per_ad_summary['KENP Books'] * kenp_book_sales_multiplier)

    # Removed redundant Amazon Sales data load

    # Add summary line comparing attributed royalties to KDP total
    attributed_royalties_sum = per_ad_summary['Attributed_Royalties'].sum()
    summary_row = pd.DataFrame([{
        'Ad name': 'TOTAL ATTRIBUTED',
        'Attributed_Royalties': attributed_royalties_sum,
        'Total_KDP_Royalties': sales_royalties
    }])

    per_ad_summary = pd.concat([per_ad_summary, summary_row], ignore_index=True)

    # Save per-ad summary to Excel
    per_ad_summary['Week'] = f"{start_date.date()} to {end_date.date()}"

    per_ad_output_file = './Per_Ad_Performance_Tracker.xlsx'
    try:
        existing_per_ad = pd.read_excel(per_ad_output_file)
        per_ad_combined = pd.concat([existing_per_ad, per_ad_summary], ignore_index=True)
    except FileNotFoundError:
        per_ad_combined = per_ad_summary

    per_ad_combined['Rolling_3Wk_ROAS_Blended'] = per_ad_combined.groupby('Ad name')['ROAS_Blended'].transform(lambda x: x.rolling(window=3, min_periods=1).mean())

    per_ad_combined.to_excel(per_ad_output_file, index=False)
    print(f"Per-ad summary saved to {per_ad_output_file}")

    # Existing global totals already handled above. Removed redundant loop.

    # Estimate books read from KENP 
    attr_kenp_books = attr_kenp_total / kenp_pages_per_book

    # Compute ROAS
    roas_attr = attr_sales_total / fb_spend if fb_spend else 0
    roas_total = (sales_royalties + attr_kenp_royalties) / fb_spend if fb_spend else 0

    # Compute blended ROAS (including KENP books as equivalent to sales)
    blended_sales = attr_sales_total * profit_per_ebook + (attr_kenp_books * kenp_book_sales_multiplier)
    roas_blended = blended_sales / fb_spend if fb_spend else 0

    # Output summary
    summary = pd.DataFrame([{
        'Week': f"{start_date.date()} to {end_date.date()}",
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
