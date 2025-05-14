import pandas as pd
import os
import glob
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

kenp_book_sales_multiplier = 1.97  # per KU book, how much money do we make
profit_per_ebook = 2.71
kenp_pages_per_book = 450

# User-defined list of date ranges
analysis_ranges = [
    ('2025-04-13', '2025-04-19'),
    ('2025-04-20', '2025-04-26'),
    ('2025-04-27', '2025-05-03'),
    ('2025-05-04', '2025-05-10')  # <-- Add more date ranges here
]

# Clean out old Per_Ad_Performance_Tracker data
correlation_file = './Ad_Book_Correlation_Tracker.xlsx'
if os.path.exists(correlation_file):
    os.remove(correlation_file)
    print(f"Deleted old {correlation_file} to start fresh.")
per_ad_output_file = './Per_Ad_Performance_Tracker.xlsx'
if os.path.exists(per_ad_output_file):
    os.remove(per_ad_output_file)
    print(f"Deleted old {per_ad_output_file} to start fresh.")
plotting_file = './Ad_Book_Plotting_Tracker.xlsx'
if os.path.exists(plotting_file):
    os.remove(plotting_file)
    print(f"Deleted old {plotting_file} to start fresh.")

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

    # Convert Book1-Book4 into a 'books' list per row
    mapping_data['books'] = mapping_data[['Book1', 'Book2', 'Book3', 'Book4']].values.tolist()
    mapping_data['books'] = mapping_data['books'].apply(lambda x: [b for b in x if pd.notna(b)])

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

    debug_unmapped_check = True
    if debug_unmapped_check:
        unmapped = attr_all_data[~attr_all_data['Ad group'].isin(mapping_data['Ad group'])]['Ad group'].unique()
        if len(unmapped) > 0:
            print("WARNING: The following Ad groups from Attribution did not map to any Mapping entry:")
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
    # Load Combined Sales sheet to track sales per book
    combined_sales_data = pd.read_excel(sales_file, sheet_name='Combined Sales')
    combined_sales_data['Royalty Date'] = pd.to_datetime(combined_sales_data['Royalty Date'], errors='coerce')
    kenp_sales_data = pd.read_excel(sales_file, sheet_name='KENP')
    kenp_sales_data['Date'] = pd.to_datetime(kenp_sales_data['Date'], errors='coerce')

    # Filter only Amazon.com sales
    combined_sales_data = combined_sales_data[combined_sales_data['Marketplace'] == 'Amazon.com']
    kenp_sales_data = kenp_sales_data[kenp_sales_data['Marketplace'] == 'Amazon.com']

    weekly_book_sales = combined_sales_data[(combined_sales_data['Royalty Date'] >= start_date) & (combined_sales_data['Royalty Date'] <= end_date)]
    weekly_kenp_sales = kenp_sales_data[(kenp_sales_data['Date'] >= start_date) & (kenp_sales_data['Date'] <= end_date)]

    # Aggregate sales per book for the week
    book_sales_summary = weekly_book_sales.groupby('Title').agg({'Net Units Sold': 'sum', 'Royalty': 'sum'}).reset_index()
    kenp_summary = weekly_kenp_sales.groupby('Title').agg({'KENP': 'sum'}).reset_index()

    print('Weekly book sales summary:')

    # For each ad, sum up sales for linked books
    ad_book_sales = []
    for _, row in mapping_data.iterrows():
        ad_name = row['Ad Name (FB)']
        linked_books = row['books']

        sales_for_ad = book_sales_summary[book_sales_summary['Title'].apply(lambda t: any(book in str(t) for book in linked_books))].agg({'Net Units Sold': 'sum', 'Royalty': 'sum'}).fillna(0)
        kenp_for_ad = kenp_summary[kenp_summary['Title'].apply(lambda t: any(book in str(t) for book in linked_books))].agg({'KENP': 'sum'}).fillna(0)

        ad_book_sales.append({
            'Ad Name (FB)': ad_name,
            'Total Linked Book Units Sold': sales_for_ad['Net Units Sold'] + kenp_for_ad['KENP'] / kenp_pages_per_book,
            'Total Linked Book Royalty': sales_for_ad['Royalty']
        })

    ad_book_sales_df = pd.DataFrame(ad_book_sales)

    # Filter out ads with zero linked book units sold
    ad_book_sales_df = ad_book_sales_df[ad_book_sales_df['Total Linked Book Units Sold'] > 0]
    print('Weekly Ad-Book Sales Summary:')

    # Build dataset entry for plotting
    ad_book_sales_df['Week'] = f"{start_date.date()} to {end_date.date()}"
    ad_book_sales_df['FB_Clicks'] = ad_book_sales_df['Ad Name (FB)'].map(fb_data.set_index('Ad name')['Results']).fillna(0)
    ad_book_sales_df['Click-throughs'] = ad_book_sales_df['FB_Clicks']

    # Append to plotting dataset
    try:
        existing_plot = pd.read_excel(plotting_file)
        ad_book_combined_plot = pd.concat([existing_plot, ad_book_sales_df], ignore_index=True)
    except FileNotFoundError:
        ad_book_combined_plot = ad_book_sales_df

    ad_book_combined_plot.to_excel(plotting_file, index=False)
    print(f"Ad-Book plotting data saved to {plotting_file}")

    # Append to correlation dataset
    try:
        existing_corr = pd.read_excel(correlation_file)
        ad_book_combined = pd.concat([existing_corr, ad_book_sales_df], ignore_index=True)
    except FileNotFoundError:
        ad_book_combined = ad_book_sales_df

    ad_book_combined.to_excel(correlation_file, index=False)
    print(f"Ad-Book correlation data saved to {correlation_file}")
    print(ad_book_sales_df)
    print(book_sales_summary)
    #sales_data = pd.read_excel(sales_file, sheet_name='Combined Sales')
    #sales_data['Royalty Date'] = pd.to_datetime(sales_data['Royalty Date'], errors='coerce')
    #sales_data = sales_data[(sales_data['Royalty Date'] >= start_date) & (sales_data['Royalty Date'] <= end_date)]
    sales_units = weekly_book_sales['Net Units Sold'].sum()
    sales_kenp = weekly_kenp_sales['KENP'].sum() / kenp_pages_per_book
    sales_royalties = weekly_book_sales['Royalty'].sum() + sales_kenp * kenp_book_sales_multiplier

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

    # Correlation analysis after processing all date ranges
if os.path.exists(correlation_file):
    corr_data = pd.read_excel(correlation_file)

    correlations = []
    for ad_name, group in corr_data.groupby('Ad Name (FB)'):
        if group['Click-throughs'].nunique() > 1 and group['Total Linked Book Units Sold'].nunique() > 1:
            corr_value = group['Click-throughs'].corr(group['Total Linked Book Units Sold'])
        else:
            corr_value = None  # Not enough variation for correlation

        correlations.append({
            'Ad Name (FB)': ad_name,
            'Correlation Clicks vs Sales': corr_value,
            'Weeks of Data': group['Week'].nunique()
        })

    correlations_df = pd.DataFrame(correlations)
    with pd.ExcelWriter(correlation_file, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        correlations_df.to_excel(writer, sheet_name='Correlations', index=False)

    print(f"Correlation analysis saved to {correlation_file}")

# Save to Excel (append or create)
    output_file = './Weekly_Ad_Performance_Tracker.xlsx'
    try:
        existing = pd.read_excel(output_file)
        combined = pd.concat([existing, summary], ignore_index=True)
    except FileNotFoundError:
        combined = summary

    combined.to_excel(output_file, index=False)
    print(f"Summary appended to {output_file}")

# Plotting Section
plotting_file = './Ad_Book_Plotting_Tracker.xlsx'
if os.path.exists(plotting_file):
    plot_data = pd.read_excel(plotting_file)

    # Ensure plots directory exists
    os.makedirs('./plots', exist_ok=True)

    # Plot FB Clicks per Ad per Week
    plt.figure(figsize=(12, 6))
    for ad_name in plot_data['Ad Name (FB)'].unique():
        ad_data = plot_data[plot_data['Ad Name (FB)'] == ad_name]
        plt.plot(ad_data['Week'], ad_data['FB_Clicks'], marker='o', label=ad_name)

    plt.title('FB Clicks per Ad per Week')
    plt.xlabel('Week')
    plt.ylabel('FB Clicks')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('./plots/FB_Clicks_per_Ad.png')
    plt.close()

    # Plot Linked Book Sales per Ad per Week
    plt.figure(figsize=(12, 6))
    for ad_name in plot_data['Ad Name (FB)'].unique():
        ad_data = plot_data[plot_data['Ad Name (FB)'] == ad_name]
        plt.plot(ad_data['Week'], ad_data['Total Linked Book Units Sold'], marker='o', label=ad_name)

    plt.title('Linked Book Sales per Ad per Week')
    plt.xlabel('Week')
    plt.ylabel('Units Sold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('./plots/Linked_Book_Sales_per_Ad.png')
    plt.close()

    # Combined Comparison Plot
    melted = plot_data.melt(id_vars=['Ad Name (FB)', 'Week'], value_vars=['FB_Clicks', 'Total Linked Book Units Sold'], var_name='Metric', value_name='Count')

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=melted, x='Week', y='Count', hue='Ad Name (FB)', style='Metric', markers=True, dashes=False)
    plt.title('FB Clicks vs Book Sales per Ad')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('./plots/FB_vs_Book_Sales_per_Ad.png')
    plt.close()

    print("Plots saved in ./plots folder")
