import os
import pandas as pd
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), "athlinks_scraper_project"))
from athlinks_scraper.core import fetch_master_events, get_results

master_id = "15776" # Branford Turkey Trot
print(f"Restoring data for Master ID: {master_id}")

events = fetch_master_events(master_id)
print(f"Found {len(events)} events.")

data_dir = "dashboard/data"
os.makedirs(data_dir, exist_ok=True)

for i, event in enumerate(events):
    year = event['date_str'][:4]
    event_id = event['id']
    print(f"Scraping {year} (Event ID: {event_id})...")
    
    try:
        df = get_results(event_id)
        if not df.empty:
            filename = os.path.join(data_dir, f"scraped_{master_id}_{year}.parquet")
            df.to_parquet(filename, index=False)
            print(f"Saved {filename}")
        else:
            print(f"No results for {year}")
    except Exception as e:
        print(f"Error scraping {year}: {e}")

print("Restoration complete.")
