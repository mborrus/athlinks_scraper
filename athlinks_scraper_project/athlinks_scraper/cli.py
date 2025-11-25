import argparse
import sys
import os
import re
from .core import get_results, extract_event_id, extract_master_id, fetch_master_events

def sanitize_filename(name):
    """
    Sanitizes a string to be safe for filenames.
    """
    # Remove invalid characters
    name = re.sub(r'[<>:"/\\|?*]', '', name)
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    return name

def process_event(event_id, output_dir=None, output_file=None):
    """
    Helper to scrape a single event and save it.
    """
    print(f"Scraping results for Event ID: {event_id}")
    df = get_results(event_id)
    
    if df.empty:
        print(f"No results found for Event ID: {event_id}")
        return

    # Determine output path
    if output_file:
        output_path = output_file
    elif output_dir:
        # Generate filename from Event Name and ID
        event_name = df.iloc[0].get("Event Name", "Unknown_Event")
        specific_event_id = df.iloc[0].get("Event ID", "Unknown_ID")
        safe_name = sanitize_filename(f"{event_name}_{specific_event_id}")
        filename = f"{safe_name}.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
    else:
        output_path = "results.csv"

    df.to_csv(output_path, index=False)
    print(f"Successfully saved {len(df)} rows to {output_path}")

def main():
    parser = argparse.ArgumentParser(description="Scrape Athlinks race results to CSV.")
    parser.add_argument("url", help="The Athlinks event URL or Event ID.")
    parser.add_argument("--output", "-o", help="Output CSV filename.")
    parser.add_argument("--output-dir", "-d", help="Output directory. Filename will be auto-generated from Event Name.")
    parser.add_argument("--all-years", action="store_true", help="If a Master Event URL is provided, scrape all past years.")
    
    args = parser.parse_args()
    
    try:
        # 1. Check if it's a specific event URL
        specific_id = extract_event_id(args.url)
        if specific_id:
            process_event(specific_id, args.output_dir, args.output)
            return

        # 2. Check if it's a master event URL
        master_id = extract_master_id(args.url)
        if master_id:
            print(f"Detected Master Event ID: {master_id}")
            events = fetch_master_events(master_id)
            
            if not events:
                print("No events found for this Master ID.")
                return

            if args.all_years:
                print(f"Found {len(events)} events. Scraping all years...")
                for event in events:
                    print(f"Processing {event['name']} ({event['date_str']})...")
                    try:
                        process_event(event['id'], args.output_dir, args.output)
                    except Exception as e:
                        print(f"Failed to scrape event {event['id']}: {e}")
            else:
                # Default: Scrape the latest event
                latest_event = events[0]
                print(f"Found {len(events)} events. Scraping latest: {latest_event['name']} ({latest_event['date_str']})")
                process_event(latest_event['id'], args.output_dir, args.output)
            return

        # 3. Fallback: Try to use the input as an ID directly
        if args.url.isdigit():
             process_event(args.url, args.output_dir, args.output)
        else:
            print("Could not determine Event ID from URL.")
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
