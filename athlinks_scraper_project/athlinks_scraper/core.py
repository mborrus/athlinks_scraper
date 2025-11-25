import requests
import pandas as pd
import re
from datetime import datetime
from urllib.parse import urlparse

def extract_event_id(url):
    """
    Extracts the specific Event ID from an Athlinks URL.
    Expected format: .../event/{event_id}/results...
    or .../results/Event/{event_id}/...
    """
    # Try to find 'Event/XXXXX' pattern first (Case sensitive to distinguish from 'event/' master ID)
    match = re.search(r'Event/(\d+)', url)
    if match:
        return match.group(1)
    
    # Fallback to 'event/XXXXX' if it looks like a specific result page
    # But be careful not to catch the master event ID if it's just /event/12345 without /results/
    if 'results' in url.lower():
        match = re.search(r'event/(\d+)', url, re.IGNORECASE)
        if match:
            return match.group(1)
            
    return None

def extract_master_id(url):
    """
    Extracts the Master Event ID from an Athlinks URL.
    Expected format: .../event/{master_id}
    """
    # Look for /event/XXXXX where XXXXX is the master ID
    # This usually appears at the beginning of the path after domain
    match = re.search(r'athlinks\.com/event/(\d+)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

def fetch_master_events(master_id):
    """
    Fetches all child events for a given master event ID.
    Returns a list of event objects (id, name, date).
    """
    url = f"https://reignite-api.athlinks.com/master/{master_id}/metadata"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        events = []
        # The 'events' list in the JSON contains the child events
        for event in data.get('events', []):
            # Extract relevant info
            events.append({
                'id': event.get('id'),
                'name': event.get('name'),
                'date': event.get('start', {}).get('epoch'), # Timestamp
                'date_str': pd.to_datetime(event.get('start', {}).get('epoch'), unit='ms').strftime('%Y-%m-%d') if event.get('start', {}).get('epoch') else 'Unknown'
            })
            
        # Sort by date descending (newest first)
        events.sort(key=lambda x: x['date'] or 0, reverse=True)
        return events
        
    except Exception as e:
        print(f"Error fetching master events: {e}")
        return []

def fetch_metadata(event_id):
    """
    Fetches event metadata (Name, Date, etc.)
    """
    url = f"https://reignite-api.athlinks.com/event/{event_id}/metadata"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Warning: Could not fetch metadata: {e}")
        return {}

def fetch_results(event_id):
    """
    Fetches all results for the given event ID from the Athlinks API.
    Handles pagination automatically.
    Returns the raw list of course objects.
    """
    base_url = f"https://reignite-api.athlinks.com/event/{event_id}/results"
    # We need to store the raw course objects to preserve the structure
    # But pagination might return partial course objects?
    # Let's accumulate the 'intervals' -> 'results' into a structure we can parse later.
    # Actually, to keep it simple, let's just return a flat list of enriched result dicts here?
    # No, separation of concerns. Let's return the raw data blocks.
    
    # Issue: The API returns a list of courses. Pagination likely appends results to the 'results' list inside the intervals.
    # If we just append the whole response objects, we might duplicate course metadata but that's fine.
    # We will parse it all together.
    
    all_data_blocks = []
    limit = 100
    from_index = 0
    
    print(f"Fetching results for Event ID: {event_id}...")
    
    while True:
        params = {
            "correlationId": "",
            "from": from_index,
            "limit": limit
        }
        
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data: {e}")
            break
            
        batch_results_count = 0
        
        if isinstance(data, list):
            all_data_blocks.extend(data) # Store the raw blocks
            for course in data:
                if 'intervals' in course:
                    for interval in course['intervals']:
                        if 'results' in interval:
                            batch_results_count += len(interval['results'])
        
        print(f"Fetched {batch_results_count} results")
        
        if batch_results_count == 0:
            break
            
        from_index += limit
        
    return all_data_blocks

def parse_results(data_blocks, metadata=None):
    """
    Parses the list of raw data blocks into a flat list of dicts suitable for CSV.
    Enriches with metadata if provided.
    """
    parsed_data = []
    
    # Extract Event Info
    event_name = metadata.get('name', '') if metadata else ''
    event_date = ''
    if metadata and 'start' in metadata and 'epoch' in metadata['start']:
        try:
            dt = datetime.fromtimestamp(metadata['start']['epoch'] / 1000)
            event_date = dt.strftime('%Y-%m-%d')
        except:
            pass
    
    event_id = metadata.get('id', '') if metadata else ''

    for course in data_blocks:
        race_obj = course.get('race') or {}
        race_type = race_obj.get('name', '')
        
        if 'intervals' in course:
            for interval in course['intervals']:
                dist_obj = interval.get('distance') or {}
                dist_meters = dist_obj.get('meters')
                if 'results' in interval:
                    for r in interval['results']:
                        # Calculate Pace
                        pace_str = ""
                        if r.get("chipTimeInMillis") and dist_meters:
                            try:
                                time_min = int(r["chipTimeInMillis"]) / 1000 / 60
                                dist_miles = dist_meters * 0.000621371
                                if dist_miles > 0:
                                    pace_min_per_mile = time_min / dist_miles
                                    p_min = int(pace_min_per_mile)
                                    p_sec = int((pace_min_per_mile - p_min) * 60)
                                    pace_str = f"{p_min}:{p_sec:02d}"
                            except Exception:
                                pass

                        # Basic info
                        location = r.get("location") or {}
                        rankings = r.get("rankings") or {}
                        
                        entry = {
                            "Event ID": event_id,
                            "Event Name": event_name,
                            "Event Date": event_date,
                            "Race Type": race_type,
                            "Name": r.get("displayName"),
                            "Gender": r.get("gender"),
                            "Age": r.get("age"),
                            "Bib": r.get("bib"),
                            "City": location.get("locality"),
                            "State": location.get("region"),
                            "Country": location.get("country"),
                            "Time": r.get("chipTimeInMillis"), # Needs conversion
                            "Pace": pace_str,
                            "Overall Rank": rankings.get("overall"),
                            "Gender Rank": rankings.get("gender"),
                            "Division Rank": rankings.get("primary"),
                            "Status": r.get("status"),
                        }
                        
                        # Convert time from millis to readable string
                        if entry["Time"]:
                            seconds = int(entry["Time"]) // 1000
                            m, s = divmod(seconds, 60)
                            h, m = divmod(m, 60)
                            entry["Time"] = f"{h:02d}:{m:02d}:{s:02d}" if h > 0 else f"{m:02d}:{s:02d}"
                            
                        parsed_data.append(entry)
        
    return parsed_data

def results_to_df(data_blocks, metadata=None):
    """
    Converts parsed results to a Pandas DataFrame.
    """
    parsed = parse_results(data_blocks, metadata)
    df = pd.DataFrame(parsed)
    return df

def get_results(url_or_id):
    """
    Main entry point. Takes a URL or Event ID, fetches results, and returns a DataFrame.
    """
    if str(url_or_id).isdigit():
        event_id = url_or_id
    else:
        event_id = extract_event_id(url_or_id)
        
    metadata = fetch_metadata(event_id)
    raw_data = fetch_results(event_id)
    df = results_to_df(raw_data, metadata)
    return df
