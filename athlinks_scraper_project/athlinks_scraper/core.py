import time
import requests
import pandas as pd
import re
from datetime import datetime
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- HTTP configuration ---------------------------------------------------
# Seconds to wait for the Athlinks API before giving up on one request.
DEFAULT_TIMEOUT = 15
# Seconds to pause between paginated results requests so we don't hammer the API.
REQUEST_DELAY_SECONDS = 0.5
# Hard cap on results pages per event (100 results/page -> 50,000 results).
MAX_PAGES = 500

_SESSION = None


def build_session():
    """
    Creates a requests.Session that automatically retries transient failures.

    Retries up to 3 times on connection errors and on 429/500/502/503/504
    responses, sleeping 1s, 2s, 4s between attempts (backoff_factor=1).
    """
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    return session


def get_session():
    """Returns the shared module-level session, creating it on first use."""
    global _SESSION
    if _SESSION is None:
        _SESSION = build_session()
    return _SESSION


def fetch_json(url, params=None, session=None, timeout=DEFAULT_TIMEOUT):
    """
    GETs `url` and returns the parsed JSON body.

    Raises requests.RequestException (or a subclass such as HTTPError) if the
    request ultimately fails after retries. Callers decide whether that is fatal.
    """
    session = session or get_session()
    response = session.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def post_json(url, body, session=None, timeout=DEFAULT_TIMEOUT):
    """
    POSTs `body` as JSON to `url` and returns the parsed JSON response.
    Raises requests.RequestException on failure (after the session's retries).
    """
    session = session or get_session()
    response = session.post(url, json=body, timeout=timeout)
    response.raise_for_status()
    return response.json()


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

def fetch_master_events(master_id, session=None):
    """
    Fetches all child events for a given master event ID.
    Returns a list of event dicts (id, name, date, date_str), newest first.
    Returns [] if the request fails.
    """
    url = f"https://reignite-api.athlinks.com/master/{master_id}/metadata"
    try:
        data = fetch_json(url, session=session)
    except requests.RequestException as e:
        print(f"Error fetching master events: {e}")
        return []

    events = []
    for event in data.get('events', []):
        epoch = (event.get('start') or {}).get('epoch')
        events.append({
            'id': event.get('id'),
            'name': event.get('name'),
            'date': epoch,
            'date_str': pd.to_datetime(epoch, unit='ms').strftime('%Y-%m-%d') if epoch else 'Unknown',
        })

    # Sort by date descending (newest first)
    events.sort(key=lambda x: x['date'] or 0, reverse=True)
    return events

def fetch_metadata(event_id, session=None):
    """
    Fetches event metadata (name, date, etc.). Returns {} if the request fails,
    because metadata is optional enrichment and should not abort a scrape.
    """
    url = f"https://reignite-api.athlinks.com/event/{event_id}/metadata"
    try:
        return fetch_json(url, session=session)
    except requests.RequestException as e:
        print(f"Warning: Could not fetch metadata: {e}")
        return {}

def fetch_results(event_id, session=None):
    """
    Fetches all results for the given event ID from the Athlinks API,
    following pagination until a page contains zero results.

    Returns the raw list of "course" blocks exactly as the API returned them
    (parse_results flattens them). Pauses REQUEST_DELAY_SECONDS between pages
    and stops after MAX_PAGES as a safety net.

    Raises requests.RequestException if any page cannot be fetched, so callers
    never receive a silently-truncated result set.
    """
    base_url = f"https://reignite-api.athlinks.com/event/{event_id}/results"
    limit = 100
    from_index = 0
    all_data_blocks = []

    print(f"Fetching results for Event ID: {event_id}...")

    for page_number in range(MAX_PAGES):
        params = {"correlationId": "", "from": from_index, "limit": limit}
        data = fetch_json(base_url, params=params, session=session)

        batch_results_count = 0
        if isinstance(data, list):
            all_data_blocks.extend(data)
            for course in data:
                for interval in course.get('intervals', []):
                    batch_results_count += len(interval.get('results', []))

        print(f"Fetched {batch_results_count} results")

        if batch_results_count == 0:
            break

        from_index += limit
        if page_number + 1 < MAX_PAGES:
            time.sleep(REQUEST_DELAY_SECONDS)
    else:
        print(f"Warning: stopped after {MAX_PAGES} pages; results may be incomplete.")

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

def get_results(url_or_id, session=None):
    """
    Main entry point. Takes a URL or Event ID, fetches results, and returns a DataFrame.
    Raises requests.RequestException if the results cannot be fetched.
    """
    if str(url_or_id).isdigit():
        event_id = url_or_id
    else:
        event_id = extract_event_id(url_or_id)

    metadata = fetch_metadata(event_id, session=session)
    raw_data = fetch_results(event_id, session=session)
    return results_to_df(raw_data, metadata)
