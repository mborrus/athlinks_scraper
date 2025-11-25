import requests
import json

def fetch_metadata(event_id):
    url = f"https://reignite-api.athlinks.com/event/{event_id}/metadata"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error: {e}")
        return {}

event_id = "994637"
data = fetch_metadata(event_id)
print(json.dumps(data, indent=2))
