import re

def extract_master_id(url):
    match = re.search(r'athlinks\.com/event/(\d+)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

url = "https://www.athlinks.com/event/15776/results/Event/994637/Course/2152769/Results?page=1"
print(f"URL: {url}")
print(f"Extracted Master ID: {extract_master_id(url)}")
