"""New York Road Runners provider (results.nyrr.org).

API facts (verified 2026-09-06): base https://rmsprodapi.nyrr.org/api/v2, all
POST + JSON, no auth. pageSize max 100. Event codes differ per year, so
sibling years are found by searching on the normalised event name.
"""
import re
from typing import List

from .. import core
from .base import EventRef, RaceProvider, to_canonical
from .naming import race_group_key, race_group_label

NYRR_API = "https://rmsprodapi.nyrr.org/api/v2"
NYRR_PAGE_SIZE = 100

_EVENT_CODE = re.compile(r"results\.nyrr\.org/event/([A-Za-z0-9]+)", re.IGNORECASE)

_UNIT_TO_RACE_TYPE = {
    "1M": "1 Mile", "2M": "2 Mile", "4M": "4 Mile", "5M": "5 Mile", "10M": "10 Mile",
    "5K": "5K", "8K": "8K", "10K": "10K", "15K": "15K", "20K": "20K",
    "HALF": "Half Marathon", "MAR": "Marathon",
}


def extract_event_code(url):
    match = _EVENT_CODE.search(url or "")
    return match.group(1) if match else None


def race_type_from_unit(unit_code, distance_name):
    """'MAR' -> 'Marathon'; unknown codes fall back to the API's distanceName."""
    return _UNIT_TO_RACE_TYPE.get((unit_code or "").upper(), distance_name or "")


class NyrrProvider(RaceProvider):
    name = "nyrr"

    def matches(self, url: str) -> bool:
        return "results.nyrr.org" in url.lower()

    def _details(self, event_code, session=None):
        data = core.post_json(NYRR_API + "/events/details", {"eventCode": event_code}, session=session)
        return data.get("eventDetails") or {}

    def list_events(self, url: str, session=None) -> List[EventRef]:
        event_code = extract_event_code(url)
        if not event_code:
            raise ValueError(f"Could not find an NYRR event code in {url}")

        details = self._details(event_code, session=session)
        this_name = details.get("eventName") or event_code
        group = race_group_key(this_name)

        search = core.post_json(
            NYRR_API + "/events/search",
            {"year": None, "searchString": race_group_label(this_name), "distance": None,
             "pageIndex": 1, "pageSize": NYRR_PAGE_SIZE},
            session=session,
        )

        refs = []
        for item in search.get("items", []):
            if race_group_key(item.get("eventName", "")) != group:
                continue
            refs.append(EventRef(
                source=self.name,
                event_id=item["eventCode"],
                name=item.get("eventName") or "",
                date_str=(item.get("startDateTime") or "Unknown")[:10],
                race_group=group,
                race_type=race_type_from_unit(item.get("distanceUnitCode"), item.get("distanceName")),
            ))

        refs.sort(key=lambda r: r.date_str, reverse=True)
        return refs

    def fetch_event(self, ref: EventRef, session=None):
        rows = []
        for page_index in range(1, core.MAX_PAGES + 1):
            data = core.post_json(
                NYRR_API + "/runners/finishers-filter",
                {"eventCode": ref.event_id, "pageIndex": page_index, "pageSize": NYRR_PAGE_SIZE,
                 "sortColumn": "overallPlace", "sortDescending": False},
                session=session,
            )
            items = data.get("items") or []
            for f in items:
                rows.append({
                    "Source": self.name,
                    "Race Group": ref.race_group,
                    "Event ID": ref.event_id,
                    "Event Name": ref.name,
                    "Event Date": ref.date_str,
                    "Race Type": ref.race_type,
                    "Name": f"{f.get('firstName', '')} {f.get('lastName', '')}".strip(),
                    "Gender": f.get("gender"),
                    "Age": f.get("age"),
                    "Bib": f.get("bib"),
                    "City": f.get("city"),
                    "State": f.get("stateProvince"),
                    "Country": f.get("countryCode"),
                    "Time": f.get("overallTime"),
                    "Pace": f.get("pace"),
                    "Overall Rank": f.get("overallPlace"),
                    "Gender Rank": f.get("genderPlace"),
                    "Division Rank": None,
                    "Status": "CONF",
                })
            print(f"Fetched {len(items)} NYRR results (page {page_index})")
            if len(items) < NYRR_PAGE_SIZE:
                break
            core.time.sleep(core.REQUEST_DELAY_SECONDS)
        return to_canonical(rows)
