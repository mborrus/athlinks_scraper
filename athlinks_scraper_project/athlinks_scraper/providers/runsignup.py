"""RunSignup provider (runsignup.com).

API facts (verified 2026-09-06): base https://runsignup.com/Rest, GET with
format=json, no API key for public results. A RunSignup "race" spans years;
each "event" is one distance in one year. The results endpoint needs both
ids, so EventRef.event_id is "{race_id}:{event_id}".
"""
import re
from datetime import datetime
from typing import List

from .. import core
from .base import EventRef, RaceProvider, to_canonical
from .naming import race_group_key, strip_fractional_seconds

RUNSIGNUP_API = "https://runsignup.com/Rest"
RUNSIGNUP_PAGE_SIZE = 1000

_RACE_ID_PATH = re.compile(r"runsignup\.com/Race/Results/(\d+)", re.IGNORECASE)
_RACE_ID_QUERY = re.compile(r"[?&]raceId=(\d+)", re.IGNORECASE)


def extract_race_id(url):
    for pattern in (_RACE_ID_PATH, _RACE_ID_QUERY):
        match = pattern.search(url or "")
        if match:
            return match.group(1)
    return None


def _date_from_start_time(start_time):
    """'11/15/2025 08:20' -> '2025-11-15'; unparseable -> 'Unknown'."""
    if not start_time:
        return "Unknown"
    try:
        return datetime.strptime(start_time.split(" ")[0], "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return "Unknown"


def _split_ids(ref_event_id):
    race_id, _, event_id = ref_event_id.partition(":")
    if not event_id:
        raise ValueError(f"RunSignup event id must look like 'race:event', got {ref_event_id!r}")
    return race_id, event_id


class RunSignupProvider(RaceProvider):
    name = "runsignup"

    def matches(self, url: str) -> bool:
        return "runsignup.com" in url.lower()

    def list_events(self, url: str, session=None) -> List[EventRef]:
        race_id = extract_race_id(url)
        if not race_id:
            raise ValueError(
                f"Could not find a RunSignup race id in {url}. "
                "Use the results page URL, e.g. https://runsignup.com/Race/Results/100692"
            )
        data = core.fetch_json(f"{RUNSIGNUP_API}/race/{race_id}", params={"format": "json"}, session=session)
        race = data.get("race") or {}
        race_name = race.get("name") or f"RunSignup race {race_id}"
        group = race_group_key(race_name)

        refs = []
        for ev in race.get("events") or []:
            refs.append(EventRef(
                source=self.name,
                event_id=f"{race_id}:{ev['event_id']}",
                name=f"{race_name} - {ev.get('name', '')}".strip(" -"),
                date_str=_date_from_start_time(ev.get("start_time")),
                race_group=group,
                race_type=ev.get("name") or "",
            ))
        refs.sort(key=lambda r: r.date_str, reverse=True)
        return refs

    def fetch_event(self, ref: EventRef, session=None):
        race_id, event_id = _split_ids(ref.event_id)
        url = f"{RUNSIGNUP_API}/race/{race_id}/results/get-results"
        rows = []
        for page in range(1, core.MAX_PAGES + 1):
            data = core.fetch_json(
                url,
                params={"format": "json", "event_id": event_id, "num": RUNSIGNUP_PAGE_SIZE, "page": page},
                session=session,
            )
            sets = [s for s in (data.get("individual_results_sets") or []) if s.get("public_results", "T") == "T"]
            results = (sets[0].get("results") or []) if sets else []
            for r in results:
                time_str = strip_fractional_seconds(r.get("chip_time") or r.get("clock_time"))
                rows.append({
                    "Source": self.name,
                    "Race Group": ref.race_group,
                    "Event ID": ref.event_id,
                    "Event Name": ref.name,
                    "Event Date": ref.date_str,
                    "Race Type": ref.race_type,
                    "Name": f"{r.get('first_name', '')} {r.get('last_name', '')}".strip(),
                    "Gender": r.get("gender"),
                    "Age": r.get("age"),
                    "Bib": str(r["bib"]) if r.get("bib") is not None else None,
                    "City": r.get("city"),
                    "State": r.get("state"),
                    "Country": r.get("country_code"),
                    "Time": time_str,
                    "Pace": r.get("pace"),
                    "Overall Rank": r.get("place"),
                    "Gender Rank": None,
                    "Division Rank": None,
                    "Status": "CONF",
                })
            print(f"Fetched {len(results)} RunSignup results (page {page})")
            if len(results) < RUNSIGNUP_PAGE_SIZE:
                break
            core.time.sleep(core.REQUEST_DELAY_SECONDS)
        return to_canonical(rows)
