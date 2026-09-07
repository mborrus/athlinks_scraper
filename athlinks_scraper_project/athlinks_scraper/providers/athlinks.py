"""Athlinks provider — thin adapter over the existing core module."""
from typing import List

from .. import core
from .base import EventRef, RaceProvider, to_canonical


class AthlinksProvider(RaceProvider):
    name = "athlinks"

    def matches(self, url: str) -> bool:
        return "athlinks.com" in url.lower()

    def list_events(self, url: str, session=None) -> List[EventRef]:
        # A results URL naming one specific year's event: resolve the master
        # id from that event's own metadata rather than guessing from the URL
        # (a full results URL also contains the master id, but the event's
        # metadata is the authoritative source).
        event_id = core.extract_event_id(url)
        if event_id:
            meta = core.fetch_metadata(event_id, session=session)
            master_id = str(meta.get("masterId") or "")
            if not master_id:
                raise ValueError(f"Athlinks event {event_id} has no master id")
        else:
            master_id = core.extract_master_id(url)
            if not master_id:
                raise ValueError(f"Could not find an Athlinks event id in {url}")

        events = core.fetch_master_events(master_id, session=session)
        return [
            EventRef(
                source=self.name,
                event_id=str(e["id"]),
                name=e.get("name") or "",
                date_str=e.get("date_str") or "Unknown",
                race_group=str(master_id),
            )
            for e in events
        ]

    def fetch_event(self, ref: EventRef, session=None):
        df = core.get_results(ref.event_id, session=session)
        rows = df.to_dict("records")
        for row in rows:
            row["Source"] = self.name
            row["Race Group"] = ref.race_group
        return to_canonical(rows)
