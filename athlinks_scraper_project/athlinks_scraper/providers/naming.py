"""Name normalisation shared by all providers."""
import re

_YEAR = re.compile(r"\b(19|20)\d{2}\b")


def race_group_key(name):
    """
    Stable slug that identifies "the same race" across years and providers.
    '2024 NYRR Frosty 5K' -> 'frosty-5k'.
    """
    s = (name or "").lower()
    s = s.replace("new york road runners", " ")
    s = _YEAR.sub(" ", s)
    s = re.sub(r"\bnyrr\b", " ", s)
    s = re.sub(r"\bthe\b", " ", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def race_group_label(name):
    """
    Human display name for a race group: original casing, year and 'NYRR' removed.
    '2024 NYRR Frosty 5K' -> 'Frosty 5K'.
    """
    s = re.sub(r"(?i)new york road runners", " ", name or "")
    s = _YEAR.sub(" ", s)
    s = re.sub(r"(?i)\bnyrr\b", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip(" -–")


def strip_fractional_seconds(time_str):
    """'21:52.05' -> '21:52'. Leaves None / '' untouched."""
    if not time_str:
        return time_str
    return re.sub(r"\.\d+$", "", time_str)
