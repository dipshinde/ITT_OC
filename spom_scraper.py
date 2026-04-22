"""
spom_scraper.py
---------------
Scraper for ICAI SPOM (Self-Paced Online Module) exam slot availability.
Synchronized with working logic from spom_test.py.
"""

import hashlib
import json
import logging
import threading
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://spmt.icai.org/ICAI/"
SPOM_URL = f"{BASE_URL}LoginAction_showSlotDetails.action"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": SPOM_URL,
}

# ─── States cache ─────────────────────────────────────────────────────────────
_SPOM_STATES_CACHE = None
_SPOM_STATES_LOCK = threading.Lock()

# ─── Internal helpers ─────────────────────────────────────────────────────────

def _new_session() -> requests.Session:
    """Create a requests Session with retry logic and AJAX headers."""
    s = requests.Session()
    s.headers.update(_HEADERS)
    retry_strategy = Retry(
        total=3,
        backoff_factor=2, 
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("https://", adapter)
    return s

def _init_session_cookies(s: requests.Session):
    """Prime session cookies by hitting the main page, exactly like spom_test.py."""
    try:
        s.get(SPOM_URL, timeout=30)
    except Exception as exc:
        logger.warning(f"Could not prime SPOM session cookies: {exc}")

def _make_primed_session() -> requests.Session:
    """Create a session and hit the home page first to ensure the server accepts AJAX calls."""
    s = _new_session()
    _init_session_cookies(s)
    return s

def _parse_icai_data(raw_text: str) -> list[dict]:
    """Parses the ICAI format: value$$label##value$$label"""
    items = []
    if not raw_text or "null" in raw_text.lower():
        return items
    
    rows = raw_text.strip().split("##")
    for row in rows:
        if "$$" in row:
            parts = row.split("$$", 1)
            items.append({"value": parts[0].strip(), "label": parts[1].strip()})
    return items

# ─── Public scraping functions ────────────────────────────────────────────────

def fetch_spom_states() -> list[dict]:
    """Fetch all Indian State options via AJAX."""
    global _SPOM_STATES_CACHE
    with _SPOM_STATES_LOCK:
        if _SPOM_STATES_CACHE is not None:
            return _SPOM_STATES_CACHE

    try:
        s = _make_primed_session()
        r = s.get(f"{BASE_URL}LoginAction_getStatesForCountry.action", params={"countryPk": "1"}, timeout=30)
        r.raise_for_status()
        states = _parse_icai_data(r.text)
        if states:
            with _SPOM_STATES_LOCK:
                _SPOM_STATES_CACHE = states
        return states
    except Exception as exc:
        logger.error(f"fetch_spom_states failed: {exc}")
        return []

def fetch_spom_cities(state_value: str) -> list[dict]:
    """Fetch City options for the given state."""
    try:
        s = _make_primed_session()
        r = s.get(f"{BASE_URL}LoginAction_getCityForTestCenters.action", params={"statePk": state_value}, timeout=30)
        r.raise_for_status()
        return _parse_icai_data(r.text)
    except Exception as exc:
        logger.error(f"fetch_spom_cities(state={state_value}) failed: {exc}")
        return []

def _fetch_spom_centres(city_value: str, session=None) -> list[dict]:
    """Fetch Test Centre options for the given city."""
    try:
        s = session or _make_primed_session()
        r = s.get(f"{BASE_URL}LoginAction_getTestCentreForCity.action", params={"selectedCity": city_value}, timeout=30)
        r.raise_for_status()
        return _parse_icai_data(r.text)
    except Exception as exc:
        logger.error(f"_fetch_spom_centres(city={city_value}) failed: {exc}")
        return []

def fetch_spom_availability(state_value: str, city_value: str, centre_value: str, centre_label: str = "", session=None) -> dict:
    """Fetch slot availability for one specific test centre.

    Returns available as list of {"date": str, "seats": int} dicts so that
    alert messages can show the seat count alongside the date. Booked remains
    a plain list of date strings (seat count is always 0, not useful to store).
    """
    result = {"centre": centre_label or centre_value, "available": [], "booked": [], "error": None}
    try:
        s = session or _make_primed_session()
        r = s.get(f"{BASE_URL}LoginAction_getTestCenterAddress.action", params={"cmbTstCenter": centre_value}, timeout=30)
        r.raise_for_status()
        raw = r.text.strip()

        if "##" not in raw:
            result["error"] = "No valid data returned"
            return result

        parts = raw.split("##", 1)
        date_blob = parts[1].strip() if len(parts) > 1 else ""

        if not date_blob or "NoDatesAvlMsg" in date_blob:
            return result

        for slot in date_blob.split(","):
            if "&&" in slot:
                date_val, capacity = slot.split("&&", 1)
                cap = int(capacity.strip())
                if cap > 0:
                    result["available"].append({"date": date_val.strip(), "seats": cap})
                else:
                    result["booked"].append(date_val.strip())

        result["available"].sort(key=lambda x: x["date"])
        result["booked"].sort()
    except Exception as exc:
        logger.error(f"fetch_spom_availability failed: {exc}")
        result["error"] = str(exc)
    return result

def fetch_all_city_availability(state_value: str, city_value: str) -> list[dict]:
    """Fetch availability for ALL centres in a city."""
    s = _make_primed_session()
    centres = _fetch_spom_centres(city_value, session=s)
    results = []
    for c in centres:
        results.append(fetch_spom_availability(state_value, city_value, c['value'], c['label'], session=s))
        time.sleep(0.5) 
    return results

def compute_spom_hash(centre_results: list[dict]) -> str:
    # available is now list[{"date": str, "seats": int}]; sort by date for stable hash
    key_data = {
        item["centre"]: sorted(
            [{"date": s["date"], "seats": s["seats"]} for s in item.get("available", [])],
            key=lambda x: x["date"]
        )
        for item in centre_results
    }
    return hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()

def find_new_available_dates(old_results: list[dict], new_results: list[dict]) -> dict[str, list[dict]]:
    """
    Returns a dict of centre → list of newly available {"date": str, "seats": int} dicts.
    Old results may be plain date strings (from persisted state) or dicts — handled below.
    """
    def _date_set(items):
        result = set()
        for x in items:
            result.add(x["date"] if isinstance(x, dict) else x)
        return result

    old_map = {item["centre"]: _date_set(item.get("available", [])) for item in old_results}
    new_dates = {}
    for item in new_results:
        centre = item["centre"]
        old_dates = old_map.get(centre, set())
        added = sorted(
            [s for s in item.get("available", []) if s["date"] not in old_dates],
            key=lambda x: x["date"]
        )
        if added:
            new_dates[centre] = added
    return new_dates
