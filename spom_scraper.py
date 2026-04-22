"""
spom_scraper.py
---------------
Scraper for ICAI SPOM (Self-Paced Online Module) exam slot availability.
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
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": SPOM_URL,
}

# ─── States cache ─────────────────────────────────────────────────────────────
_SPOM_STATES_CACHE: list[dict] | None = None
_SPOM_STATES_LOCK = threading.Lock()


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _new_session() -> requests.Session:
    """Create a requests Session with retry logic and AJAX headers."""
    s = requests.Session()
    s.headers.update(_HEADERS)
    # Reverted backoff_factor to 2 to give ICAI servers time to recover
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("https://", adapter)
    return s


def _init_session_cookies(s: requests.Session):
    """GET the main SPOM page once to acquire session cookies before AJAX calls."""
    try:
        # Reverted timeout to 30s as JSESSIONID assignment is slow
        s.get(SPOM_URL, timeout=30)
    except Exception as exc:
        logger.warning(f"Could not prime SPOM session cookies: {exc}")


def _make_primed_session() -> requests.Session:
    """Create one session and prime it with cookies in a single step."""
    s = _new_session()
    _init_session_cookies(s)
    return s


def _parse_icai_data(raw_text: str) -> list[dict]:
    """
    Parse the ICAI AJAX response format: value$$label##value$$label##...
    Returns a list of dicts to match spom_test.py.
    """
    items: list[dict] = []
    if not raw_text or "null" in raw_text.lower():
        return items
    for row in raw_text.strip().split("##"):
        if "$$" in row:
            parts = row.split("$$", 1)
            value = parts[0].strip()
            label = parts[1].strip()
            if value and label:
                items.append({"value": value, "label": label})
    return items


# ─── Public scraping functions ────────────────────────────────────────────────

def fetch_spom_states() -> list[dict]:
    """Fetch all Indian State options from the SPOM portal via AJAX."""
    global _SPOM_STATES_CACHE

    with _SPOM_STATES_LOCK:
        if _SPOM_STATES_CACHE is not None:
            logger.info("fetch_spom_states: returning cached state list")
            return _SPOM_STATES_CACHE

    try:
        s = _make_primed_session()
        r = s.get(
            f"{BASE_URL}LoginAction_getStatesForCountry.action",
            params={"countryPk": "1"},
            timeout=30, # Increased from 10s to 30s
        )
        r.raise_for_status()
        states = _parse_icai_data(r.text)
        if not states:
            logger.warning("fetch_spom_states: server returned empty state list")
            return []

        with _SPOM_STATES_LOCK:
            _SPOM_STATES_CACHE = states
        logger.info("fetch_spom_states: fetched and cached %d states", len(states))
        return states
    except Exception as exc:
        logger.error(f"fetch_spom_states failed: {exc}")
        return []


def fetch_spom_cities(state_value: str) -> list[dict]:
    """Fetch City options for the given state from the SPOM portal via AJAX."""
    try:
        s = _make_primed_session()
        r = s.get(
            f"{BASE_URL}LoginAction_getCityForTestCenters.action",
            params={"statePk": state_value},
            timeout=30, # Increased from 10s to 30s
        )
        r.raise_for_status()
        cities = _parse_icai_data(r.text)
        if not cities:
            logger.warning(
                f"fetch_spom_cities: server returned empty city list (state={state_value!r})"
            )
        return cities
    except Exception as exc:
        logger.error(f"fetch_spom_cities(state={state_value!r}) failed: {exc}")
        return []


def _fetch_spom_centres(
    city_value: str,
    session: requests.Session | None = None,
) -> list[dict]:
    """Fetch Test Centre options for the given city from the SPOM portal via AJAX."""
    try:
        s = session or _make_primed_session()
        r = s.get(
            f"{BASE_URL}LoginAction_getTestCentreForCity.action",
            params={"selectedCity": city_value},
            timeout=30, # Increased from 10s to 30s
        )
        r.raise_for_status()
        centres = _parse_icai_data(r.text)
        if not centres:
            logger.warning(
                f"_fetch_spom_centres: no centres returned for city={city_value!r}"
            )
        return centres
    except Exception as exc:
        logger.error(f"_fetch_spom_centres(city={city_value!r}) failed: {exc}")
        return []


def fetch_spom_availability(
    state_value:  str,
    city_value:   str,
    centre_value: str,
    centre_label: str = "",
    session: requests.Session | None = None,
) -> dict:
    """Fetch slot availability for one specific test centre via AJAX."""
    result: dict = {
        "centre":    centre_label or centre_value,
        "available": [],
        "booked":    [],
        "error":     None,
    }
    try:
        s = session or _make_primed_session()
        r = s.get(
            f"{BASE_URL}LoginAction_getTestCenterAddress.action",
            params={"cmbTstCenter": centre_value},
            timeout=30, # Increased from 10s to 30s
        )
        r.raise_for_status()
        raw = r.text.strip()

        if not raw or "##" not in raw:
            result["error"] = "Server returned empty or invalid response"
            return result

        parts     = raw.split("##", 1)
        date_blob = parts[1].strip() if len(parts) > 1 else ""

        if not date_blob or "NoDatesAvlMsg" in date_blob:
            result["error"] = "No slots available for booking"
            return result

        available: list[str] = []
        booked:    list[str] = []

        for slot in date_blob.split(","):
            slot = slot.strip()
            if "&&" not in slot:
                continue
            date_val, capacity_str = slot.split("&&", 1)
            date_val = date_val.strip()
            if not date_val:
                continue
            try:
                capacity = int(capacity_str.strip())
            except ValueError:
                continue

            if capacity > 0:
                available.append(date_val)
            else:
                booked.append(date_val)

        result["available"] = sorted(set(available))
        result["booked"]    = sorted(set(booked))

    except Exception as exc:
        logger.error(
            f"fetch_spom_availability(centre={centre_label!r}) failed: {exc}",
            exc_info=True,
        )
        result["error"] = str(exc)

    return result


def fetch_all_city_availability(state_value: str, city_value: str) -> list[dict]:
    """Fetch slot availability for ALL test centres in a given city."""
    shared_session = _make_primed_session()

    centres = _fetch_spom_centres(city_value, session=shared_session)
    if not centres:
        logger.warning(
            f"fetch_all_city_availability: no centres found "
            f"(state={state_value!r}, city={city_value!r})"
        )
        return []

    results: list[dict] = []
    # Updated to loop over dicts based on the new parse_icai_data format
    for centre in centres:
        label = centre["label"]
        value = centre["value"]
        
        logger.info(f"  SPOM: fetching availability for centre '{label}'")
        avail = fetch_spom_availability(
            state_value, city_value, value, label,
            session=shared_session,
        )
        results.append(avail)
        time.sleep(0.5)

    return results


# ─── Hashing helpers ──────────────────────────────────────────────────────────

def compute_spom_hash(centre_results: list[dict]) -> str:
    key_data = {
        item["centre"]: sorted(item.get("available", []))
        for item in centre_results
    }
    serialized = json.dumps(key_data, sort_keys=True)
    return hashlib.sha256(serialized.encode()).hexdigest()


def find_new_available_dates(
    old_results: list[dict],
    new_results: list[dict],
) -> dict[str, list[str]]:
    old_map: dict[str, set] = {
        item["centre"]: set(item.get("available", []))
        for item in old_results
    }
    new_dates: dict[str, list[str]] = {}
    for item in new_results:
        centre   = item["centre"]
        current  = set(item.get("available", []))
        previous = old_map.get(centre, set())
        added    = sorted(current - previous)
        if added:
            new_dates[centre] = added

    return new_dates
