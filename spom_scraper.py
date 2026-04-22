"""
spom_scraper.py
---------------
Scraper for ICAI SPOM (Self-Paced Online Module) exam slot availability.

Target URL:
  https://spmt.icai.org/ICAI/LoginAction_showSlotDetails.action

How the portal actually works
──────────────────────────────
  Each dropdown is populated via AJAX GET requests, NOT HTML form POSTs.
  The response format is:   value$$label##value$$label##...
  Availability response:    address##DATE&&CAPACITY,DATE&&CAPACITY,...

  A CAPACITY > 0 means seats are available (green).
  A CAPACITY == 0 means fully booked (red).

AJAX endpoints
──────────────
  States  : GET LoginAction_getStatesForCountry.action?countryPk=1
  Cities  : GET LoginAction_getCityForTestCenters.action?statePk=<state_id>
  Centres : GET LoginAction_getTestCentreForCity.action?selectedCity=<city_val>
  Slots   : GET LoginAction_getTestCenterAddress.action?cmbTstCenter=<centre_val>

FIX (root cause)
─────────────────
  The previous version used POST requests + BeautifulSoup HTML parsing to
  discover states/cities/centres and a jQuery UI datepicker parser for slots.
  The portal never returns HTML dropdowns in response to POSTs — it uses pure
  AJAX.  The HTML parser always returned empty lists, so fetch_spom_states()
  and fetch_spom_cities() silently returned [], making the entire SPOM feature
  non-functional.  This rewrite uses the correct AJAX endpoints and the
  value$$label## data format, exactly matching spom_test.py which was
  confirmed working.
"""

import hashlib
import json
import logging
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


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _new_session() -> requests.Session:
    """Create a requests Session with retry logic and AJAX headers."""
    s = requests.Session()
    s.headers.update(_HEADERS)
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,          # waits 2 s, 4 s, 8 s between retries
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("https://", adapter)
    return s


def _init_session_cookies(s: requests.Session):
    """
    GET the main SPOM page to acquire session cookies before AJAX calls.
    Swallowed silently if the page is temporarily unreachable.
    """
    try:
        s.get(SPOM_URL, timeout=30)
    except Exception as exc:
        logger.warning(f"Could not prime SPOM session cookies: {exc}")


def _parse_icai_data(raw_text: str) -> list[tuple[str, str]]:
    """
    Parse the ICAI AJAX response format:  value$$label##value$$label##...

    Returns [(label, value), ...] — label-first to match the rest of the
    codebase convention (e.g. fetch_regions returns (label, value) tuples).
    Blank or 'null' responses return an empty list.
    """
    items: list[tuple[str, str]] = []
    if not raw_text or "null" in raw_text.lower():
        return items
    for row in raw_text.strip().split("##"):
        if "$$" in row:
            parts = row.split("$$", 1)
            value = parts[0].strip()
            label = parts[1].strip()
            if value and label:
                items.append((label, value))
    return items


# ─── Public scraping functions ────────────────────────────────────────────────

def fetch_spom_states() -> list[tuple[str, str]]:
    """
    Fetch all Indian State options from the SPOM portal via AJAX.

    Returns: [(label, value), ...]
    e.g. [("Maharashtra", "12"), ("Gujarat", "7"), ...]
    """
    try:
        s = _new_session()
        _init_session_cookies(s)
        r = s.get(
            f"{BASE_URL}LoginAction_getStatesForCountry.action",
            params={"countryPk": "1"},
            timeout=30,
        )
        r.raise_for_status()
        states = _parse_icai_data(r.text)
        if not states:
            logger.warning("fetch_spom_states: server returned empty state list")
        return states
    except Exception as exc:
        logger.error(f"fetch_spom_states failed: {exc}")
        return []


def fetch_spom_cities(state_value: str) -> list[tuple[str, str]]:
    """
    Fetch City options for the given state from the SPOM portal via AJAX.

    Args:
        state_value: The numeric state ID returned by fetch_spom_states().

    Returns: [(label, value), ...]
    e.g. [("Pune", "PUNE"), ("Mumbai", "MUMBAI"), ...]
    """
    try:
        s = _new_session()
        _init_session_cookies(s)
        r = s.get(
            f"{BASE_URL}LoginAction_getCityForTestCenters.action",
            params={"statePk": state_value},
            timeout=30,
        )
        r.raise_for_status()
        cities = _parse_icai_data(r.text)
        if not cities:
            logger.warning(
                f"fetch_spom_cities: server returned empty city list "
                f"(state={state_value!r})"
            )
        return cities
    except Exception as exc:
        logger.error(f"fetch_spom_cities(state={state_value!r}) failed: {exc}")
        return []


def _fetch_spom_centres(city_value: str) -> list[tuple[str, str]]:
    """
    Fetch Test Centre options for the given city from the SPOM portal via AJAX.

    Internal helper — not part of the public API consumed by bot.py.

    Args:
        city_value: The city value (e.g. "PUNE") from fetch_spom_cities().

    Returns: [(label, value), ...]
    """
    try:
        s = _new_session()
        _init_session_cookies(s)
        r = s.get(
            f"{BASE_URL}LoginAction_getTestCentreForCity.action",
            params={"selectedCity": city_value},
            timeout=30,
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
) -> dict:
    """
    Fetch slot availability for one specific test centre via AJAX.

    The server returns a string of the form:
        <address>##DATE&&CAPACITY,DATE&&CAPACITY,...
    A CAPACITY > 0 means green (available); 0 means red (fully booked).

    Args:
        state_value:  State ID (passed through for API consistency; not sent
                      to this specific endpoint, but kept for caller symmetry).
        city_value:   City value (same note as state_value).
        centre_value: Test centre value from _fetch_spom_centres().
        centre_label: Human-readable centre name for display.

    Returns:
      {
        "centre":    str,                    # display name
        "available": ["04-May-2026", ...],   # dates with seats > 0  (green)
        "booked":    ["27-Apr-2026", ...],   # dates with seats == 0 (red)
        "error":     None | str,
      }
    """
    result: dict = {
        "centre":    centre_label or centre_value,
        "available": [],
        "booked":    [],
        "error":     None,
    }
    try:
        s = _new_session()
        _init_session_cookies(s)
        r = s.get(
            f"{BASE_URL}LoginAction_getTestCenterAddress.action",
            params={"cmbTstCenter": centre_value},
            timeout=30,
        )
        r.raise_for_status()
        raw = r.text.strip()

        if not raw or "##" not in raw:
            result["error"] = "Server returned empty or invalid response"
            logger.warning(
                f"fetch_spom_availability: unexpected response for "
                f"centre={centre_label!r}: {raw[:80]!r}"
            )
            return result

        # Split on the first ## only: parts[0] = address, parts[1] = date blob
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
                logger.debug(
                    f"Could not parse capacity {capacity_str!r} "
                    f"for date {date_val!r} at {centre_label!r}"
                )
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
    """
    Fetch slot availability for ALL test centres in a given city.

    Returns a list of per-centre dicts (see fetch_spom_availability for schema).
    An empty list means no centres were found for this city.
    """
    centres = _fetch_spom_centres(city_value)
    if not centres:
        logger.warning(
            f"fetch_all_city_availability: no centres found "
            f"(state={state_value!r}, city={city_value!r})"
        )
        return []

    results: list[dict] = []
    for label, value in centres:
        logger.info(f"  SPOM: fetching availability for centre '{label}'")
        avail = fetch_spom_availability(state_value, city_value, value, label)
        results.append(avail)
        time.sleep(0.8)   # polite delay between requests

    return results


# ─── Hashing helpers ──────────────────────────────────────────────────────────

def compute_spom_hash(centre_results: list[dict]) -> str:
    """
    Hash only the available dates across all centres.
    Hash changes ONLY when new green (available) dates appear or disappear.
    Booked dates / errors do NOT affect the hash.
    """
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
    """
    Compare previous and current availability and return ONLY newly-available dates.

    Returns:
      { centre_name: [new_date1, new_date2, ...], ... }
    Only entries with at least one new date are included.
    """
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
