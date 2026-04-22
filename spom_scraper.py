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
# Indian states are completely static — no need to re-fetch on every /spom command.
# Cached on first successful fetch and reused for the bot's lifetime.
_SPOM_STATES_CACHE: list[tuple[str, str]] | None = None
_SPOM_STATES_LOCK = threading.Lock()


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _new_session() -> requests.Session:
    """Create a requests Session with retry logic and AJAX headers.

    FIX: backoff_factor reduced from 2 → 0.3.
    The old value (2) caused retries to wait 2 s + 4 s + 8 s = 14 s minimum
    on any transient error, which compounded with the slow session-priming GETs
    to make /spom appear completely frozen.
    """
    s = requests.Session()
    s.headers.update(_HEADERS)
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.3,        # waits 0.3 s, 0.6 s, 1.2 s — fast recovery
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("https://", adapter)
    return s


def _init_session_cookies(s: requests.Session):
    """
    GET the main SPOM page once to acquire session cookies before AJAX calls.

    FIX: timeout reduced from 30 s → 15 s. The page only needs to deliver a
    Set-Cookie header; we don't read the body. 30 s was the dominant source of
    slowness when this was called once-per-function (see _make_primed_session).
    """
    try:
        s.get(SPOM_URL, timeout=15)
    except Exception as exc:
        logger.warning(f"Could not prime SPOM session cookies: {exc}")


def _make_primed_session() -> requests.Session:
    """
    Create one session and prime it with cookies in a single step.

    FIX (root-cause of slowness): previously every public function
    (fetch_spom_states, fetch_spom_cities, _fetch_spom_centres,
    fetch_spom_availability) created its own session and called
    _init_session_cookies() independently.  For a city with N test centres
    that meant (2 + N) separate full-page GETs to SPOM_URL just for cookie
    priming, each with a 30-second timeout.

    Now callers create ONE primed session at the start of a high-level
    operation (fetch_spom_states, fetch_spom_cities, fetch_all_city_availability)
    and pass it down to sub-functions, so cookie priming happens exactly once.
    """
    s = _new_session()
    _init_session_cookies(s)
    return s


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

    FIX: Results are cached in memory after the first successful fetch.
    India's state list is completely static — there is zero reason to make a
    network round-trip (+ cookie-prime GET) on every /spom command.
    Subsequent calls return instantly from the cache.

    Returns: [(label, value), ...]
    e.g. [("Maharashtra", "12"), ("Gujarat", "7"), ...]
    """
    global _SPOM_STATES_CACHE

    # Fast path — return cache without any network I/O
    with _SPOM_STATES_LOCK:
        if _SPOM_STATES_CACHE is not None:
            logger.info("fetch_spom_states: returning cached state list (%d states)",
                        len(_SPOM_STATES_CACHE))
            return _SPOM_STATES_CACHE

    # Slow path — first call only: ONE session prime, ONE AJAX GET
    try:
        s = _make_primed_session()     # FIX: one prime, not two separate calls
        r = s.get(
            f"{BASE_URL}LoginAction_getStatesForCountry.action",
            params={"countryPk": "1"},
            timeout=10,                # FIX: 10 s is plenty for a small AJAX response
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


def fetch_spom_cities(state_value: str) -> list[tuple[str, str]]:
    """
    Fetch City options for the given state from the SPOM portal via AJAX.

    FIX: ONE session prime per call (via _make_primed_session) instead of the
    previous pattern that called _init_session_cookies separately and then
    immediately made the AJAX call on the same session — redundant but fine.
    The real win here is that we no longer create a fresh session + prime for
    the states call AND the cities call — the background thread in bot.py calls
    these sequentially, so each gets exactly one prime.

    Args:
        state_value: The numeric state ID returned by fetch_spom_states().

    Returns: [(label, value), ...]
    e.g. [("Pune", "PUNE"), ("Mumbai", "MUMBAI"), ...]
    """
    try:
        s = _make_primed_session()     # FIX: one prime per cities fetch
        r = s.get(
            f"{BASE_URL}LoginAction_getCityForTestCenters.action",
            params={"statePk": state_value},
            timeout=10,                # FIX: was 30 s
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


def _fetch_spom_centres(
    city_value: str,
    session: requests.Session | None = None,
) -> list[tuple[str, str]]:
    """
    Fetch Test Centre options for the given city from the SPOM portal via AJAX.

    FIX: accepts an optional pre-primed session so fetch_all_city_availability
    can reuse one session across all centre lookups instead of priming a new
    one for every centre.

    Args:
        city_value: The city value (e.g. "PUNE") from fetch_spom_cities().
        session:    Pre-primed requests.Session. Created here if not provided.

    Returns: [(label, value), ...]
    """
    try:
        s = session or _make_primed_session()
        r = s.get(
            f"{BASE_URL}LoginAction_getTestCentreForCity.action",
            params={"selectedCity": city_value},
            timeout=10,                # FIX: was 30 s
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
    """
    Fetch slot availability for one specific test centre via AJAX.

    FIX: accepts an optional pre-primed session so fetch_all_city_availability
    can reuse one session for all centres rather than priming N separate sessions
    (one per centre), which was the primary cause of the multi-minute delays.

    The server returns a string of the form:
        <address>##DATE&&CAPACITY,DATE&&CAPACITY,...
    A CAPACITY > 0 means green (available); 0 means red (fully booked).
    """
    result: dict = {
        "centre":    centre_label or centre_value,
        "available": [],
        "booked":    [],
        "error":     None,
    }
    try:
        s = session or _make_primed_session()   # FIX: reuse session if provided
        r = s.get(
            f"{BASE_URL}LoginAction_getTestCenterAddress.action",
            params={"cmbTstCenter": centre_value},
            timeout=10,                          # FIX: was 30 s
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

    FIX: Create ONE session and prime it ONCE for the entire operation.
    Previously each sub-call (_fetch_spom_centres, fetch_spom_availability)
    created its own session and primed it independently — for a city with N
    centres that was (1 + N) full-page GETs to SPOM_URL, each up to 30 s.
    Now it is exactly 1 GET to SPOM_URL regardless of how many centres exist.

    Returns a list of per-centre dicts (see fetch_spom_availability for schema).
    An empty list means no centres were found for this city.
    """
    # ONE session, ONE cookie prime — shared across all centre lookups below
    shared_session = _make_primed_session()

    centres = _fetch_spom_centres(city_value, session=shared_session)
    if not centres:
        logger.warning(
            f"fetch_all_city_availability: no centres found "
            f"(state={state_value!r}, city={city_value!r})"
        )
        return []

    results: list[dict] = []
    for label, value in centres:
        logger.info(f"  SPOM: fetching availability for centre '{label}'")
        avail = fetch_spom_availability(
            state_value, city_value, value, label,
            session=shared_session,    # FIX: reuse the already-primed session
        )
        results.append(avail)
        time.sleep(0.5)   # polite delay — reduced from 0.8 s since we're not re-priming

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
