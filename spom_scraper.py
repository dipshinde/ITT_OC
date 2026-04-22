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

# FIX: Use Accept: text/html for the priming GET (page request), not AJAX headers.
# AJAX headers are only set on the actual data-fetch requests below.
_PAGE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_AJAX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": SPOM_URL,
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── States cache ─────────────────────────────────────────────────────────────
_SPOM_STATES_CACHE = None
_SPOM_STATES_LOCK = threading.Lock()

# ─── Internal helpers ─────────────────────────────────────────────────────────

def _new_session() -> requests.Session:
    """Create a requests Session with retry logic."""
    s = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    s.mount("https://", adapter)
    return s

def _init_session_cookies(s: requests.Session):
    """
    Prime session cookies by hitting the public-facing slot details page.

    FIX: The priming request must look like a real browser page-load, NOT an
    AJAX call.  We use _PAGE_HEADERS here and switch to _AJAX_HEADERS only for
    the actual data requests.  Previously, sending X-Requested-With on the
    priming GET caused some Struts/Java app servers to return an empty or
    error response instead of the full page, so no JSESSIONID was ever set.

    Also hitting the base portal URL first (LoginAction.action) before the slot
    details page, mirroring how a real user navigates — this is often required
    for Struts apps that create the session context at the root action.
    """
    try:
        # Step 1: hit the root login/landing page to get initial session cookie
        r0 = s.get(f"{BASE_URL}LoginAction.action", headers=_PAGE_HEADERS, timeout=30, allow_redirects=True)
        logger.debug(f"[SPOM prime] root page status={r0.status_code}, cookies={dict(s.cookies)}")
    except Exception as exc:
        logger.warning(f"[SPOM prime] root page failed (non-fatal): {exc}")

    try:
        # Step 2: hit the actual slot-details page to establish full session context
        r1 = s.get(SPOM_URL, headers=_PAGE_HEADERS, timeout=30, allow_redirects=True)
        logger.debug(f"[SPOM prime] slot-details status={r1.status_code}, cookies={dict(s.cookies)}")
        if not s.cookies:
            logger.warning("[SPOM prime] No cookies set after priming — AJAX calls may be rejected by the server")
    except Exception as exc:
        logger.warning(f"[SPOM prime] slot-details page failed (non-fatal): {exc}")

def _make_primed_session() -> requests.Session:
    """Create a session and prime it with two browser-like page loads."""
    s = _new_session()
    _init_session_cookies(s)
    return s

def _parse_icai_data(raw_text: str, context: str = "") -> list[dict]:
    """
    Parses the ICAI format: value$$label##value$$label

    FIX: Added HTML detection and improved logging so failures are visible in
    logs instead of silently returning [].
    """
    items = []

    if not raw_text:
        logger.warning(f"[SPOM parse{' ' + context if context else ''}] Empty response body")
        return items

    # FIX: Detect when the server returns an HTML page (login redirect, error page)
    # instead of the expected pipe-delimited data format.
    if "<html" in raw_text[:200].lower() or "<!doctype" in raw_text[:200].lower():
        logger.warning(
            f"[SPOM parse{' ' + context if context else ''}] Server returned HTML instead of data "
            f"(likely a login redirect or error page). First 300 chars: {raw_text[:300]!r}"
        )
        return items

    if "null" in raw_text.lower():
        logger.warning(
            f"[SPOM parse{' ' + context if context else ''}] Server returned null-like response: {raw_text[:200]!r}"
        )
        return items

    rows = raw_text.strip().split("##")
    for row in rows:
        if "$$" in row:
            parts = row.split("$$", 1)
            items.append({"value": parts[0].strip(), "label": parts[1].strip()})

    if not items:
        logger.warning(
            f"[SPOM parse{' ' + context if context else ''}] Parsed 0 items from response. "
            f"Raw (first 300 chars): {raw_text[:300]!r}"
        )

    return items

# ─── Public scraping functions ────────────────────────────────────────────────

def fetch_spom_states() -> list[dict]:
    """Fetch all Indian State options via AJAX."""
    global _SPOM_STATES_CACHE

    # FIX: Hold the lock for the full check-and-return so two simultaneous
    # callers don't both see None and both fire an HTTP request.
    with _SPOM_STATES_LOCK:
        if _SPOM_STATES_CACHE is not None:
            logger.debug(f"[SPOM] Returning cached states ({len(_SPOM_STATES_CACHE)} entries)")
            return _SPOM_STATES_CACHE

    # Cache miss — fetch from portal (outside the lock so we don't block other threads)
    try:
        s = _make_primed_session()

        url = f"{BASE_URL}LoginAction_getStatesForCountry.action"
        logger.info(f"[SPOM] Fetching states from {url}")

        r = s.get(url, params={"countryPk": "1"}, headers=_AJAX_HEADERS, timeout=30)

        # FIX: Log status + raw response BEFORE parsing so failures are visible
        logger.info(f"[SPOM] States response: status={r.status_code}, length={len(r.text)}, "
                    f"content-type={r.headers.get('Content-Type', 'unknown')}")
        logger.debug(f"[SPOM] States raw response (first 500): {r.text[:500]!r}")

        r.raise_for_status()

        states = _parse_icai_data(r.text, context="states")

        if states:
            logger.info(f"[SPOM] Successfully fetched {len(states)} states")
            with _SPOM_STATES_LOCK:
                _SPOM_STATES_CACHE = states
        else:
            logger.error(
                f"[SPOM] fetch_spom_states returned 0 states. "
                f"Full response: {r.text[:1000]!r}"
            )

        return states

    except Exception as exc:
        logger.error(f"[SPOM] fetch_spom_states failed: {exc}", exc_info=True)
        return []

def fetch_spom_cities(state_value: str) -> list[dict]:
    """Fetch City options for the given state."""
    try:
        s = _make_primed_session()

        url = f"{BASE_URL}LoginAction_getCityForTestCenters.action"
        logger.info(f"[SPOM] Fetching cities for state={state_value}")

        r = s.get(url, params={"statePk": state_value}, headers=_AJAX_HEADERS, timeout=30)

        logger.info(f"[SPOM] Cities response: status={r.status_code}, length={len(r.text)}")
        logger.debug(f"[SPOM] Cities raw response (first 500): {r.text[:500]!r}")

        r.raise_for_status()
        return _parse_icai_data(r.text, context=f"cities(state={state_value})")

    except Exception as exc:
        logger.error(f"[SPOM] fetch_spom_cities(state={state_value}) failed: {exc}", exc_info=True)
        return []

def _fetch_spom_centres(city_value: str, session=None) -> list[dict]:
    """Fetch Test Centre options for the given city."""
    try:
        s = session or _make_primed_session()

        url = f"{BASE_URL}LoginAction_getTestCentreForCity.action"
        logger.info(f"[SPOM] Fetching centres for city={city_value}")

        r = s.get(url, params={"selectedCity": city_value}, headers=_AJAX_HEADERS, timeout=30)

        logger.info(f"[SPOM] Centres response: status={r.status_code}, length={len(r.text)}")
        logger.debug(f"[SPOM] Centres raw response (first 500): {r.text[:500]!r}")

        r.raise_for_status()
        return _parse_icai_data(r.text, context=f"centres(city={city_value})")

    except Exception as exc:
        logger.error(f"[SPOM] _fetch_spom_centres(city={city_value}) failed: {exc}", exc_info=True)
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

        url = f"{BASE_URL}LoginAction_getTestCenterAddress.action"
        r = s.get(url, params={"cmbTstCenter": centre_value}, headers=_AJAX_HEADERS, timeout=30)
        r.raise_for_status()
        raw = r.text.strip()

        logger.debug(f"[SPOM] Availability raw (centre={centre_value}, first 300): {raw[:300]!r}")

        if "##" not in raw:
            logger.warning(f"[SPOM] No '##' delimiter in availability response for centre={centre_value}. Raw: {raw[:300]!r}")
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
        logger.error(f"[SPOM] fetch_spom_availability(centre={centre_value}) failed: {exc}", exc_info=True)
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
