"""
spom_scraper.py
---------------
Scraper for ICAI SPOM (Self-Paced Online Module) exam slot availability.

KEY FIXES vs previous version:
  1. SPOM_URL now points to the correct PUBLIC page (LoginAction_showCentreDetails.action)
     instead of LoginAction_showSlotDetails.action which is the logged-in booking page and
     redirects to login — so no valid JSESSIONID was ever being established.

  2. fetch_spom_states() now parses states directly from the page HTML instead of calling
     the AJAX endpoint. The HTML pre-embeds all Indian states in <select id="cmbStateList">
     so parsing is simpler, faster, and 100% reliable with no AJAX dependency.

  3. All AJAX calls (cities, centres, availability) now run on a session that was properly
     primed from the correct public page, so the JSESSIONID is valid.
"""

import hashlib
import json
import logging
import threading
import time
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL  = "https://spmt.icai.org/ICAI/"

# FIX: correct public-facing URL — this is the page with the slot availability form.
# LoginAction_showSlotDetails.action (old value) is the LOGGED-IN booking page and
# redirects to LoginAction.action, so no session cookie was ever set.
SPOM_URL  = f"{BASE_URL}LoginAction_showCentreDetails.action"

# Browser-like headers for page-load requests (not AJAX)
_PAGE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Headers for XHR calls (mirrors what the jQuery $.ajax sends)
_AJAX_HEADERS = {
    "User-Agent":        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":            "*/*",
    "X-Requested-With":  "XMLHttpRequest",
    "Referer":           SPOM_URL,
    "Accept-Language":   "en-US,en;q=0.9",
}

# ─── States cache ─────────────────────────────────────────────────────────────
_SPOM_STATES_CACHE = None
_SPOM_STATES_LOCK  = threading.Lock()

# ─── Internal helpers ─────────────────────────────────────────────────────────

def _new_session() -> requests.Session:
    """Create a requests Session with retry logic."""
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def _make_primed_session() -> requests.Session:
    """
    Create a session and load the public slot-details page to get a JSESSIONID.

    FIX: Previously the code hit LoginAction_showSlotDetails.action (the logged-in
    booking page), which redirected to the login page and set no useful session
    cookie. Now we hit the correct public page so the server returns a proper
    JSESSIONID that the AJAX endpoints will accept.
    """
    s = _new_session()
    try:
        r = s.get(SPOM_URL, headers=_PAGE_HEADERS, timeout=30, allow_redirects=True)
        logger.debug(
            f"[SPOM prime] status={r.status_code}, "
            f"cookies={list(s.cookies.keys())}, url={r.url}"
        )
        if not s.cookies:
            logger.warning(
                "[SPOM prime] No session cookie received — AJAX calls may be rejected. "
                f"Final URL after redirects: {r.url}"
            )
    except Exception as exc:
        logger.warning(f"[SPOM prime] Page load failed (non-fatal): {exc}")
    return s


def _parse_icai_data(raw_text: str, context: str = "") -> list[dict]:
    """
    Parse the ICAI pipe-delimited format:  value$$label##value$$label

    Logs clearly when the server returns HTML or null so failures are visible.
    """
    tag = f"[SPOM parse/{context}]" if context else "[SPOM parse]"
    items = []

    if not raw_text:
        logger.warning(f"{tag} Empty response body")
        return items

    if "<html" in raw_text[:200].lower() or "<!doctype" in raw_text[:200].lower():
        logger.warning(
            f"{tag} Server returned HTML (login redirect or error page) "
            f"instead of data. First 300 chars: {raw_text[:300]!r}"
        )
        return items

    if "null" in raw_text.lower():
        logger.warning(f"{tag} Server returned null-like response: {raw_text[:200]!r}")
        return items

    for row in raw_text.strip().split("##"):
        if "$$" in row:
            val, label = row.split("$$", 1)
            items.append({"value": val.strip(), "label": label.strip()})

    if not items:
        logger.warning(f"{tag} Parsed 0 items. Raw (first 300): {raw_text[:300]!r}")

    return items


# ─── Public scraping functions ────────────────────────────────────────────────

def fetch_spom_states() -> list[dict]:
    """
    Return all Indian State options.

    FIX: States are pre-embedded in the page HTML inside <select id="cmbStateList">.
    The AJAX endpoint (LoginAction_getStatesForCountry.action) is only triggered by
    the browser when the user changes the country dropdown — it is NOT called on
    page load for India (which is pre-selected).  Parsing the HTML directly is
    simpler, faster, and requires no extra round-trip.
    """
    global _SPOM_STATES_CACHE

    with _SPOM_STATES_LOCK:
        if _SPOM_STATES_CACHE is not None:
            logger.debug(f"[SPOM] Returning cached states ({len(_SPOM_STATES_CACHE)} entries)")
            return _SPOM_STATES_CACHE

    try:
        s = _new_session()
        logger.info(f"[SPOM] Loading centre-details page to parse states: {SPOM_URL}")

        r = s.get(SPOM_URL, headers=_PAGE_HEADERS, timeout=30, allow_redirects=True)

        logger.info(
            f"[SPOM] Page response: status={r.status_code}, "
            f"length={len(r.text)}, final_url={r.url}"
        )

        # Detect redirect to login page
        if "LoginAction_input" in r.url or "LoginAction.action" in r.url:
            logger.error(
                f"[SPOM] Redirected to login page ({r.url}). "
                "The centre-details page may now require authentication."
            )
            return []

        r.raise_for_status()

        # ── Parse states from <select id="cmbStateList"> ──────────────────────
        soup   = BeautifulSoup(r.text, "html.parser")
        select = soup.find("select", id="cmbStateList")

        if not select:
            logger.error(
                "[SPOM] <select id='cmbStateList'> not found in page. "
                "Page structure may have changed. "
                f"Page snippet: {r.text[:500]!r}"
            )
            return []

        states = [
            {"value": opt["value"], "label": opt.get_text(strip=True)}
            for opt in select.find_all("option")
            if opt.get("value") and opt["value"] not in ("-1", "")
        ]

        if states:
            logger.info(f"[SPOM] Parsed {len(states)} states from page HTML")
            with _SPOM_STATES_LOCK:
                _SPOM_STATES_CACHE = states
        else:
            logger.error(
                "[SPOM] Parsed 0 states from <select id='cmbStateList'>. "
                f"Select HTML: {str(select)[:500]}"
            )

        return states

    except Exception as exc:
        logger.error(f"[SPOM] fetch_spom_states failed: {exc}", exc_info=True)
        return []


def fetch_spom_cities(state_value: str) -> list[dict]:
    """Fetch City options for the given state via AJAX."""
    try:
        s = _make_primed_session()
        url = f"{BASE_URL}LoginAction_getCityForTestCenters.action"

        logger.info(f"[SPOM] Fetching cities for state={state_value}")
        r = s.get(url, params={"statePk": state_value}, headers=_AJAX_HEADERS, timeout=30)

        logger.info(f"[SPOM] Cities: status={r.status_code}, length={len(r.text)}")
        logger.debug(f"[SPOM] Cities raw (first 300): {r.text[:300]!r}")

        r.raise_for_status()
        return _parse_icai_data(r.text, context=f"cities(state={state_value})")

    except Exception as exc:
        logger.error(f"[SPOM] fetch_spom_cities(state={state_value}) failed: {exc}", exc_info=True)
        return []


def _fetch_spom_centres(city_value: str, session=None) -> list[dict]:
    """Fetch Test Centre options for the given city via AJAX."""
    try:
        s   = session or _make_primed_session()
        url = f"{BASE_URL}LoginAction_getTestCentreForCity.action"

        logger.info(f"[SPOM] Fetching centres for city={city_value}")
        r = s.get(url, params={"selectedCity": city_value}, headers=_AJAX_HEADERS, timeout=30)

        logger.info(f"[SPOM] Centres: status={r.status_code}, length={len(r.text)}")
        logger.debug(f"[SPOM] Centres raw (first 300): {r.text[:300]!r}")

        r.raise_for_status()
        return _parse_icai_data(r.text, context=f"centres(city={city_value})")

    except Exception as exc:
        logger.error(f"[SPOM] _fetch_spom_centres(city={city_value}) failed: {exc}", exc_info=True)
        return []


def fetch_spom_availability(
    state_value: str,
    city_value: str,
    centre_value: str,
    centre_label: str = "",
    session=None,
) -> dict:
    """
    Fetch slot availability for one specific test centre.

    Returns available as list of {"date": str, "seats": int} and
    booked as list of date strings.
    """
    result = {"centre": centre_label or centre_value, "available": [], "booked": [], "error": None}
    try:
        s   = session or _make_primed_session()
        url = f"{BASE_URL}LoginAction_getTestCenterAddress.action"

        r = s.get(url, params={"cmbTstCenter": centre_value}, headers=_AJAX_HEADERS, timeout=30)
        r.raise_for_status()
        raw = r.text.strip()

        logger.debug(f"[SPOM] Availability raw (centre={centre_value}, first 300): {raw[:300]!r}")

        if "##" not in raw:
            logger.warning(
                f"[SPOM] No '##' in availability for centre={centre_value}. Raw: {raw[:300]!r}"
            )
            result["error"] = "No valid data returned"
            return result

        parts     = raw.split("##", 1)
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
        logger.error(
            f"[SPOM] fetch_spom_availability(centre={centre_value}) failed: {exc}",
            exc_info=True,
        )
        result["error"] = str(exc)

    return result


def fetch_all_city_availability(state_value: str, city_value: str) -> list[dict]:
    """Fetch availability for ALL centres in a city."""
    s       = _make_primed_session()
    centres = _fetch_spom_centres(city_value, session=s)
    results = []
    for c in centres:
        results.append(
            fetch_spom_availability(state_value, city_value, c["value"], c["label"], session=s)
        )
        time.sleep(0.5)
    return results


def compute_spom_hash(centre_results: list[dict]) -> str:
    key_data = {
        item["centre"]: sorted(
            [{"date": s["date"], "seats": s["seats"]} for s in item.get("available", [])],
            key=lambda x: x["date"],
        )
        for item in centre_results
    }
    return hashlib.sha256(json.dumps(key_data, sort_keys=True).encode()).hexdigest()


def find_new_available_dates(
    old_results: list[dict], new_results: list[dict]
) -> dict[str, list[dict]]:
    """
    Returns a dict of centre → list of newly available {"date": str, "seats": int} dicts.
    Old results may be plain date strings (from persisted state) or dicts — handled below.
    """
    def _date_set(items):
        return {x["date"] if isinstance(x, dict) else x for x in items}

    old_map   = {item["centre"]: _date_set(item.get("available", [])) for item in old_results}
    new_dates = {}

    for item in new_results:
        centre    = item["centre"]
        old_dates = old_map.get(centre, set())
        added     = sorted(
            [s for s in item.get("available", []) if s["date"] not in old_dates],
            key=lambda x: x["date"],
        )
        if added:
            new_dates[centre] = added

    return new_dates
