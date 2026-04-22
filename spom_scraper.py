"""
spom_scraper.py
---------------
Scraper for ICAI SPOM (Self-Paced Online Module) exam slot availability.

ROOT CAUSE FIX: spmt.icai.org firewalls Railway's server IPs (ConnectTimeout).
Your local machine works; Railway's US/EU IPs are blocked by ICAI's WAF.

SOLUTION:
  1. States are hardcoded — they're static Indian states that never change,
     so no HTTP call is needed at all. Eliminates the blocked fetch entirely.
  2. AJAX calls (cities/centres/availability) route through a proxy when the
     SPOM_PROXY_URL environment variable is set in Railway.

SETUP (Railway dashboard → Variables):
  SPOM_PROXY_URL = http://username:password@proxyhost:port
  (Leave unset to run without proxy — useful for local dev on Indian IP)

FREE PROXY OPTIONS:
  - Webshare.io        — 10 free proxies, Indian IPs available
  - ProxyScrape        — free list at proxyscrape.com
  - Bright Data        — has free trial
  Or deploy on a VPS in India (Hostinger ~$3/mo, has Mumbai region)
"""

import hashlib
import json
import logging
import os
import threading
import time
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

BASE_URL = "https://spmt.icai.org/ICAI/"
SPOM_URL = f"{BASE_URL}LoginAction_showCentreDetails.action"

_PAGE_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_AJAX_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":           "*/*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer":          SPOM_URL,
    "Accept-Language":  "en-US,en;q=0.9",
}

# ─── Hardcoded states ─────────────────────────────────────────────────────────
# These are static — Indian states don't change. Hardcoding eliminates the
# blocked HTTP fetch to spmt.icai.org entirely for the states step.
# Source: parsed directly from LoginAction_showCentreDetails.action HTML.
_HARDCODED_STATES = [
    {"value": "35", "label": "Andaman And Nicobar"},
    {"value": "1",  "label": "Andhra Pradesh"},
    {"value": "2",  "label": "Arunachal Pradesh"},
    {"value": "3",  "label": "Assam"},
    {"value": "4",  "label": "Bihar"},
    {"value": "5",  "label": "Chhattisgarh"},
    {"value": "6",  "label": "Delhi"},
    {"value": "45", "label": "Delhi Ncr"},
    {"value": "7",  "label": "Goa"},
    {"value": "8",  "label": "Gujarat"},
    {"value": "9",  "label": "Haryana"},
    {"value": "10", "label": "Himachal Pradesh"},
    {"value": "11", "label": "Jammu And Kashmir"},
    {"value": "12", "label": "Jharkhand"},
    {"value": "13", "label": "Karnataka"},
    {"value": "14", "label": "Kerala"},
    {"value": "15", "label": "Madhya Pradesh"},
    {"value": "16", "label": "Maharashtra"},
    {"value": "17", "label": "Manipur"},
    {"value": "18", "label": "Meghalaya"},
    {"value": "19", "label": "Mizoram"},
    {"value": "20", "label": "Nagaland"},
    {"value": "21", "label": "Odisha"},
    {"value": "22", "label": "Pondicherry"},
    {"value": "23", "label": "Punjab"},
    {"value": "24", "label": "Rajasthan"},
    {"value": "25", "label": "Sikkim"},
    {"value": "27", "label": "Tamil Nadu"},
    {"value": "28", "label": "Telangana"},
    {"value": "29", "label": "Tripura"},
    {"value": "30", "label": "Uttar Pradesh"},
    {"value": "31", "label": "Uttarakhand"},
    {"value": "32", "label": "West Bengal"},
]

# ─── States cache ─────────────────────────────────────────────────────────────
_SPOM_STATES_CACHE = None
_SPOM_STATES_LOCK  = threading.Lock()

# ─── Proxy helpers ────────────────────────────────────────────────────────────

def _get_proxies() -> dict | None:
    """
    Read SPOM_PROXY_URL from environment.
    Set this in Railway dashboard → Variables if spmt.icai.org blocks your IPs.

    Example values:
      http://user:pass@proxy.webshare.io:80
      socks5://user:pass@proxy.example.com:1080
    """
    proxy_url = os.environ.get("SPOM_PROXY_URL", "").strip()
    if proxy_url:
        logger.info(f"[SPOM] Using proxy: {proxy_url.split('@')[-1]}")  # hide credentials
        return {"http": proxy_url, "https": proxy_url}
    return None


def _new_session() -> requests.Session:
    """Create a session with retry logic and optional proxy."""
    s = requests.Session()
    proxies = _get_proxies()
    if proxies:
        s.proxies.update(proxies)
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def _make_primed_session() -> requests.Session:
    """Create a session primed with a valid JSESSIONID from the public page."""
    s = _new_session()
    try:
        r = s.get(SPOM_URL, headers=_PAGE_HEADERS, timeout=30, allow_redirects=True)
        logger.debug(f"[SPOM prime] status={r.status_code}, cookies={list(s.cookies.keys())}")
        if not s.cookies:
            logger.warning("[SPOM prime] No session cookie — AJAX calls may fail")
    except Exception as exc:
        logger.warning(f"[SPOM prime] Failed (non-fatal): {exc}")
    return s


def _parse_icai_data(raw_text: str, context: str = "") -> list[dict]:
    """Parse ICAI format: value$$label##value$$label"""
    tag = f"[SPOM/{context}]" if context else "[SPOM]"
    items = []

    if not raw_text:
        logger.warning(f"{tag} Empty response")
        return items
    if "<html" in raw_text[:200].lower() or "<!doctype" in raw_text[:200].lower():
        logger.warning(f"{tag} Got HTML instead of data (login redirect?): {raw_text[:200]!r}")
        return items
    if "null" in raw_text.lower():
        logger.warning(f"{tag} Null response: {raw_text[:200]!r}")
        return items

    for row in raw_text.strip().split("##"):
        if "$$" in row:
            val, label = row.split("$$", 1)
            items.append({"value": val.strip(), "label": label.strip()})

    if not items:
        logger.warning(f"{tag} Parsed 0 items. Raw: {raw_text[:300]!r}")
    return items


# ─── Public scraping functions ────────────────────────────────────────────────

def fetch_spom_states() -> list[dict]:
    """
    Return all Indian states.

    FIX: Returns hardcoded list instantly — no HTTP call needed.
    spmt.icai.org blocks Railway/cloud IPs so any fetch would timeout.
    States are static (Indian states don't change) so hardcoding is safe.
    """
    logger.info(f"[SPOM] Returning {len(_HARDCODED_STATES)} hardcoded states (no HTTP needed)")
    return _HARDCODED_STATES


def fetch_spom_cities(state_value: str) -> list[dict]:
    """Fetch City options for the given state via AJAX."""
    try:
        s = _make_primed_session()
        url = f"{BASE_URL}LoginAction_getCityForTestCenters.action"
        logger.info(f"[SPOM] Fetching cities for state={state_value}")
        r = s.get(url, params={"statePk": state_value}, headers=_AJAX_HEADERS, timeout=30)
        logger.info(f"[SPOM] Cities: status={r.status_code}, length={len(r.text)}")
        r.raise_for_status()
        return _parse_icai_data(r.text, context=f"cities/state={state_value}")
    except Exception as exc:
        logger.error(f"[SPOM] fetch_spom_cities(state={state_value}) failed: {exc}", exc_info=True)
        return []


def _fetch_spom_centres(city_value: str, session=None) -> list[dict]:
    """Fetch Test Centre options for the given city via AJAX."""
    try:
        s = session or _make_primed_session()
        url = f"{BASE_URL}LoginAction_getTestCentreForCity.action"
        logger.info(f"[SPOM] Fetching centres for city={city_value}")
        r = s.get(url, params={"selectedCity": city_value}, headers=_AJAX_HEADERS, timeout=30)
        logger.info(f"[SPOM] Centres: status={r.status_code}, length={len(r.text)}")
        r.raise_for_status()
        return _parse_icai_data(r.text, context=f"centres/city={city_value}")
    except Exception as exc:
        logger.error(f"[SPOM] _fetch_spom_centres(city={city_value}) failed: {exc}", exc_info=True)
        return []


def fetch_spom_availability(
    state_value: str, city_value: str, centre_value: str,
    centre_label: str = "", session=None,
) -> dict:
    """Fetch slot availability for one specific test centre."""
    result = {"centre": centre_label or centre_value, "available": [], "booked": [], "error": None}
    try:
        s = session or _make_primed_session()
        url = f"{BASE_URL}LoginAction_getTestCenterAddress.action"
        r = s.get(url, params={"cmbTstCenter": centre_value}, headers=_AJAX_HEADERS, timeout=30)
        r.raise_for_status()
        raw = r.text.strip()

        if "##" not in raw:
            logger.warning(f"[SPOM] No ## in availability for centre={centre_value}: {raw[:200]!r}")
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
        logger.error(f"[SPOM] fetch_spom_availability(centre={centre_value}) failed: {exc}", exc_info=True)
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
    """Returns dict of centre → newly available {date, seats} dicts."""
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
