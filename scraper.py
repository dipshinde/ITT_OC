"""
scraper.py
----------
Handles the full ASP.NET WebForms postback chain for the ICAI batch listing page.

How ASP.NET WebForms dropdowns work:
  1. GET the page → server sends HTML with __VIEWSTATE (encrypted session state)
  2. User picks Region → browser POSTs with __EVENTTARGET = region dropdown ID
  3. Server re-renders page with PoU options populated → new __VIEWSTATE
  4. User picks PoU → another POST
  5. User picks Course → another POST
  6. Final page has the batch table
We replicate this exact chain using requests.
"""

import requests
from bs4 import BeautifulSoup
import hashlib
import json
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://www.icaionlineregistration.org/launchbatchdetail.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.icaionlineregistration.org",
    "Referer": BASE_URL,
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _extract_viewstate(soup: BeautifulSoup) -> dict:
    """Pull all hidden ASP.NET state fields from the current page."""
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                 "__EVENTTARGET", "__EVENTARGUMENT", "__LASTFOCUS"]:
        el = soup.find("input", {"name": name})
        if el:
            fields[name] = el.get("value", "")
        else:
            fields[name] = ""  # Must always be present in POST body
    return fields


def _find_select(soup: BeautifulSoup, keyword: str):
    """
    Find a <select> element whose id or name contains keyword (case-insensitive).
    Returns the element or None.
    """
    keyword = keyword.lower()
    for sel in soup.find_all("select"):
        sel_id = (sel.get("id") or "").lower()
        sel_name = (sel.get("name") or "").lower()
        if keyword in sel_id or keyword in sel_name:
            return sel
    return None


def _option_value(select_el, text_fragment: str) -> str | None:
    """
    Find the value of an <option> whose text contains text_fragment (case-insensitive).
    Returns None if not found.
    """
    if select_el is None:
        return None
    text_fragment = text_fragment.lower()
    for opt in select_el.find_all("option"):
        if text_fragment in opt.text.strip().lower():
            return opt.get("value")
    return None


def _do_postback(session: requests.Session, soup: BeautifulSoup,
                 event_target: str, extra_fields: dict) -> BeautifulSoup:
    """
    Simulate an ASP.NET __doPostBack call.
    event_target: the 'name' attribute of the dropdown that changed (dot-notation).
    extra_fields: any additional form fields to include (e.g., the changed dropdown's value).
    """
    payload = _extract_viewstate(soup)
    payload["__EVENTTARGET"] = event_target
    payload["__EVENTARGUMENT"] = ""
    payload.update(extra_fields)

    resp = session.post(BASE_URL, data=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


# ─── Core scrape function ─────────────────────────────────────────────────────

def scrape_batches(region: str, pou: str, course: str) -> list[dict]:
    """
    Navigate the ICAI batch page and return a list of batch dicts for
    the specified region / PoU / course combination.

    Each dict contains whatever columns the batch table exposes, e.g.:
      { "batch_no": "...", "start_date": "...", "end_date": "...",
        "venue": "...", "seats": "...", "status": "..." }

    Returns [] if no batches found (could mean none available OR page structure changed).
    """
    session = requests.Session()
    session.headers.update({"User-Agent": HEADERS["User-Agent"]})

    # ── Step 1: Initial page load ─────────────────────────────────────────────
    logger.info("Loading ICAI batch page...")
    resp = session.get(BASE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # Discover the actual <select> elements dynamically (robust to control ID changes)
    region_sel = _find_select(soup, "region")
    if region_sel is None:
        raise ValueError("Could not find Region dropdown on page. Site may have changed layout.")

    region_name = region_sel.get("name")  # e.g. "ctl00$ContentPlaceHolder1$ddlRegion"
    region_value = _option_value(region_sel, region)
    if region_value is None:
        available = [o.text.strip() for o in region_sel.find_all("option")]
        raise ValueError(f"Region '{region}' not found. Available: {available}")

    logger.info(f"Region '{region}' → value={region_value}, field={region_name}")

    # ── Step 2: Select Region (triggers PoU population) ───────────────────────
    soup = _do_postback(session, soup,
                        event_target=region_name,
                        extra_fields={region_name: region_value})

    # ── Step 3: Find PoU dropdown (now populated) ─────────────────────────────
    pou_sel = _find_select(soup, "pou") or _find_select(soup, "branch") or _find_select(soup, "centre")
    if pou_sel is None:
        raise ValueError("Could not find PoU dropdown after selecting Region.")

    pou_name = pou_sel.get("name")
    pou_value = _option_value(pou_sel, pou)
    if pou_value is None:
        available = [o.text.strip() for o in pou_sel.find_all("option")]
        raise ValueError(f"PoU '{pou}' not found. Available: {available}")

    logger.info(f"PoU '{pou}' → value={pou_value}, field={pou_name}")

    # ── Step 4: Select PoU ────────────────────────────────────────────────────
    soup = _do_postback(session, soup,
                        event_target=pou_name,
                        extra_fields={
                            region_name: region_value,
                            pou_name: pou_value,
                        })

    # ── Step 5: Find Course dropdown ──────────────────────────────────────────
    course_sel = _find_select(soup, "course")
    if course_sel is None:
        raise ValueError("Could not find Course dropdown after selecting PoU.")

    course_name = course_sel.get("name")
    course_value = _option_value(course_sel, course)
    if course_value is None:
        available = [o.text.strip() for o in course_sel.find_all("option")]
        raise ValueError(f"Course '{course}' not found. Available: {available}")

    logger.info(f"Course '{course}' → value={course_value}, field={course_name}")

    # ── Step 6: Select Course → get batch results ─────────────────────────────
    soup = _do_postback(session, soup,
                        event_target=course_name,
                        extra_fields={
                            region_name: region_value,
                            pou_name: pou_value,
                            course_name: course_value,
                        })

    # ── Step 7: Parse batch table ─────────────────────────────────────────────
    batches = _parse_batch_table(soup)
    logger.info(f"Found {len(batches)} batch(es)")
    return batches


def _parse_batch_table(soup: BeautifulSoup) -> list[dict]:
    """
    Extract all batch rows from the results table.
    Strategy: find the first table with more than 1 row and parse it generically
    so the code doesn't break if ICAI changes column order.
    """
    batches = []

    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # First row = headers
        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        if not headers:
            continue

        # Check it looks like a batch table (has at least one date-like or batch-like header)
        header_text = " ".join(headers).lower()
        if not any(k in header_text for k in ["batch", "date", "start", "venue", "seat", "status"]):
            continue

        # Parse data rows
        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            row_data = {}
            for i, header in enumerate(headers):
                if i < len(cells):
                    row_data[header] = cells[i].get_text(strip=True)
            if row_data:
                batches.append(row_data)

        # Take the first matching table only
        if batches:
            break

    return batches


# ─── State comparison ─────────────────────────────────────────────────────────

def compute_hash(batches: list[dict]) -> str:
    """Stable hash of the batch list for change detection."""
    serialized = json.dumps(batches, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode()).hexdigest()


def has_changed(batches: list[dict], state_file: str = "state.json") -> tuple[bool, dict]:
    """
    Compare current batches against last saved state.
    Returns (changed: bool, old_state: dict).
    """
    new_hash = compute_hash(batches)

    try:
        with open(state_file, "r") as f:
            old_state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        old_state = {}

    old_hash = old_state.get("hash", "")
    changed = new_hash != old_hash
    return changed, old_state


def save_state(batches: list[dict], state_file: str = "state.json"):
    """Persist current batch data and its hash."""
    state = {
        "hash": compute_hash(batches),
        "batches": batches,
    }
    with open(state_file, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
