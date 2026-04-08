"""
scraper.py
----------
Handles the full ASP.NET WebForms postback chain for the ICAI batch listing page.
Includes verbose diagnostic logging so failures are readable in GitHub Actions.
"""

import requests
from bs4 import BeautifulSoup
import hashlib
import json
import logging
import time

logger = logging.getLogger(__name__)

BASE_URL = "https://www.icaionlineregistration.org/launchbatchdetail.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IN,en-GB;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}


# ─── Diagnostic helpers ───────────────────────────────────────────────────────

def _dump_all_selects(soup: BeautifulSoup, label: str):
    """Log every <select> found on the page — critical for debugging field names."""
    selects = soup.find_all("select")
    logger.info(f"[{label}] Found {len(selects)} <select> element(s) on page:")
    for sel in selects:
        options = [f"'{o.get('value')}' → {o.text.strip()}" for o in sel.find_all("option")]
        logger.info(f"  id={sel.get('id')!r}  name={sel.get('name')!r}")
        logger.info(f"    Options: {options}")


def _dump_page_snippet(soup: BeautifulSoup, label: str):
    """Log a snippet of the page body for debugging blank/error responses."""
    body = soup.find("body")
    text = body.get_text(separator=" ", strip=True)[:600] if body else str(soup)[:600]
    logger.info(f"[{label}] Page text snippet: {text}")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _extract_viewstate(soup: BeautifulSoup) -> dict:
    """Pull all hidden ASP.NET state fields from the current page."""
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                 "__EVENTTARGET", "__EVENTARGUMENT", "__LASTFOCUS"]:
        el = soup.find("input", {"name": name})
        fields[name] = el.get("value", "") if el else ""
    vs_len = len(fields.get("__VIEWSTATE", ""))
    logger.info(f"  ViewState length: {vs_len} chars {'✓' if vs_len > 100 else '⚠ VERY SHORT — possible issue'}")
    return fields


def _find_select_by_keywords(soup: BeautifulSoup, *keywords):
    """
    Find a <select> whose id or name contains ANY of the keywords (case-insensitive).
    """
    for keyword in keywords:
        kw = keyword.lower()
        for sel in soup.find_all("select"):
            sel_id   = (sel.get("id")   or "").lower()
            sel_name = (sel.get("name") or "").lower()
            if kw in sel_id or kw in sel_name:
                logger.info(f"  Found dropdown for keyword '{keyword}': id={sel.get('id')!r} name={sel.get('name')!r}")
                return sel
    return None


def _find_select_by_index(soup: BeautifulSoup, index: int):
    """Fallback: get the Nth <select> on the page (0-based)."""
    selects = soup.find_all("select")
    if index < len(selects):
        sel = selects[index]
        logger.info(f"  Fallback: using select[{index}] → id={sel.get('id')!r} name={sel.get('name')!r}")
        return sel
    return None


def _option_value(select_el, text_fragment: str):
    """
    Find the value of an <option> whose text contains text_fragment (case-insensitive).
    Logs all available options if not found.
    """
    if select_el is None:
        return None
    text_fragment = text_fragment.lower()
    for opt in select_el.find_all("option"):
        if text_fragment in opt.text.strip().lower():
            return opt.get("value")
    available = [f"'{o.text.strip()}'" for o in select_el.find_all("option")]
    logger.warning(f"  '{text_fragment}' not found. Available options: {available}")
    return None


def _do_postback(session: requests.Session, soup: BeautifulSoup,
                 event_target: str, extra_fields: dict, step_label: str):
    """Simulate an ASP.NET __doPostBack call with full logging."""
    logger.info(f"  POSTing for step: {step_label} | __EVENTTARGET={event_target!r}")

    payload = _extract_viewstate(soup)
    payload["__EVENTTARGET"] = event_target
    payload["__EVENTARGUMENT"] = ""
    payload.update(extra_fields)

    headers = {**HEADERS, "Content-Type": "application/x-www-form-urlencoded",
               "Origin": "https://www.icaionlineregistration.org",
               "Referer": BASE_URL}

    time.sleep(1)  # polite delay between requests
    resp = session.post(BASE_URL, data=payload, headers=headers, timeout=30)
    logger.info(f"  Response: HTTP {resp.status_code}  ({len(resp.text)} chars)")
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


# ─── Core scrape function ─────────────────────────────────────────────────────

def scrape_batches(region: str, pou: str, course: str) -> list[dict]:
    """
    Navigate the ICAI batch page via the full dropdown postback chain.
    Returns a list of batch dicts (may be empty if none available).
    Raises on unrecoverable errors.
    """
    session = requests.Session()
    session.headers.update({
        "User-Agent": HEADERS["User-Agent"],
        "Accept-Language": HEADERS["Accept-Language"],
    })

    # ── Step 1: Initial GET ───────────────────────────────────────────────────
    logger.info("━━━ Step 1: Loading ICAI page (initial GET)")
    resp = session.get(BASE_URL, headers=HEADERS, timeout=30)
    logger.info(f"  HTTP {resp.status_code} | {len(resp.text)} chars")
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    _dump_all_selects(soup, "initial page")
    _dump_page_snippet(soup, "initial page")

    # ── Region dropdown ───────────────────────────────────────────────────────
    logger.info(f"\n━━━ Step 2: Selecting Region = '{region}'")
    region_sel = (
        _find_select_by_keywords(soup, "region") or
        _find_select_by_index(soup, 0)
    )
    if region_sel is None:
        raise ValueError("FATAL: No <select> elements found at all. Page may have failed to load or changed structure.")

    region_field = region_sel.get("name")
    region_value = _option_value(region_sel, region)
    if region_value is None:
        raise ValueError(f"Region '{region}' not found in dropdown '{region_field}'")

    logger.info(f"  → field={region_field!r}  value={region_value!r}")
    soup = _do_postback(session, soup,
                        event_target=region_field,
                        extra_fields={region_field: region_value},
                        step_label=f"Select Region={region}")
    _dump_all_selects(soup, "after region select")

    # ── PoU dropdown ──────────────────────────────────────────────────────────
    logger.info(f"\n━━━ Step 3: Selecting PoU = '{pou}'")
    pou_sel = (
        _find_select_by_keywords(soup, "pou", "branch", "centre", "city", "place", "unit") or
        _find_select_by_index(soup, 1)
    )
    if pou_sel is None:
        _dump_page_snippet(soup, "after region select — PoU not found")
        raise ValueError("Could not find PoU dropdown after selecting Region.")

    pou_field = pou_sel.get("name")
    pou_value = _option_value(pou_sel, pou)
    if pou_value is None:
        raise ValueError(f"PoU '{pou}' not found in dropdown '{pou_field}'")

    logger.info(f"  → field={pou_field!r}  value={pou_value!r}")
    soup = _do_postback(session, soup,
                        event_target=pou_field,
                        extra_fields={region_field: region_value, pou_field: pou_value},
                        step_label=f"Select PoU={pou}")
    _dump_all_selects(soup, "after PoU select")

    # ── Course dropdown ───────────────────────────────────────────────────────
    logger.info(f"\n━━━ Step 4: Selecting Course = '{course}'")
    course_sel = (
        _find_select_by_keywords(soup, "course", "batch", "program", "programme") or
        _find_select_by_index(soup, 2)
    )
    if course_sel is None:
        _dump_page_snippet(soup, "after PoU select — course not found")
        raise ValueError("Could not find Course dropdown after selecting PoU.")

    course_field = course_sel.get("name")
    course_value = _option_value(course_sel, course)
    if course_value is None:
        raise ValueError(f"Course '{course}' not found in dropdown '{course_field}'")

    logger.info(f"  → field={course_field!r}  value={course_value!r}")
    soup = _do_postback(session, soup,
                        event_target=course_field,
                        extra_fields={
                            region_field: region_value,
                            pou_field: pou_value,
                            course_field: course_value,
                        },
                        step_label=f"Select Course={course}")

    # ── Parse results ─────────────────────────────────────────────────────────
    logger.info("\n━━━ Step 5: Parsing batch results")
    _dump_page_snippet(soup, "final results page")
    batches = _parse_batch_table(soup)
    logger.info(f"  → Found {len(batches)} batch(es)")
    return batches


def _parse_batch_table(soup: BeautifulSoup) -> list[dict]:
    """
    Extract all batch rows from the results table.
    Tries multiple strategies to find the right table.
    """
    batches = []
    tables = soup.find_all("table")
    logger.info(f"  Tables on page: {len(tables)}")

    for idx, table in enumerate(tables):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        if not headers:
            continue

        header_text = " ".join(headers).lower()
        logger.info(f"  Table[{idx}] headers: {headers}")

        batch_keywords = ["batch", "date", "start", "venue", "seat", "status",
                          "from", "to", "available", "schedule"]
        if not any(k in header_text for k in batch_keywords):
            continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            row_data = {headers[i]: cells[i].get_text(strip=True)
                        for i in range(min(len(headers), len(cells)))}
            if any(v.strip() for v in row_data.values()):
                batches.append(row_data)

        if batches:
            logger.info(f"  Using table[{idx}] with {len(batches)} data row(s)")
            break

    return batches


# ─── State helpers ────────────────────────────────────────────────────────────

def compute_hash(batches: list[dict]) -> str:
    serialized = json.dumps(batches, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode()).hexdigest()
