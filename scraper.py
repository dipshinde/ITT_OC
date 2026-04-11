"""
scraper.py
----------
Handles the full ASP.NET WebForms postback chain for the ICAI batch listing page.

FIX: compute_hash now sorts the batch list before hashing so that identical
     batches returned in different row order do NOT trigger false change alerts.

FIX: Diagnostic dump functions (_dump_all_selects, _dump_page_snippet) now
     emit at DEBUG level so Railway logs aren't flooded every 60 seconds.
     Set LOG_LEVEL=DEBUG in your Railway env vars to re-enable them.
"""

import hashlib
import json
import logging
import time

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://www.icaionlineregistration.org/launchbatchdetail.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":                  "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language":         "en-IN,en-GB;q=0.9,en;q=0.8",
    "Accept-Encoding":         "gzip, deflate, br",
    "Connection":              "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control":           "max-age=0",
}


# ─── Diagnostic helpers (DEBUG level — silent in production) ──────────────────

def _dump_all_selects(soup: BeautifulSoup, label: str):
    """Log every <select> found on the page — useful for debugging field changes."""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    selects = soup.find_all("select")
    logger.debug(f"[{label}] Found {len(selects)} <select> element(s) on page:")
    for sel in selects:
        options = [f"'{o.get('value')}' → {o.text.strip()}" for o in sel.find_all("option")]
        logger.debug(f"  id={sel.get('id')!r}  name={sel.get('name')!r}")
        logger.debug(f"    Options: {options}")


def _dump_page_snippet(soup: BeautifulSoup, label: str):
    """Log a snippet of the page body — useful for diagnosing blank/error responses."""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    body = soup.find("body")
    text = body.get_text(separator=" ", strip=True)[:600] if body else str(soup)[:600]
    logger.debug(f"[{label}] Page text snippet: {text}")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _extract_viewstate(soup: BeautifulSoup) -> dict:
    """Pull all hidden ASP.NET state fields from the current page."""
    fields = {}
    for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                 "__EVENTTARGET", "__EVENTARGUMENT", "__LASTFOCUS"]:
        el = soup.find("input", {"name": name})
        fields[name] = el.get("value", "") if el else ""
    vs_len = len(fields.get("__VIEWSTATE", ""))
    logger.debug(f"  ViewState length: {vs_len} chars {'✓' if vs_len > 100 else '⚠ VERY SHORT'}")
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
                logger.debug(f"  Found dropdown for keyword '{keyword}': id={sel.get('id')!r} name={sel.get('name')!r}")
                return sel
    return None


def _find_select_by_index(soup: BeautifulSoup, index: int):
    """Fallback: get the Nth <select> on the page (0-based)."""
    selects = soup.find_all("select")
    if index < len(selects):
        sel = selects[index]
        logger.debug(f"  Fallback: using select[{index}] → id={sel.get('id')!r} name={sel.get('name')!r}")
        return sel
    return None


def _option_value(select_el, text_fragment: str):
    """
    Find the value of an <option> whose text contains text_fragment (case-insensitive).
    Logs all available options at DEBUG level if not found.
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
    """Simulate an ASP.NET __doPostBack call."""
    logger.debug(f"  POSTing for step: {step_label} | __EVENTTARGET={event_target!r}")

    payload = _extract_viewstate(soup)
    payload["__EVENTTARGET"]   = event_target
    payload["__EVENTARGUMENT"] = ""
    payload.update(extra_fields)

    headers = {
        **HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin":   "https://www.icaionlineregistration.org",
        "Referer":  BASE_URL,
    }

    time.sleep(0.5)   # polite delay — 0.5 s is sufficient; ICAI doesn't rate-limit hard
    resp = session.post(BASE_URL, data=payload, headers=headers, timeout=30)
    logger.debug(f"  Response: HTTP {resp.status_code}  ({len(resp.text)} chars)")
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
        "User-Agent":      HEADERS["User-Agent"],
        "Accept-Language": HEADERS["Accept-Language"],
    })

    # ── Step 1: Initial GET ───────────────────────────────────────────────────
    logger.info(f"Scraping: {region} / {pou} / {course}")
    resp = session.get(BASE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    _dump_all_selects(soup, "initial page")
    _dump_page_snippet(soup, "initial page")

    # ── Step 2: Region dropdown ───────────────────────────────────────────────
    region_sel = (
        _find_select_by_keywords(soup, "region") or
        _find_select_by_index(soup, 0)
    )
    if region_sel is None:
        raise ValueError("No <select> elements found at all — page structure may have changed.")

    region_field = region_sel.get("name")
    region_value = _option_value(region_sel, region)
    if region_value is None:
        raise ValueError(f"Region '{region}' not found in dropdown '{region_field}'")

    soup = _do_postback(session, soup,
                        event_target=region_field,
                        extra_fields={region_field: region_value},
                        step_label=f"Select Region={region}")
    _dump_all_selects(soup, "after region select")

    # ── Step 3: PoU dropdown ──────────────────────────────────────────────────
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

    soup = _do_postback(session, soup,
                        event_target=pou_field,
                        extra_fields={region_field: region_value, pou_field: pou_value},
                        step_label=f"Select PoU={pou}")
    _dump_all_selects(soup, "after PoU select")

    # ── Step 4: Course dropdown ───────────────────────────────────────────────
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

    soup = _do_postback(session, soup,
                        event_target=course_field,
                        extra_fields={
                            region_field: region_value,
                            pou_field:    pou_value,
                            course_field: course_value,
                        },
                        step_label=f"Select Course={course}")

    # ── Step 4.5: Click "Get List" button ────────────────────────────────────
    payload = _extract_viewstate(soup)
    payload.pop("__EVENTTARGET",   None)
    payload.pop("__EVENTARGUMENT", None)
    payload[region_field]  = region_value
    payload[pou_field]     = pou_value
    payload[course_field]  = course_value
    payload["btn_getlist"] = "Get List"

    headers = {
        **HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin":   "https://www.icaionlineregistration.org",
        "Referer":  BASE_URL,
    }
    time.sleep(0.5)
    resp = session.post(BASE_URL, data=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "lxml")

    # ── Step 5: Parse results ─────────────────────────────────────────────────
    _dump_page_snippet(soup, "final results page")
    batches = _parse_batch_table(soup)
    logger.info(f"  → Found {len(batches)} batch(es) for {region}/{pou}/{course}")
    return batches


def _parse_batch_table(soup: BeautifulSoup) -> list[dict]:
    """
    Extract all batch rows from the results table.
    Tries multiple strategies to find the right table.
    """
    batches = []
    tables  = soup.find_all("table")
    logger.debug(f"  Tables on page: {len(tables)}")

    for idx, table in enumerate(tables):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        if not headers:
            continue

        header_text = " ".join(headers).lower()
        logger.debug(f"  Table[{idx}] headers: {headers}")

        batch_keywords = ["batch", "date", "start", "venue", "seat", "status",
                          "from", "to", "available", "schedule"]
        if not any(k in header_text for k in batch_keywords):
            continue

        for row in rows[1:]:
            cells = row.find_all("td")
            if not cells:
                continue
            row_data = {
                headers[i]: cells[i].get_text(strip=True)
                for i in range(min(len(headers), len(cells)))
            }
            if any(v.strip() for v in row_data.values()):
                batches.append(row_data)

        if batches:
            logger.debug(f"  Using table[{idx}] with {len(batches)} data row(s)")
            break

    return batches


# ─── State helpers ────────────────────────────────────────────────────────────

def compute_hash(batches: list[dict]) -> str:
    """
    Stable hash of a batch list.

    FIX: The list is sorted by (Batch No, From Date) before serialising so
    that identical batches returned in a different row order by ICAI's server
    do NOT produce a different hash and therefore do NOT trigger false
    'Batch Update' alerts.
    """
    sorted_batches = sorted(
        batches,
        key=lambda b: (
            str(b.get("Batch No",   b.get("BatchNo",   ""))),
            str(b.get("From Date",  b.get("FromDate",  ""))),
        )
    )
    serialized = json.dumps(sorted_batches, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialized.encode()).hexdigest()
