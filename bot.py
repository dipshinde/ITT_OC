"""
bot.py
------
Core Telegram bot logic for the ICAI Batch Monitor.

Responsibilities:
  - User onboarding flow: Region -> PoU -> Course (inline keyboard)
  - Command handling: /start /watch /status /stop /registered /help
  - scrape_and_alert(): called by background monitor thread every 60 s
  - Seat-threshold alerts: notifies at 15 / 10 / 5 / 1 seats remaining
  - State persistence: users.json (user prefs + Telegram offset)
                       state.json (batch hashes + seat alert history)

Thread safety: all file I/O should be wrapped in STATE_LOCK (defined here,
used by main.py and background monitor).
"""

import os
import json
import logging
import threading
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from scraper import scrape_batches, compute_hash

# --- Logging -----------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Config ------------------------------------------------------------------

TOKEN    = os.environ["TELEGRAM_BOT_TOKEN"]
API_BASE = f"https://api.telegram.org/bot{TOKEN}"
ICAI_URL = "https://www.icaionlineregistration.org/launchbatchdetail.aspx"
ICAI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"
}

USERS_FILE = "users.json"
STATE_FILE = "state.json"

# Seat thresholds at which to send a special low-seat alert (descending)
SEAT_THRESHOLDS = [15, 10, 5, 1]

COURSES = [
    "Advanced (ICITSS) MCS Course",
    "Advanced (ICITSS) MCS Course - Weekend",
    "AICITSS - Advanced Information Technology",
    "ICITSS - Information Technology",
    "ICITSS - Orientation Course",
]

# Shared lock — import this in main.py to synchronise file access
STATE_LOCK = threading.Lock()

# --- Telegram helpers ---------------------------------------------------------

def tg(method: str, **data):
    try:
        r = requests.post(f"{API_BASE}/{method}", json=data, timeout=15)
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Telegram API error ({method}): {e}")
        return {}

def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = markup
    return tg("sendMessage", **payload)

def edit(chat_id, message_id, text, markup=None):
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
    }
    if markup:
        payload["reply_markup"] = markup
    return tg("editMessageText", **payload)

def answer_cb(cb_id, text="OK"):
    tg("answerCallbackQuery", callback_query_id=cb_id, text=text)

def ikb(rows):
    """Build an inline keyboard. rows = list of list of (label, callback_data)."""
    return {
        "inline_keyboard": [
            [{"text": t, "callback_data": d} for t, d in row]
            for row in rows
        ]
    }

# --- ICAI helpers -------------------------------------------------------------

def fetch_regions():
    """Return list of (label, value) for the Region dropdown."""
    try:
        r = requests.get(ICAI_URL, headers=ICAI_HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "lxml")
        sel = soup.find("select", {"id": "ddl_reg"})
        if not sel:
            return []
        return [
            (o.text.strip(), o["value"])
            for o in sel.find_all("option")
            if o.get("value") and o["value"] != "Select"
        ]
    except Exception as e:
        logger.error(f"fetch_regions failed: {e}")
        return []

def fetch_pous(region_value: str):
    """POST region selection and return PoU options as (label, value)."""
    try:
        r0 = requests.get(ICAI_URL, headers=ICAI_HEADERS, timeout=20)
        soup0 = BeautifulSoup(r0.text, "lxml")

        def vs(name):
            el = soup0.find("input", {"name": name})
            return el["value"] if el else ""

        payload = {
            "__VIEWSTATE":          vs("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": vs("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION":    vs("__EVENTVALIDATION"),
            "__EVENTTARGET":        "ddl_reg",
            "__EVENTARGUMENT":      "",
            "ddl_reg":              region_value,
        }
        headers = {
            **ICAI_HEADERS,
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": ICAI_URL,
        }
        r1 = requests.post(ICAI_URL, data=payload, headers=headers, timeout=20)
        soup1 = BeautifulSoup(r1.text, "lxml")
        sel = soup1.find("select", {"id": "ddlPou"})
        if not sel:
            return []
        return [
            (o.text.strip(), o["value"])
            for o in sel.find_all("option")
            if o.get("value")
        ]
    except Exception as e:
        logger.error(f"fetch_pous failed: {e}")
        return []

# --- State (users.json + state.json) ------------------------------------------

def load_state() -> dict:
    """Load Telegram offset + user preferences from users.json."""
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"_offset": 0, "users": {}}

def save_state(state: dict):
    """Persist Telegram offset + user preferences to users.json."""
    try:
        with open(USERS_FILE, "w") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except IOError as e:
        logger.error(f"save_state failed: {e}")

def load_batch_state() -> dict:
    """Load previously seen batch hashes + seat alert history from state.json."""
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}

def save_batch_state(batch_state: dict):
    """Persist batch hashes + seat alert history to state.json."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(batch_state, f, indent=2, ensure_ascii=False)
    except IOError as e:
        logger.error(f"save_batch_state failed: {e}")

# --- Setup flow helpers -------------------------------------------------------

def start_setup(chat_id: str, state: dict, message_id=None):
    """Begin the Region -> PoU -> Course selection flow."""
    regions = fetch_regions()
    if not regions:
        send(chat_id, "Could not reach the ICAI site. Please try again in a minute.")
        return

    state["users"].setdefault(chat_id, {})
    state["users"][chat_id]["pending"] = {
        "step": "region",
        "region_map": {label: val for label, val in regions},
    }

    rows = [
        [regions[i], regions[i + 1]] if i + 1 < len(regions) else [regions[i]]
        for i in range(0, len(regions), 2)
    ]
    markup = ikb([[(label, f"region:{label}") for label, _ in row] for row in rows])
    text = "Welcome! Let's set up your batch alert.\n\n<b>Step 1 of 3 — Select your Region:</b>"
    if message_id:
        edit(chat_id, message_id, text, markup)
    else:
        send(chat_id, text, markup)


def ask_pou(chat_id: str, region_label: str, state: dict, message_id: int):
    pending      = state["users"][chat_id].get("pending", {})
    region_map   = pending.get("region_map", {})
    region_value = region_map.get(region_label)

    if not region_value:
        send(chat_id, "Region not recognised. Type /watch to restart.")
        return

    pous = fetch_pous(region_value)
    if not pous:
        send(chat_id, "Could not fetch city list. Type /watch to try again.")
        return

    state["users"][chat_id]["pending"] = {
        "step":         "pou",
        "region_label": region_label,
        "region_value": region_value,
        "pou_map":      {label: val for label, val in pous},
    }

    rows   = [[pous[i], pous[i + 1]] if i + 1 < len(pous) else [pous[i]] for i in range(0, len(pous), 2)]
    markup = ikb([[(label, f"pou:{label}") for label, _ in row] for row in rows])
    edit(chat_id, message_id,
         f"<b>Step 2 of 3 — Select your City/PoU</b>\n(Region: {region_label})",
         markup)


def ask_course(chat_id: str, pou_label: str, state: dict, message_id: int):
    pending   = state["users"][chat_id].get("pending", {})
    pou_map   = pending.get("pou_map", {})
    pou_value = pou_map.get(pou_label)

    if not pou_value:
        send(chat_id, "City not recognised. Type /watch to restart.")
        return

    state["users"][chat_id]["pending"]["step"]      = "course"
    state["users"][chat_id]["pending"]["pou_label"] = pou_label
    state["users"][chat_id]["pending"]["pou_value"] = pou_value

    markup = ikb([[(c, f"course:{c}")] for c in COURSES])
    edit(chat_id, message_id,
         f"<b>Step 3 of 3 — Select your Course</b>\n(City: {pou_label})",
         markup)


def confirm_subscription(chat_id: str, course: str, state: dict, message_id: int):
    pending      = state["users"][chat_id].get("pending", {})
    region_label = pending.get("region_label")
    pou_label    = pending.get("pou_label")

    # Save subscription (clear pending, mark active, clear registered flag)
    state["users"][chat_id] = {
        "region":     region_label,
        "pou":        pou_label,
        "course":     course,
        "active":     True,
        "registered": False,
    }

    # Update the message first with a loading state
    edit(
        chat_id,
        message_id,
        f"<b>You're all set!</b>\n\n"
        f"Region : {region_label}\n"
        f"City    : {pou_label}\n"
        f"Course  : {course}\n\n"
        f"⏳ Fetching current batch details...",
    )

    # Immediately scrape and show current batch status to the new user
    try:
        batches = scrape_batches(region_label, pou_label, course)
        if batches:
            body = format_batches(batches)
            send(
                chat_id,
                f"<b>📋 Current Batches for {course}</b>\n"
                f"Region: {region_label} / {pou_label}\n\n"
                f"{body}\n\n"
                f"<a href='https://www.icaionlineregistration.org/launchbatchdetail.aspx'>"
                f"Register on ICAI portal</a>\n\n"
                f"I'm monitoring continuously and will alert you when seats drop to "
                f"<b>15, 10, 5, and 1</b>.\n"
                f"Once you've registered, send /registered so I stop notifying you.",
            )
        else:
            send(
                chat_id,
                f"<b>No batches listed yet</b> for {course} in {pou_label}.\n\n"
                f"I'm monitoring continuously — you'll be alerted the moment batches appear, "
                f"and again at <b>15, 10, 5, and 1</b> seats remaining.\n\n"
                f"Once you've registered, send /registered.",
            )
    except Exception as e:
        logger.error(f"Initial scrape for {chat_id} failed: {e}", exc_info=True)
        send(
            chat_id,
            "Could not fetch current batch data right now, but I'm watching continuously.\n"
            "You'll be alerted automatically when batches appear or seats change.",
        )

# --- Command handlers ---------------------------------------------------------

def handle_message(msg: dict, state: dict):
    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text", "").strip()

    if text.startswith("/start") or text.startswith("/watch"):
        start_setup(chat_id, state)

    elif text.startswith("/status"):
        u = state["users"].get(chat_id, {})
        if u.get("registered"):
            send(chat_id, "You've already marked yourself as registered. Use /watch to monitor a new batch.")
        elif u.get("active"):
            send(
                chat_id,
                f"<b>Currently watching:</b>\n\n"
                f"Region: {u['region']} / {u['pou']}\n"
                f"Course: {u['course']}\n\n"
                f"Alerts fire at 15 / 10 / 5 / 1 seats remaining.\n"
                f"Use /watch to change, /stop to pause, or /registered once you've enrolled.",
            )
        else:
            send(chat_id, "No active watch. Use /watch to set one up.")

    elif text.startswith("/stop"):
        if chat_id in state["users"]:
            state["users"][chat_id]["active"] = False
        send(chat_id, "Alerts paused. Use /watch anytime to resubscribe.")

    elif text.lower().startswith("/registered") or text.lower() == "registered":
        _handle_registered(chat_id, state)

    elif text.startswith("/help"):
        send(
            chat_id,
            "<b>ICAI Batch Monitor Bot</b>\n\n"
            "/watch       — set up or change your batch alert\n"
            "/status      — see your current watch\n"
            "/stop        — pause alerts\n"
            "/registered  — I've enrolled! Stop notifying me\n"
            "/help        — this message\n\n"
            "<i>You'll be alerted automatically when seats drop to 15, 10, 5, and 1.</i>",
        )

    else:
        send(chat_id, "Use /watch to set up your batch alert, or /help for commands.")


def _handle_registered(chat_id: str, state: dict):
    """User has successfully registered — deactivate their watchlist entry."""
    u = state["users"].get(chat_id, {})
    if not u or not u.get("active"):
        send(chat_id, "You don't have an active watch to close. Use /watch to set one up.")
        return

    region = u.get("region", "")
    pou    = u.get("pou", "")
    course = u.get("course", "")

    # Mark as registered & inactive
    state["users"][chat_id] = {
        "region":     region,
        "pou":        pou,
        "course":     course,
        "active":     False,
        "registered": True,
    }

    # Clean up seat-alert history for this combo so it's fresh on next /watch
    key = _make_key(region, pou, course)
    try:
        batch_state = load_batch_state()
        if key in batch_state:
            batch_state[key].pop("seat_alerts_sent", None)
            save_batch_state(batch_state)
    except Exception as e:
        logger.warning(f"Could not clean seat alert state for {key}: {e}")

    send(
        chat_id,
        "🎉 <b>Congratulations on registering!</b>\n\n"
        "Your watchlist is now closed — no more seat alerts for this batch.\n\n"
        "Use /watch anytime to monitor another batch.",
    )


def handle_callback(cb: dict, state: dict):
    chat_id    = str(cb["message"]["chat"]["id"])
    message_id = cb["message"]["message_id"]
    data       = cb.get("data", "")
    cb_id      = cb["id"]

    answer_cb(cb_id)
    state["users"].setdefault(chat_id, {})

    if data.startswith("region:"):
        ask_pou(chat_id, data[len("region:"):], state, message_id)
    elif data.startswith("pou:"):
        ask_course(chat_id, data[len("pou:"):], state, message_id)
    elif data.startswith("course:"):
        confirm_subscription(chat_id, data[len("course:"):], state, message_id)

# --- Poll Telegram updates ----------------------------------------------------

def process_updates(state: dict):
    """Fetch pending Telegram updates and dispatch to handlers."""
    offset = state.get("_offset", 0)
    try:
        resp = requests.get(
            f"{API_BASE}/getUpdates",
            params={"offset": offset, "timeout": 5},
            timeout=20,
        )
        updates = resp.json().get("result", [])
    except requests.RequestException as e:
        logger.error(f"getUpdates failed: {e}")
        return

    for upd in updates:
        state["_offset"] = upd["update_id"] + 1
        if "message" in upd:
            try:
                handle_message(upd["message"], state)
            except Exception as e:
                logger.error(f"Error handling message: {e}", exc_info=True)
        elif "callback_query" in upd:
            try:
                handle_callback(upd["callback_query"], state)
            except Exception as e:
                logger.error(f"Error handling callback: {e}", exc_info=True)

# --- Batch formatting ---------------------------------------------------------

def format_batches(batches: list) -> str:
    if not batches:
        return "No batches currently listed."
    lines = []
    for b in batches:
        seats    = b.get("Available Seats", b.get("AvailableSeats", "?"))
        batch_no = b.get("Batch No", b.get("BatchNo", "?"))
        from_d   = b.get("From Date", b.get("FromDate", ""))
        to_d     = b.get("To Date",   b.get("ToDate",   ""))
        timing   = b.get("Batch Time", b.get("BatchTime", ""))
        lines.append(
            f"  <b>{batch_no}</b>\n"
            f"     {from_d} to {to_d}  |  {timing}\n"
            f"     Seats available: <b>{seats}</b>"
        )
    return "\n\n".join(lines)

# --- Seat threshold helpers ---------------------------------------------------

def _seats_int(batch: dict):
    """Return available seats as int, or None if not parseable."""
    raw = batch.get("Available Seats", batch.get("AvailableSeats", ""))
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return None


def _new_threshold_fires(batch: dict, already_sent: list) -> list:
    """
    Return thresholds that should fire now but haven't been sent yet.
    A threshold fires once when seats <= threshold.
    """
    seats = _seats_int(batch)
    if seats is None:
        return []
    return [t for t in SEAT_THRESHOLDS if seats <= t and t not in already_sent]


# --- Scrape and alert (called by background monitor thread) -------------------

def _make_key(region: str, pou: str, course: str) -> str:
    return f"{region}|{pou}|{course}"


def scrape_and_alert(state: dict):
    """
    For every active (non-registered) subscribed user:
      1. Scrape current batches from ICAI
      2. Alert on any hash change (new/updated batches)
      3. Independently check seat thresholds (15/10/5/1) and alert per batch
      4. Skip users who have sent /registered
      5. Persist updated state to state.json
    """
    users       = state.get("users", {})
    batch_state = load_batch_state()

    # Group users by (region, pou, course) — skip inactive / registered
    watchlist: dict = {}
    for chat_id, u in users.items():
        if not u.get("active"):
            continue
        if u.get("registered"):
            continue
        if "pending" in u:
            continue
        if not (u.get("region") and u.get("pou") and u.get("course")):
            continue
        key = _make_key(u["region"], u["pou"], u["course"])
        watchlist.setdefault(key, []).append(chat_id)

    for key, chat_ids in watchlist.items():
        region, pou, course = key.split("|", 2)
        logger.info(f"Checking: {region} / {pou} / {course}  ({len(chat_ids)} subscriber(s))")

        try:
            batches  = scrape_batches(region, pou, course)
            new_hash = compute_hash(batches)
        except Exception as e:
            logger.error(f"Scrape failed for {key}: {e}", exc_info=True)
            continue

        old_entry        = batch_state.get(key, {})
        old_hash         = old_entry.get("hash", "")
        old_batch_nos    = {b.get("Batch No", "") for b in old_entry.get("batches", [])}
        seat_alerts_sent = old_entry.get("seat_alerts_sent", {})  # {batch_no: [thresholds]}

        is_first = (old_hash == "")
        changed  = (new_hash != old_hash)

        # ── 1. Build seat-threshold alerts ────────────────────────────────────
        threshold_msgs = []
        for b in batches:
            batch_no = str(b.get("Batch No", b.get("BatchNo", "unknown")))
            already  = seat_alerts_sent.get(batch_no, [])
            fires    = _new_threshold_fires(b, already)
            if fires:
                seats  = _seats_int(b)
                from_d = b.get("From Date", b.get("FromDate", ""))
                to_d   = b.get("To Date",   b.get("ToDate",   ""))
                timing = b.get("Batch Time", b.get("BatchTime", ""))
                threshold_msgs.append(
                    f"⚠️ <b>Only {seats} seat(s) left!</b>\n"
                    f"  Batch <b>{batch_no}</b>  |  {from_d} to {to_d}  |  {timing}"
                )
                seat_alerts_sent.setdefault(batch_no, []).extend(fires)

        # ── 2. Persist updated state ──────────────────────────────────────────
        batch_state[key] = {
            "hash":             new_hash,
            "batches":          batches,
            "last_checked":     datetime.now(timezone.utc).isoformat(),
            "region":           region,
            "pou":              pou,
            "course":           course,
            "seat_alerts_sent": seat_alerts_sent,
        }

        # ── 3. Change-based notifications ────────────────────────────────────
        if changed or is_first:
            new_batch_nos   = {b.get("Batch No", "") for b in batches}
            added_batch_nos = new_batch_nos - old_batch_nos
            newly_added     = [b for b in batches if b.get("Batch No", "") in added_batch_nos]
            batches_with_seats = [
                b for b in batches
                if str(b.get("Available Seats", b.get("AvailableSeats", "0"))) not in ("0", "")
            ]

            if is_first and not batches:
                logger.info("  First run — no batches yet, baseline saved")
            else:
                if batches_with_seats:
                    header = "🔔 <b>ICAI Batch Update — Seats Available!</b>"
                    body   = format_batches(batches_with_seats)
                elif newly_added:
                    header = "🔔 <b>New ICAI Batches Added</b> (no seats open yet)"
                    body   = format_batches(newly_added)
                else:
                    header = "🔔 <b>ICAI Batch Update</b>"
                    body   = format_batches(batches)

                change_msg = (
                    f"{header}\n\n"
                    f"Region : {region} / {pou}\n"
                    f"Course : {course}\n\n"
                    f"{body}\n\n"
                    f"<a href='https://www.icaionlineregistration.org/launchbatchdetail.aspx'>"
                    f"Register on ICAI portal →</a>\n\n"
                    f"Send /registered once you've enrolled."
                )
                logger.info(f"  Alerting {len(chat_ids)} user(s) — batch change detected")
                for chat_id in chat_ids:
                    send(chat_id, change_msg)
        else:
            logger.info(f"  No change ({len(batches)} batch(es))")

        # ── 4. Seat-threshold notifications ───────────────────────────────────
        if threshold_msgs:
            threshold_alert = (
                f"🚨 <b>Low Seat Alert</b>\n"
                f"Region: {region} / {pou}\n"
                f"Course: {course}\n\n"
                + "\n\n".join(threshold_msgs) +
                f"\n\n<a href='https://www.icaionlineregistration.org/launchbatchdetail.aspx'>"
                f"Register NOW on ICAI portal →</a>\n\n"
                f"Send /registered once you've enrolled."
            )
            logger.info(f"  Seat-threshold alert → {len(chat_ids)} user(s)")
            for chat_id in chat_ids:
                send(chat_id, threshold_alert)

    save_batch_state(batch_state)
