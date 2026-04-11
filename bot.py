"""
bot.py
------
Core Telegram bot logic for the ICAI Batch Monitor.

Responsibilities:
  - User onboarding flow: Region -> PoU -> Course (inline keyboard)
  - Command handling: /start /watch /status /stop /registered /help
  - scrape_and_alert(): called by background monitor thread every 60 s
  - Seat-threshold alerts: notifies at 15 / 10 / 5 / 1 seats remaining
  - State persistence: MongoDB via db.py

Fixes applied in this version
──────────────────────────────
  1. MESSAGE QUEUE   — all outgoing Telegram sends go through a rate-limited
                       queue (max 1 msg / 0.05 s globally, 1 msg / 1 s per chat).
                       Prevents Telegram 429 flood errors when alerting many users.

  2. LOCKING         — each user has a `processing` flag in their session.
                       While a setup flow step is running (e.g. fetching PoUs),
                       further taps/commands from that user are silently dropped,
                       preventing duplicate postbacks and race conditions.

  3. HEARTBEAT       — every scrape cycle records success/failure to MongoDB.
                       After HEARTBEAT_FAIL_THRESHOLD consecutive failures the bot
                       sends a Telegram alert to ADMIN_CHAT_ID (env var) so you
                       know immediately when the ICAI page DOM has changed or the
                       site is down.

Thread safety: all state I/O is wrapped in STATE_LOCK (defined here,
used by main.py and background monitor).
"""

import os
import logging
import threading
import time
import queue
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from scraper import scrape_batches, compute_hash
from db import (
    load_state, save_state,
    load_batch_state, save_batch_state,
    record_heartbeat_ok, record_heartbeat_fail, get_heartbeat,
)

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

# Admin chat ID — set this env var on Railway to receive heartbeat failure alerts.
# Find your chat ID by messaging @userinfobot on Telegram.
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")

# How many consecutive scrape failures before alerting admin
HEARTBEAT_FAIL_THRESHOLD = 3

# Seat thresholds at which to send a special low-seat alert (descending)
SEAT_THRESHOLDS = [15, 10, 5, 1]

COURSES = [
    "Advanced (ICITSS) MCS Course",
    "Advanced (ICITSS) MCS Course - Weekend",
    "AICITSS - Advanced Information Technology",
    "ICITSS - Information Technology",
    "ICITSS - Orientation Course",
]

# Shared lock — import this in main.py to synchronise state access
STATE_LOCK = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 1 — MESSAGE QUEUE
# ═══════════════════════════════════════════════════════════════════════════════
# Telegram rate limits: 30 messages/second globally, 1 message/second per chat.
# When a batch alert fires for many users simultaneously, naïve sends cause 429s.
# Solution: a background worker drains a queue with enforced delays.

_MSG_QUEUE: queue.Queue = queue.Queue()
_GLOBAL_SEND_DELAY  = 0.05   # seconds between any two sends (≈ 20 msg/s, safe margin)
_PER_CHAT_DELAY     = 1.1    # seconds between sends to the same chat_id
_last_sent_per_chat: dict[str, float] = {}
_last_sent_global   = 0.0
_queue_lock         = threading.Lock()


def _queue_worker():
    """
    Background thread: drains _MSG_QUEUE and sends messages with rate limiting.
    Each item is (method: str, payload: dict).
    """
    global _last_sent_global
    logger.info("Message queue worker started.")
    while True:
        item = _MSG_QUEUE.get()
        if item is None:          # sentinel — stop the thread
            break
        method, payload = item
        chat_id = str(payload.get("chat_id", ""))

        with _queue_lock:
            now = time.monotonic()

            # Per-chat throttle
            last_chat = _last_sent_per_chat.get(chat_id, 0.0)
            wait_chat = max(0.0, _PER_CHAT_DELAY - (now - last_chat))

            # Global throttle
            wait_global = max(0.0, _GLOBAL_SEND_DELAY - (now - _last_sent_global))

            wait = max(wait_chat, wait_global)

        if wait > 0:
            time.sleep(wait)

        try:
            r = requests.post(f"{API_BASE}/{method}", json=payload, timeout=15)
            result = r.json()
            if not result.get("ok"):
                # 429 = flood — re-queue after retry_after seconds
                if r.status_code == 429:
                    retry_after = result.get("parameters", {}).get("retry_after", 5)
                    logger.warning(f"Telegram 429 — retrying after {retry_after}s (chat {chat_id})")
                    time.sleep(retry_after)
                    _MSG_QUEUE.put(item)
                else:
                    logger.error(f"Telegram {method} failed: {result}")
        except requests.RequestException as e:
            logger.error(f"Telegram API error ({method}): {e}")

        with _queue_lock:
            now2 = time.monotonic()
            _last_sent_per_chat[chat_id] = now2
            _last_sent_global = now2

        _MSG_QUEUE.task_done()


# Start the queue worker daemon thread immediately at import time
_worker_thread = threading.Thread(target=_queue_worker, name="MsgQueueWorker", daemon=True)
_worker_thread.start()


# --- Telegram helpers (now enqueue instead of direct POST) -------------------

def tg(method: str, **data):
    """Low-level direct call — use only for getUpdates and answerCallbackQuery."""
    try:
        r = requests.post(f"{API_BASE}/{method}", json=data, timeout=15)
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Telegram API error ({method}): {e}")
        return {}


def send(chat_id, text, markup=None):
    """Enqueue a sendMessage (rate-limited)."""
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = markup
    _MSG_QUEUE.put(("sendMessage", payload))


def edit(chat_id, message_id, text, markup=None):
    """Enqueue an editMessageText (rate-limited)."""
    payload = {
        "chat_id":    chat_id,
        "message_id": message_id,
        "text":       text,
        "parse_mode": "HTML",
    }
    if markup:
        payload["reply_markup"] = markup
    _MSG_QUEUE.put(("editMessageText", payload))


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


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 2 — LOCKING (processing flag)
# ═══════════════════════════════════════════════════════════════════════════════

def _is_processing(chat_id: str, state: dict) -> bool:
    """Return True if this user has a setup step currently running."""
    return state["users"].get(chat_id, {}).get("processing", False)


def _set_processing(chat_id: str, state: dict, value: bool):
    state["users"].setdefault(chat_id, {})["processing"] = value


# ═══════════════════════════════════════════════════════════════════════════════
# FIX 3 — HEARTBEAT helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _check_and_alert_heartbeat():
    """
    Read current heartbeat state. If consecutive failures >= threshold,
    send a one-time Telegram alert to ADMIN_CHAT_ID.
    Only alerts once per failure streak (tracks 'admin_alerted' flag in DB).
    """
    if not ADMIN_CHAT_ID:
        return   # admin notifications not configured

    hb = get_heartbeat()
    failures = hb.get("consecutive_failures", 0)

    if failures >= HEARTBEAT_FAIL_THRESHOLD and not hb.get("admin_alerted"):
        error_msg  = hb.get("error_msg", "unknown error")
        last_error = hb.get("last_error", "unknown time")
        alert_text = (
            f"🚨 <b>Bot Health Alert</b>\n\n"
            f"Scraper has failed <b>{failures} times in a row</b>.\n\n"
            f"Last error:\n<code>{error_msg[:400]}</code>\n\n"
            f"Timestamp: {last_error}\n\n"
            f"<i>The ICAI page DOM may have changed, or the site may be down. "
            f"Check Railway logs immediately.</i>"
        )
        # Direct send (bypass queue — this is an admin alert, must go out fast)
        try:
            requests.post(
                f"{API_BASE}/sendMessage",
                json={"chat_id": ADMIN_CHAT_ID, "text": alert_text, "parse_mode": "HTML"},
                timeout=15,
            )
            logger.warning(f"Admin heartbeat alert sent ({failures} consecutive failures)")
            # Mark as alerted so we don't spam
            from db import _heartbeat_col
            _heartbeat_col().update_one(
                {"_id": "status"},
                {"$set": {"admin_alerted": True}},
            )
        except Exception as e:
            logger.error(f"Failed to send admin heartbeat alert: {e}")

    elif failures == 0 and hb.get("admin_alerted"):
        # Recovery — clear the flag so next failure streak alerts again
        try:
            from db import _heartbeat_col
            _heartbeat_col().update_one(
                {"_id": "status"},
                {"$set": {"admin_alerted": False}},
            )
        except Exception:
            pass


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
        r0   = requests.get(ICAI_URL, headers=ICAI_HEADERS, timeout=20)
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
        r1   = requests.post(ICAI_URL, data=payload, headers=headers, timeout=20)
        soup1 = BeautifulSoup(r1.text, "lxml")
        sel  = soup1.find("select", {"id": "ddlPou"})
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


# --- Setup flow helpers -------------------------------------------------------

def start_setup(chat_id: str, state: dict, message_id=None):
    """Begin the Region -> PoU -> Course selection flow."""
    if _is_processing(chat_id, state):
        # Already locked — drop duplicate silently, no spam
        return

    # Persist lock to DB immediately so next poll cycle (which reloads state
    # from MongoDB) also sees processing=True and drops the duplicate request.
    _set_processing(chat_id, state, True)
    save_state(state)

    try:
        regions = fetch_regions()
    finally:
        _set_processing(chat_id, state, False)
        save_state(state)

    if not regions:
        send(chat_id, "Could not reach the ICAI site. Please try again in a minute.")
        return

    state["users"].setdefault(chat_id, {})
    state["users"][chat_id]["pending"] = {
        "step":       "region",
        "region_map": {label: val for label, val in regions},
    }

    rows   = [
        [regions[i], regions[i + 1]] if i + 1 < len(regions) else [regions[i]]
        for i in range(0, len(regions), 2)
    ]
    markup = ikb([[(label, f"region:{label}") for label, _ in row] for row in rows])
    text   = "Welcome! Let's set up your batch alert.\n\n<b>Step 1 of 3 — Select your Region:</b>"
    if message_id:
        edit(chat_id, message_id, text, markup)
    else:
        send(chat_id, text, markup)


def ask_pou(chat_id: str, region_label: str, state: dict, message_id: int):
    if _is_processing(chat_id, state):
        answer_cb(message_id, "⏳ Still loading, please wait...")
        return

    pending      = state["users"][chat_id].get("pending", {})
    region_map   = pending.get("region_map", {})
    region_value = region_map.get(region_label)

    if not region_value:
        send(chat_id, "Region not recognised. Type /watch to restart.")
        return

    # Persist lock before slow network call (same reason as start_setup)
    _set_processing(chat_id, state, True)
    save_state(state)
    try:
        pous = fetch_pous(region_value)
    finally:
        _set_processing(chat_id, state, False)
        save_state(state)

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

    state["users"][chat_id] = {
        "region":     region_label,
        "pou":        pou_label,
        "course":     course,
        "active":     True,
        "registered": False,
    }

    edit(
        chat_id, message_id,
        f"<b>You're all set!</b>\n\n"
        f"Region : {region_label}\n"
        f"City    : {pou_label}\n"
        f"Course  : {course}\n\n"
        f"⏳ Fetching current batch details...",
    )

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

    # FIX 2: block commands while a setup step is in-flight
    if _is_processing(chat_id, state):
        # Drop silently — start_setup will send the region keyboard once done
        return

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
    u = state["users"].get(chat_id, {})
    if not u or not u.get("active"):
        send(chat_id, "You don't have an active watch to close. Use /watch to set one up.")
        return

    region = u.get("region", "")
    pou    = u.get("pou", "")
    course = u.get("course", "")

    state["users"][chat_id] = {
        "region":     region,
        "pou":        pou,
        "course":     course,
        "active":     False,
        "registered": True,
    }

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

    # FIX 2: if already processing, answer the callback and bail
    if _is_processing(chat_id, state):
        answer_cb(cb_id, "⏳ Still loading, please wait...")
        return

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
    raw = batch.get("Available Seats", batch.get("AvailableSeats", ""))
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return None


def _new_threshold_fires(batch: dict, already_sent: list) -> list:
    seats = _seats_int(batch)
    if seats is None:
        return []
    return [t for t in SEAT_THRESHOLDS if seats <= t and t not in already_sent]


# --- Scrape and alert ---------------------------------------------------------

def _make_key(region: str, pou: str, course: str) -> str:
    return f"{region}|{pou}|{course}"


def scrape_and_alert(state: dict):
    """
    For every active (non-registered) subscribed user:
      1. Scrape current batches from ICAI
      2. Alert on any hash change (new/updated batches)
      3. Independently check seat thresholds (15/10/5/1) and alert per batch
      4. Skip users who have sent /registered
      5. Persist updated state to MongoDB

    FIX 3: records heartbeat after each key. On repeated failures, an admin
    alert is sent via Telegram so you know the moment the ICAI DOM breaks.
    """
    users       = state.get("users", {})
    batch_state = load_batch_state()

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

    any_success = False

    for key, chat_ids in watchlist.items():
        region, pou, course = key.split("|", 2)
        logger.info(f"Checking: {region} / {pou} / {course}  ({len(chat_ids)} subscriber(s))")

        try:
            batches  = scrape_batches(region, pou, course)
            new_hash = compute_hash(batches)
            any_success = True
            # FIX 3: record success
            record_heartbeat_ok()
        except Exception as e:
            err_str = f"{type(e).__name__}: {e}"
            logger.error(f"Scrape failed for {key}: {err_str}", exc_info=True)
            # FIX 3: record failure and maybe alert admin
            record_heartbeat_fail(err_str)
            _check_and_alert_heartbeat()
            continue

        old_entry        = batch_state.get(key, {})
        old_hash         = old_entry.get("hash", "")
        old_batch_nos    = {b.get("Batch No", "") for b in old_entry.get("batches", [])}
        seat_alerts_sent = old_entry.get("seat_alerts_sent", {})

        is_first = (old_hash == "")
        changed  = (new_hash != old_hash)

        # ── 1. Seat-threshold alerts ──────────────────────────────────────────
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

        # ── 3. Change-based notifications ─────────────────────────────────────
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
