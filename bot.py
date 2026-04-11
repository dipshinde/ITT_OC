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

Fixes applied
─────────────
  1. HASH STABILITY    — compute_hash now sorts the batch list so identical
                         data in different row order does NOT trigger false alerts.
                         (Fix lives in scraper.py)

  2. HTML ESCAPING     — all scraped strings passed to Telegram HTML messages are
                         escaped with html.escape() so special chars (&, <, >)
                         don't corrupt the message or cause Telegram to reject it.

  3. THREAD SAFETY     — scrape_and_alert() takes a deep copy of the users dict
                         inside STATE_LOCK before iterating, preventing
                         RuntimeError: dictionary changed size during iteration
                         and ensuring the monitor never alerts already-stopped users.

  4. CONCURRENT SCRAPE — all unique watchlist keys are scraped in parallel using
                         ThreadPoolExecutor (max 4 workers) so the 60-second
                         cycle doesn't blow out with many subscribers.

  5. SEAT ALERT RESET  — seat_alerts_sent entries for batches that disappeared
                         from ICAI are pruned each cycle so the same batch number
                         reused in the next season correctly fires new alerts.

  6. MESSAGE QUEUE     — _MSG_QUEUE now has maxsize=2000. If the queue is full
                         (Telegram outage), messages are dropped with a warning
                         rather than consuming unbounded memory.

  7. MEMORY LEAK       — _last_sent_per_chat is pruned when a user is deleted,
                         preventing unbounded growth over months of operation.

  8. STUCK PROCESSING  — _reset_stuck_processing() clears processing=True flags
                         left over from a Railway restart mid-flow. Called once
                         at startup from main.py.

  9. ASYNC SETUP SCRAPE — the initial scrape in confirm_subscription() is moved
                          to a background thread so the polling loop is never
                          blocked for 5-15 seconds during user onboarding.

 10. CLEAN PUBLIC API  — inline `from db import _private_col` calls replaced with
                         proper public functions (delete_user, delete_stuck_users,
                         set_heartbeat_admin_alerted) defined in db.py.

Thread safety: all state I/O is wrapped in STATE_LOCK (defined here,
imported by main.py for synchronisation with the background monitor).
"""

import copy
import html
import logging
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from scraper import scrape_batches, compute_hash
from db import (
    load_state, save_state,
    load_batch_state, save_batch_state,
    record_heartbeat_ok, record_heartbeat_fail, get_heartbeat,
    set_heartbeat_admin_alerted,
    delete_user, delete_stuck_users,
)

# --- Logging -----------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Config ------------------------------------------------------------------

_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
if not _TOKEN:
    raise EnvironmentError(
        "TELEGRAM_BOT_TOKEN environment variable is not set. "
        "Add it to your Railway service variables."
    )

TOKEN    = _TOKEN
API_BASE = f"https://api.telegram.org/bot{TOKEN}"
ICAI_URL = "https://www.icaionlineregistration.org/launchbatchdetail.aspx"
ICAI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"
}

ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")

HEARTBEAT_FAIL_THRESHOLD = 3

SEAT_THRESHOLDS = [15, 10, 5, 1]

COURSES = [
    "Advanced (ICITSS) MCS Course",
    "Advanced (ICITSS) MCS Course - Weekend",
    "AICITSS - Advanced Information Technology",
    "ICITSS - Information Technology",
    "ICITSS - Orientation Course",
]

# Shared lock — imported by main.py to synchronise state access
STATE_LOCK = threading.Lock()


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE QUEUE — rate-limited outbox
# ═══════════════════════════════════════════════════════════════════════════════

# maxsize=2000: if Telegram is down for an extended period, messages are dropped
# (with a warning log) rather than consuming unbounded memory.
_MSG_QUEUE: queue.Queue = queue.Queue(maxsize=2000)
_GLOBAL_SEND_DELAY  = 0.05   # ≈ 20 msg/s global
_PER_CHAT_DELAY     = 1.1    # 1 msg/s per chat
_last_sent_per_chat: dict[str, float] = {}
_last_sent_global   = 0.0
_queue_lock         = threading.Lock()


def _enqueue(method: str, payload: dict):
    """Put a Telegram API call into the rate-limited queue. Drops if full."""
    try:
        _MSG_QUEUE.put_nowait((method, payload))
    except queue.Full:
        chat_id = payload.get("chat_id", "?")
        logger.warning(f"Message queue full — dropping {method} to chat {chat_id}")


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
            now         = time.monotonic()
            last_chat   = _last_sent_per_chat.get(chat_id, 0.0)
            wait_chat   = max(0.0, _PER_CHAT_DELAY   - (now - last_chat))
            wait_global = max(0.0, _GLOBAL_SEND_DELAY - (now - _last_sent_global))
            wait = max(wait_chat, wait_global)

        if wait > 0:
            time.sleep(wait)

        try:
            r      = requests.post(f"{API_BASE}/{method}", json=payload, timeout=15)
            result = r.json()
            if not result.get("ok"):
                if r.status_code == 429:
                    retry_after = result.get("parameters", {}).get("retry_after", 5)
                    logger.warning(f"Telegram 429 — retrying after {retry_after}s (chat {chat_id})")
                    time.sleep(retry_after)
                    _enqueue(method, payload)   # re-queue via _enqueue (respects maxsize)
                else:
                    logger.error(f"Telegram {method} failed: {result}")
        except requests.RequestException as e:
            logger.error(f"Telegram API error ({method}): {e}")

        with _queue_lock:
            now2 = time.monotonic()
            _last_sent_per_chat[chat_id] = now2
            _last_sent_global = now2

        _MSG_QUEUE.task_done()


_worker_thread = threading.Thread(target=_queue_worker, name="MsgQueueWorker", daemon=True)
_worker_thread.start()


# --- Telegram helpers --------------------------------------------------------

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
    _enqueue("sendMessage", payload)


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
    _enqueue("editMessageText", payload)


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
# PROCESSING FLAG — prevents duplicate postback handling
# ═══════════════════════════════════════════════════════════════════════════════

def _is_processing(chat_id: str, state: dict) -> bool:
    return state["users"].get(chat_id, {}).get("processing", False)


def _set_processing(chat_id: str, state: dict, value: bool):
    state["users"].setdefault(chat_id, {})["processing"] = value


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _reset_stuck_processing(state: dict) -> bool:
    """
    Clear processing=True flags left over from a crash or Railway restart
    mid-setup-flow. Without this, affected users are permanently silenced.

    Returns True if any flags were reset (so the caller can persist the fix).
    """
    changed = False
    for uid, u in state.get("users", {}).items():
        if u.get("processing"):
            u["processing"] = False
            changed = True
            logger.info(f"[Startup] Reset stuck processing flag for user {uid}")
    return changed


# ═══════════════════════════════════════════════════════════════════════════════
# HEARTBEAT
# ═══════════════════════════════════════════════════════════════════════════════

def _check_and_alert_heartbeat():
    """
    If consecutive failures >= threshold, send a one-time Telegram alert to
    ADMIN_CHAT_ID. Only alerts once per failure streak (admin_alerted flag).
    """
    if not ADMIN_CHAT_ID:
        return

    hb       = get_heartbeat()
    failures = hb.get("consecutive_failures", 0)

    if failures >= HEARTBEAT_FAIL_THRESHOLD and not hb.get("admin_alerted"):
        error_msg  = hb.get("error_msg",  "unknown error")
        last_error = hb.get("last_error", "unknown time")
        alert_text = (
            f"🚨 <b>Bot Health Alert</b>\n\n"
            f"Scraper has failed <b>{failures} times in a row</b>.\n\n"
            f"Last error:\n<code>{html.escape(str(error_msg))[:400]}</code>\n\n"
            f"Timestamp: {last_error}\n\n"
            f"<i>The ICAI page DOM may have changed, or the site may be down. "
            f"Check Railway logs immediately.</i>"
        )
        try:
            requests.post(
                f"{API_BASE}/sendMessage",
                json={"chat_id": ADMIN_CHAT_ID, "text": alert_text, "parse_mode": "HTML"},
                timeout=15,
            )
            logger.warning(f"Admin heartbeat alert sent ({failures} consecutive failures)")
            set_heartbeat_admin_alerted(True)
        except Exception as e:
            logger.error(f"Failed to send admin heartbeat alert: {e}")

    elif failures == 0 and hb.get("admin_alerted"):
        set_heartbeat_admin_alerted(False)


# ═══════════════════════════════════════════════════════════════════════════════
# DB CLEANUP
# ═══════════════════════════════════════════════════════════════════════════════

def _delete_user(chat_id: str, state: dict):
    """
    Remove a user from in-memory state, MongoDB, and the rate-limiter cache.
    Always use this instead of calling delete_user() directly.
    """
    state["users"].pop(chat_id, None)
    delete_user(chat_id)
    # FIX: prune the rate-limiter dict so it doesn't grow forever
    with _queue_lock:
        _last_sent_per_chat.pop(chat_id, None)
    logger.info(f"Deleted user {chat_id} from state and MongoDB.")


def cleanup_stuck_users(state: dict):
    """
    Runs daily at 1:00 AM IST. Deletes stuck/dead user documents from MongoDB
    and syncs the in-memory state.
    """
    logger.info("[Daily Cleanup] Running stuck-user cleanup...")
    try:
        total = delete_stuck_users()
        logger.info(f"[Daily Cleanup] Removed {total} stale user document(s) from MongoDB.")

        # Sync: remove any purged users from the in-memory state dict
        purged_keys = [
            uid for uid, u in list(state.get("users", {}).items())
            if "pending" in u and "course" not in u
        ]
        for uid in purged_keys:
            state["users"].pop(uid, None)
            with _queue_lock:
                _last_sent_per_chat.pop(uid, None)

        if ADMIN_CHAT_ID:
            send(
                ADMIN_CHAT_ID,
                f"🧹 <b>Daily DB Cleanup — 1:00 AM IST</b>\n\n"
                f"Removed <b>{total}</b> stale/stuck user document(s) from MongoDB.\n"
                f"Your database is clean! ✅",
            )
    except Exception as e:
        logger.error(f"[Daily Cleanup] Failed: {e}")


def start_cleanup_scheduler(state: dict):
    """
    Starts a background daemon thread that runs cleanup_stuck_users()
    every day at 1:00 AM IST (= 19:30 UTC).
    """
    def _scheduler():
        logger.info("Daily cleanup scheduler started — will run at 1:00 AM IST every day.")
        while True:
            now_utc = datetime.now(timezone.utc)
            # 1:00 AM IST = UTC+5:30 = 19:30 UTC previous day
            target_hour, target_minute = 19, 30
            next_run = now_utc.replace(
                hour=target_hour, minute=target_minute, second=0, microsecond=0
            )
            if now_utc >= next_run:
                from datetime import timedelta
                next_run += timedelta(days=1)
            wait_seconds = (next_run - now_utc).total_seconds()
            logger.info(f"[Daily Cleanup] Next run in {wait_seconds / 3600:.1f} hours (1:00 AM IST).")
            time.sleep(wait_seconds)
            cleanup_stuck_users(state)

    t = threading.Thread(target=_scheduler, name="DailyCleanup", daemon=True)
    t.start()


# --- ICAI helpers -------------------------------------------------------------

def fetch_regions():
    """Return list of (label, value) for the Region dropdown."""
    try:
        r    = requests.get(ICAI_URL, headers=ICAI_HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "lxml")
        sel  = soup.find("select", {"id": "ddl_reg"})
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
        r0    = requests.get(ICAI_URL, headers=ICAI_HEADERS, timeout=20)
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
        r1    = requests.post(ICAI_URL, data=payload, headers=headers, timeout=20)
        soup1 = BeautifulSoup(r1.text, "lxml")
        sel   = soup1.find("select", {"id": "ddlPou"})
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
        return

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
         f"<b>Step 2 of 3 — Select your City/PoU</b>\n(Region: {html.escape(region_label)})",
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
         f"<b>Step 3 of 3 — Select your Course</b>\n(City: {html.escape(pou_label)})",
         markup)


def _initial_scrape_notify(chat_id: str, region_label: str, pou_label: str, course: str):
    """
    Background thread: fetch current batches right after a user subscribes
    and send them an immediate snapshot. Runs outside the polling loop so it
    never blocks other users' messages.
    """
    try:
        batches = scrape_batches(region_label, pou_label, course)
        if batches:
            body = format_batches(batches)
            send(
                chat_id,
                f"<b>📋 Current Batches for {html.escape(course)}</b>\n"
                f"Region: {html.escape(region_label)} / {html.escape(pou_label)}\n\n"
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
                f"<b>No batches listed yet</b> for {html.escape(course)} in {html.escape(pou_label)}.\n\n"
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
        f"Region : {html.escape(region_label)}\n"
        f"City    : {html.escape(pou_label)}\n"
        f"Course  : {html.escape(course)}\n\n"
        f"⏳ Fetching current batch details...",
    )

    # FIX: run the initial scrape in a background thread so the polling loop
    # is never blocked for 5-15 seconds while ICAI's server responds.
    threading.Thread(
        target=_initial_scrape_notify,
        args=(chat_id, region_label, pou_label, course),
        daemon=True,
        name=f"InitScrape-{chat_id}",
    ).start()


# --- Command handlers ---------------------------------------------------------

def handle_message(msg: dict, state: dict):
    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text", "").strip()

    if _is_processing(chat_id, state):
        return

    if text.startswith("/start") or text.startswith("/watch"):
        start_setup(chat_id, state)

    elif text.startswith("/status"):
        u = state["users"].get(chat_id, {})
        if u.get("active"):
            send(
                chat_id,
                f"<b>Currently watching:</b>\n\n"
                f"Region: {html.escape(u['region'])} / {html.escape(u['pou'])}\n"
                f"Course: {html.escape(u['course'])}\n\n"
                f"Alerts fire at 15 / 10 / 5 / 1 seats remaining.\n"
                f"Use /watch to change, /stop to pause, or /registered once you've enrolled.",
            )
        else:
            send(chat_id, "No active watch. Use /watch to set one up.")

    elif text.startswith("/stop"):
        if chat_id in state["users"]:
            _delete_user(chat_id, state)
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
    if not u or not u.get("course"):
        send(chat_id, "You don't have a watch set up. Use /watch to get started.")
        return

    region = u.get("region", "")
    pou    = u.get("pou",    "")
    course = u.get("course", "")

    _delete_user(chat_id, state)

    # Clean up seat alert state for this key
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

    if _is_processing(chat_id, state):
        answer_cb(cb_id, "⏳ Still loading, please wait...")
        return

    answer_cb(cb_id)
    state["users"].setdefault(chat_id, {})

    pending_step = state["users"].get(chat_id, {}).get("pending", {}).get("step", "")

    if data.startswith("region:") and pending_step == "region":
        ask_pou(chat_id, data[len("region:"):], state, message_id)
    elif data.startswith("pou:") and pending_step == "pou":
        ask_course(chat_id, data[len("pou:"):], state, message_id)
    elif data.startswith("course:") and pending_step == "course":
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

def _esc(s) -> str:
    """Escape a scraped value for safe use in a Telegram HTML message."""
    return html.escape(str(s))


def format_batches(batches: list) -> str:
    if not batches:
        return "No batches currently listed."
    lines = []
    for b in batches:
        seats    = b.get("Available Seats", b.get("AvailableSeats", "?"))
        batch_no = b.get("Batch No",        b.get("BatchNo",        "?"))
        from_d   = b.get("From Date",       b.get("FromDate",       ""))
        to_d     = b.get("To Date",         b.get("ToDate",         ""))
        timing   = b.get("Batch Time",      b.get("BatchTime",      ""))
        lines.append(
            f"  <b>{_esc(batch_no)}</b>\n"
            f"     {_esc(from_d)} to {_esc(to_d)}  |  {_esc(timing)}\n"
            f"     Seats available: <b>{_esc(seats)}</b>"
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
    For every active subscribed user:
      1. Scrape current batches from ICAI (all keys in parallel)
      2. Alert on any hash change (new/updated batches)
      3. Independently check seat thresholds (15/10/5/1) and alert per batch
      4. Persist updated batch state to MongoDB

    FIX: Takes a deep copy of state["users"] under STATE_LOCK before iterating
         so the monitor thread never races with the polling thread's mutations.

    FIX: Uses ThreadPoolExecutor so multiple unique Region/PoU/Course
         combinations are scraped concurrently (max 4 workers), keeping
         the 60-second cycle from ballooning with many subscribers.
    """
    # Take a stable snapshot — never iterate the live dict
    with STATE_LOCK:
        users_snapshot = copy.deepcopy(state.get("users", {}))

    batch_state = load_batch_state()

    # Build watchlist: unique key → list of chat_ids
    watchlist: dict = {}
    for chat_id, u in users_snapshot.items():
        if not u.get("active"):
            continue
        if "pending" in u:
            continue
        if not (u.get("region") and u.get("pou") and u.get("course")):
            continue
        key = _make_key(u["region"], u["pou"], u["course"])
        watchlist.setdefault(key, []).append(chat_id)

    if not watchlist:
        return

    # ── Phase 1: Scrape all keys concurrently ─────────────────────────────────
    scrape_results: dict = {}
    scrape_errors:  dict = {}

    n_workers = min(4, len(watchlist))
    with ThreadPoolExecutor(max_workers=n_workers, thread_name_prefix="Scraper") as pool:
        futures = {}
        for key in watchlist:
            region, pou, course = key.split("|", 2)
            futures[pool.submit(scrape_batches, region, pou, course)] = key

        try:
            for fut in as_completed(futures, timeout=120):
                key = futures[fut]
                try:
                    scrape_results[key] = fut.result()
                except Exception as e:
                    err_str = f"{type(e).__name__}: {e}"
                    logger.error(f"Scrape failed for {key}: {err_str}", exc_info=True)
                    scrape_errors[key] = err_str
        except FutureTimeout:
            logger.warning("Some scrape tasks timed out (120 s); partial results will be processed.")
            for fut, key in futures.items():
                if not fut.done():
                    fut.cancel()
                    scrape_errors.setdefault(key, "TimeoutError: exceeded 120 s")

    # Record a single heartbeat for the cycle
    if scrape_errors and not scrape_results:
        # All scrapes failed
        first_err = next(iter(scrape_errors.values()))
        record_heartbeat_fail(first_err)
        _check_and_alert_heartbeat()
    elif scrape_errors:
        # Partial failure — record the first error but keep going
        first_err = next(iter(scrape_errors.values()))
        record_heartbeat_fail(first_err)
        _check_and_alert_heartbeat()
    else:
        record_heartbeat_ok()

    # ── Phase 2: Process results and send alerts ───────────────────────────────
    for key, chat_ids in watchlist.items():
        if key not in scrape_results:
            continue  # Scrape failed for this key — skip, already logged above

        batches  = scrape_results[key]
        new_hash = compute_hash(batches)
        region, pou, course = key.split("|", 2)

        old_entry        = batch_state.get(key, {})
        old_hash         = old_entry.get("hash", "")
        old_batch_nos    = {b.get("Batch No", "") for b in old_entry.get("batches", [])}
        seat_alerts_sent = old_entry.get("seat_alerts_sent", {})

        is_first = (old_hash == "")
        changed  = (new_hash != old_hash)

        # FIX: Prune seat_alerts_sent for batches that no longer exist.
        # This ensures re-used batch numbers correctly fire new alerts next season.
        new_batch_nos = {b.get("Batch No", b.get("BatchNo", "")) for b in batches}
        for gone_batch_no in list(seat_alerts_sent.keys()):
            if gone_batch_no not in new_batch_nos:
                del seat_alerts_sent[gone_batch_no]
                logger.debug(f"  Pruned seat_alerts_sent for gone batch {gone_batch_no}")

        # ── Seat-threshold alerts ─────────────────────────────────────────────
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
                    f"  Batch <b>{_esc(batch_no)}</b>  |  {_esc(from_d)} to {_esc(to_d)}  |  {_esc(timing)}"
                )
                seat_alerts_sent.setdefault(batch_no, []).extend(fires)

        # ── Persist updated batch state ───────────────────────────────────────
        batch_state[key] = {
            "hash":             new_hash,
            "batches":          batches,
            "last_checked":     datetime.now(timezone.utc).isoformat(),
            "region":           region,
            "pou":              pou,
            "course":           course,
            "seat_alerts_sent": seat_alerts_sent,
        }

        # ── Change-based notifications ────────────────────────────────────────
        if changed or is_first:
            added_batch_nos    = new_batch_nos - old_batch_nos
            newly_added        = [b for b in batches if b.get("Batch No", "") in added_batch_nos]
            batches_with_seats = [
                b for b in batches
                if str(b.get("Available Seats", b.get("AvailableSeats", "0"))) not in ("0", "")
            ]

            if is_first and not batches:
                logger.info(f"  First run — no batches yet, baseline saved ({key})")
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
                    f"Region : {_esc(region)} / {_esc(pou)}\n"
                    f"Course : {_esc(course)}\n\n"
                    f"{body}\n\n"
                    f"<a href='https://www.icaionlineregistration.org/launchbatchdetail.aspx'>"
                    f"Register on ICAI portal →</a>\n\n"
                    f"Send /registered once you've enrolled."
                )
                logger.info(f"  Alerting {len(chat_ids)} user(s) — batch change detected ({key})")
                for chat_id in chat_ids:
                    send(chat_id, change_msg)
        else:
            logger.info(f"  No change ({len(batches)} batch(es)) — {key}")

        # ── Seat-threshold notifications ──────────────────────────────────────
        if threshold_msgs:
            threshold_alert = (
                f"🚨 <b>Low Seat Alert</b>\n"
                f"Region: {_esc(region)} / {_esc(pou)}\n"
                f"Course: {_esc(course)}\n\n"
                + "\n\n".join(threshold_msgs) +
                f"\n\n<a href='https://www.icaionlineregistration.org/launchbatchdetail.aspx'>"
                f"Register NOW on ICAI portal →</a>\n\n"
                f"Send /registered once you've enrolled."
            )
            logger.info(f"  Seat-threshold alert → {len(chat_ids)} user(s) ({key})")
            for chat_id in chat_ids:
                send(chat_id, threshold_alert)

    save_batch_state(batch_state)
