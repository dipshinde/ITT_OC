"""
bot.py
------
Core Telegram bot logic for the ICAI Batch Monitor.

Responsibilities:
  - User onboarding flow: Region -> PoU -> Course (inline keyboard)
  - Command handling: /start /watch /status /stop /registered /help
  - scrape_and_alert(): called by background monitor thread every 60 s
  - Seat-threshold alerts: notifies at 10 / 5 / 1 seats remaining
  - State persistence: MongoDB via db.py

Fixes applied
─────────────
  1. MULTI-BATCH TRACKING — each user now has a `watchlist` array allowing
                             them to track multiple Region/PoU/Course combos
                             simultaneously. /watch always adds a new watch.
                             /stop and /registered show a selection keyboard
                             when the user has multiple watches.

  2. SEAT-SPAM FIX       — change-based "Batch Update" notifications now use
                            compute_structural_hash() (which ignores seat counts)
                            instead of compute_hash(). A drop from 20→19 seats
                            no longer triggers a change alert. Seat-count
                            alerts only fire at the configured thresholds.

  3. SEAT THRESHOLDS     — changed from [15, 10, 5, 1] to [10, 5, 1].
                            Alert at 15 seats removed per product decision.

  4. ZERO-SEAT GUARD     — _new_threshold_fires() now returns [] when
                            seats <= 0, preventing notifications for batches
                            with zero (or negative) available seats.

  5. HASH STABILITY      — compute_hash sorts the batch list so identical
                           data in different row order does NOT trigger alerts.
                           (Fix lives in scraper.py)

  6. HTML ESCAPING       — all scraped strings passed to Telegram HTML messages
                           are escaped with html.escape().

  7. THREAD SAFETY       — scrape_and_alert() takes a deep copy of the users
                           dict inside STATE_LOCK before iterating.

  8. CONCURRENT SCRAPE   — all unique watchlist keys scraped in parallel using
                           ThreadPoolExecutor (max 4 workers).

  9. SEAT ALERT RESET    — seat_alerts_sent entries for batches that disappeared
                           are pruned each cycle so re-used batch numbers fire
                           fresh alerts next season.

 10. MESSAGE QUEUE       — _MSG_QUEUE has maxsize=2000; messages are dropped
                           with a warning rather than consuming unbounded memory.

 11. MEMORY LEAK         — _last_sent_per_chat is pruned when a user is deleted.

 12. STUCK PROCESSING    — _reset_stuck_processing() clears processing=True
                           flags left over from a Railway restart mid-flow.

 13. ASYNC SETUP SCRAPE  — initial scrape in confirm_subscription() runs in a
                           background thread so polling loop is never blocked.

 14. USER MIGRATION      — migrate_users_to_watchlist() converts old single-
                           watch user docs (region/pou/course at top level) to
                           the new watchlist-array format. Called once at startup
                           from main.py.

Thread safety: all state I/O is wrapped in STATE_LOCK (defined here,
imported by main.py for synchronisation with the background monitor).
"""

import copy
import html
import logging
import os
import queue
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeout
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from scraper import scrape_batches, compute_hash, compute_structural_hash
from spom_scraper import (
    fetch_spom_states, fetch_spom_cities,
    fetch_all_city_availability,
    compute_spom_hash, find_new_available_dates,
)
from db import (
    load_state, save_state, save_user, save_offset,
    load_batch_state, save_batch_state,
    load_spom_state, save_spom_state,
    record_heartbeat_ok, record_heartbeat_fail, get_heartbeat,
    set_heartbeat_admin_alerted,
    delete_user, delete_stuck_users,
    set_user_email, get_users_with_email,
    clear_seat_alerts_for_key,          # FIX: targeted $unset instead of full-collection rewrite
)
from notifier import send_itt_oc_alert, send_spom_alert

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

# FIX: Never log the real token. Use this in exception handlers so Railway
# logs never contain the bot secret even in tracebacks.
_SAFE_API_BASE = "https://api.telegram.org/bot[REDACTED]"

def _redact(text: str) -> str:
    """Replace the bot token with [REDACTED] in any string before logging."""
    return text.replace(TOKEN, "[REDACTED]") if TOKEN else text
ICAI_URL = "https://www.icaionlineregistration.org/launchbatchdetail.aspx"
ICAI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"
}

ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")

HEARTBEAT_FAIL_THRESHOLD = 3

# FIX: removed 15 from thresholds per product decision; alert only at 10, 5, 1
SEAT_THRESHOLDS = [10, 5, 1]

COURSES = [
    "Advanced (ICITSS) MCS Course",
    "Advanced (ICITSS) MCS Course - Weekend",
    "AICITSS - Advanced Information Technology",
    "ICITSS - Information Technology",
    "ICITSS - Orientation Course",
]

# Shared lock — imported by main.py to synchronise state access
STATE_LOCK = threading.Lock()

# FIX: Prevents two overlapping scrape cycles from running concurrently.
# Without this, if scraping takes >60 s the next tick starts, both read the
# same old batch_state, and users receive duplicate alerts.
_SCRAPE_LOCK      = threading.Lock()
_SPOM_SCRAPE_LOCK = threading.Lock()


# ─── Granular save helper ─────────────────────────────────────────────────────

def _save_user(chat_id: str, state: dict):
    """
    Persist only this user's document to MongoDB (Fix A — granular writes).

    Replaces every bare _save_user(chat_id, state) call in handlers so that modifying
    one user writes exactly ONE MongoDB document instead of rewriting the
    entire users collection.  save_state() is now reserved for startup
    migrations that genuinely need to bulk-update every user at once.
    """
    user_data = state["users"].get(chat_id)
    if user_data is not None:
        save_user(chat_id, user_data)


# ═══════════════════════════════════════════════════════════════════════════════
# MESSAGE QUEUE — rate-limited outbox
# ═══════════════════════════════════════════════════════════════════════════════

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

        send_ok = False
        try:
            r      = requests.post(f"{API_BASE}/{method}", json=payload, timeout=15)
            result = r.json()
            if not result.get("ok"):
                if r.status_code == 429:
                    retry_after = result.get("parameters", {}).get("retry_after", 5)
                    logger.warning(f"Telegram 429 — retrying after {retry_after}s (chat {chat_id})")
                    time.sleep(retry_after)
                    # FIX: use blocking put with timeout instead of put_nowait.
                    # put_nowait silently drops messages when the queue is full —
                    # the worst time to drop a message is exactly when we're 429ing
                    # because the queue is already under load.
                    try:
                        _MSG_QUEUE.put((method, payload), timeout=10)
                    except queue.Full:
                        logger.error(f"Queue full after 429 backoff — message DROPPED to {chat_id}")
                else:
                    logger.error(f"Telegram {method} failed: {result}")
                    send_ok = True   # count as "sent" to advance rate-limiter clock
            else:
                send_ok = True
        except requests.RequestException as e:
            # FIX: redact token before logging — API_BASE contains the raw token
            logger.error(f"Telegram API error ({method}): {_redact(str(e))}")

        # FIX: only update the rate-limiter timestamp when a message was actually
        # delivered. Updating on failure causes the next queued message to wait
        # a full PER_CHAT_DELAY for no reason, compounding delays under load.
        if send_ok:
            with _queue_lock:
                now2 = time.monotonic()
                _last_sent_per_chat[chat_id] = now2
                _last_sent_global = now2

        _MSG_QUEUE.task_done()



# FIX: Do NOT start the worker thread at module import time.
# Starting threads at import makes bot.py impossible to test in isolation
# (any `import bot` immediately spawns a real Telegram-calling thread).
# Instead, main.py calls start_message_worker() explicitly during startup.
_worker_thread: threading.Thread | None = None


def start_message_worker() -> threading.Thread:
    """
    Start the rate-limited message queue worker thread.
    Called once from main.py at startup — NOT at module import time.
    Returns the thread so main.py can join it on shutdown.
    """
    global _worker_thread
    _worker_thread = threading.Thread(
        target=_queue_worker, name="MsgQueueWorker", daemon=True
    )
    _worker_thread.start()
    return _worker_thread



# --- Telegram helpers --------------------------------------------------------

def tg(method: str, **data):
    """Low-level direct call — use only for getUpdates and answerCallbackQuery."""
    try:
        r = requests.post(f"{API_BASE}/{method}", json=data, timeout=15)
        return r.json()
    except requests.RequestException as e:
        logger.error(f"Telegram API error ({method}): {_redact(str(e))}")
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
# WATCHLIST HELPERS — multi-watch support
# ═══════════════════════════════════════════════════════════════════════════════

def _get_watchlist(u: dict) -> list:
    """
    Return a user's watchlist, transparently handling the old single-watch
    format where region/pou/course were stored at the top level of the user doc.
    Always returns a list of dicts with keys: region, pou, course, registered.
    """
    if "watchlist" in u:
        return u["watchlist"]
    # Old single-watch format migration (read-only view; actual migration done at startup)
    if u.get("region") and u.get("pou") and u.get("course"):
        return [{
            "region":     u["region"],
            "pou":        u["pou"],
            "course":     u["course"],
            "registered": u.get("registered", False),
        }]
    return []


def migrate_users_to_watchlist(state: dict) -> bool:
    """
    One-time startup migration: convert old single-watch user docs
    (region/pou/course stored at top level) to the new watchlist-array format.

    Returns True if any user was migrated so the caller can persist the change.
    Called once from main.py after loading state.
    """
    changed = False
    for uid, u in state.get("users", {}).items():
        if uid == "__meta__":
            continue
        if "watchlist" not in u and u.get("region") and u.get("pou") and u.get("course"):
            u["watchlist"] = [{
                "region":     u.pop("region"),
                "pou":        u.pop("pou"),
                "course":     u.pop("course"),
                "registered": u.pop("registered", False),
            }]
            u.setdefault("active", True)
            changed = True
            logger.info(f"[Migration] Migrated user {uid} to watchlist format")
    return changed


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
    with _queue_lock:
        _last_sent_per_chat.pop(chat_id, None)
    logger.info(f"Deleted user {chat_id} from state and MongoDB.")


def cleanup_stuck_users(state: dict):
    logger.info("[Daily Cleanup] Running stuck-user cleanup...")
    try:
        total = delete_stuck_users()
        logger.info(f"[Daily Cleanup] Removed {total} stale user document(s) from MongoDB.")

        # FIX: Acquire STATE_LOCK before touching state["users"].
        # cleanup_stuck_users runs in the DailyCleanup thread while the batch
        # monitor and polling loop are also reading/writing state concurrently.
        # Mutating a shared dict without the lock is a data race.
        with STATE_LOCK:
            purged_keys = [
                uid for uid, u in list(state.get("users", {}).items())
                if "pending" in u and "course" not in u and "watchlist" not in u
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
    from datetime import timedelta   # FIX: import at function scope, not inside while loop

    def _scheduler():
        logger.info("Daily cleanup scheduler started — will run at 1:00 AM IST every day.")
        while True:
            now_utc = datetime.now(timezone.utc)
            target_hour, target_minute = 19, 30
            next_run = now_utc.replace(
                hour=target_hour, minute=target_minute, second=0, microsecond=0
            )
            if now_utc >= next_run:
                next_run += timedelta(days=1)
            wait_seconds = (next_run - now_utc).total_seconds()
            logger.info(f"[Daily Cleanup] Next run in {wait_seconds / 3600:.1f} hours (1:00 AM IST).")
            time.sleep(wait_seconds)
            cleanup_stuck_users(state)

    t = threading.Thread(target=_scheduler, name="DailyCleanup", daemon=True)
    t.start()


# --- ICAI helpers -------------------------------------------------------------

def fetch_regions():
    """
    Return list of (label, value) for the Region dropdown.

    FIX: use a Session so the ASP.NET session cookie set on the GET is
    automatically carried on any subsequent POST requests.  The original
    bare requests.get() discarded the cookie immediately after the call.
    """
    try:
        session = requests.Session()
        session.headers.update(ICAI_HEADERS)
        r = session.get(ICAI_URL, timeout=20)
        r.raise_for_status()
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
    """
    POST region selection and return PoU options as (label, value).

    FIX: use a shared Session for both the initial GET and the postback POST.
    Without a Session the ASP.NET session cookie is discarded between the two
    calls — the server rejects the stateless POST and returns a blank PoU
    dropdown, which is the root cause of intermittent /watch setup failures
    ("Could not fetch city list" error seen in production).
    """
    try:
        session = requests.Session()
        session.headers.update(ICAI_HEADERS)

        r0 = session.get(ICAI_URL, timeout=20)
        r0.raise_for_status()
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
        post_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Referer": ICAI_URL,
        }
        r1 = session.post(ICAI_URL, data=payload, headers=post_headers, timeout=20)
        r1.raise_for_status()
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
    """Begin the Region -> PoU -> Course selection flow.

    FIX: fetch_regions() is a slow HTTP call (~2-20 s) that previously ran
    synchronously on the Telegram polling thread, freezing the entire bot for
    every user while one user's region list loaded.  Now we:
      1. Show a "⏳ Loading..." message immediately (non-blocking)
      2. Spawn a background thread to do the HTTP fetch
      3. Edit the message with the real keyboard once fetch completes
    """
    if _is_processing(chat_id, state):
        return

    _set_processing(chat_id, state, True)
    _save_user(chat_id, state)

    loading_text = "⏳ <b>Fetching regions from ICAI portal...</b>\n\nThis takes a moment."
    if message_id:
        edit(chat_id, message_id, loading_text)
        bg_msg_id = message_id
    else:
        # Use tg() (direct/synchronous) to get the message_id for later editing
        resp = tg("sendMessage", chat_id=chat_id, text=loading_text, parse_mode="HTML")
        bg_msg_id = (resp.get("result") or {}).get("message_id")

    def _bg():
        try:
            regions = fetch_regions()
        except Exception as e:
            logger.error(f"fetch_regions failed: {e}")
            regions = []
        finally:
            _set_processing(chat_id, state, False)
            _save_user(chat_id, state)

        if not regions:
            if bg_msg_id:
                edit(chat_id, bg_msg_id,
                     "❌ Could not reach the ICAI site. Please try /watch again in a minute.")
            else:
                send(chat_id, "Could not reach the ICAI site. Please try again in a minute.")
            return

        state["users"].setdefault(chat_id, {})
        state["users"][chat_id]["pending"] = {
            "step":       "region",
            "region_map": {label: val for label, val in regions},
        }
        _save_user(chat_id, state)

        rows   = [
            [regions[i], regions[i + 1]] if i + 1 < len(regions) else [regions[i]]
            for i in range(0, len(regions), 2)
        ]
        markup = ikb([[(label, f"region:{label}") for label, _ in row] for row in rows])

        existing = _get_watchlist(state["users"].get(chat_id, {}))
        active_count = sum(1 for w in existing if not w.get("registered"))
        if active_count > 0:
            intro = (
                f"You're already watching <b>{active_count}</b> batch(es). "
                f"Let's add another one!\n\n"
                f"<b>Step 1 of 3 — Select your Region:</b>"
            )
        else:
            intro = "Welcome! Let's set up your batch alert.\n\n<b>Step 1 of 3 — Select your Region:</b>"

        if bg_msg_id:
            edit(chat_id, bg_msg_id, intro, markup)
        else:
            send(chat_id, intro, markup)

    threading.Thread(target=_bg, name=f"RegionFetch-{chat_id}", daemon=True).start()


def ask_pou(chat_id: str, region_label: str, state: dict, message_id: int):
    if _is_processing(chat_id, state):
        answer_cb(message_id, "⏳ Still loading, please wait...")
        return

    # FIX: use .get() instead of direct key access — chat_id may not exist if
    # the user taps a stale inline button after their account was deleted.
    pending      = state["users"].get(chat_id, {}).get("pending", {})
    region_map   = pending.get("region_map", {})
    region_value = region_map.get(region_label)

    if not region_value:
        send(chat_id, "Region not recognised. Type /watch to restart.")
        return

    _set_processing(chat_id, state, True)
    _save_user(chat_id, state)

    # FIX: show loading immediately — fetch_pous() makes 2 sequential HTTP
    # requests (~10-40 s) and must NOT block the polling thread.
    edit(chat_id, message_id,
         "⏳ <b>Fetching cities for your region...</b>\n\nThis takes a moment.")

    def _bg():
        try:
            pous = fetch_pous(region_value)
        except Exception as e:
            logger.error(f"fetch_pous failed: {e}")
            pous = []
        finally:
            _set_processing(chat_id, state, False)
            _save_user(chat_id, state)

        if not pous:
            edit(chat_id, message_id,
                 "❌ Could not fetch city list. Type /watch to try again.")
            return

        state["users"][chat_id]["pending"] = {
            "step":         "pou",
            "region_label": region_label,
            "region_value": region_value,
            "pou_map":      {label: val for label, val in pous},
        }
        _save_user(chat_id, state)

        rows   = [[pous[i], pous[i + 1]] if i + 1 < len(pous) else [pous[i]] for i in range(0, len(pous), 2)]
        markup = ikb([[(label, f"pou:{label}") for label, _ in row] for row in rows])
        edit(chat_id, message_id,
             f"<b>Step 2 of 3 — Select your City/PoU</b>\n(Region: {html.escape(region_label)})",
             markup)

    threading.Thread(target=_bg, name=f"PouFetch-{chat_id}", daemon=True).start()


def ask_course(chat_id: str, pou_label: str, state: dict, message_id: int):
    # FIX: use .get() — same stale-button KeyError risk as ask_pou
    pending   = state["users"].get(chat_id, {}).get("pending", {})
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
    and send them an immediate snapshot.
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
                f"<b>10, 5, and 1</b>.\n"
                f"Once you've registered, send /registered so I stop notifying you.",
            )
        else:
            send(
                chat_id,
                f"<b>No batches listed yet</b> for {html.escape(course)} in {html.escape(pou_label)}.\n\n"
                f"I'm monitoring continuously — you'll be alerted the moment batches appear, "
                f"and again at <b>10, 5, and 1</b> seats remaining.\n\n"
                f"Once you've registered, send /registered.",
            )
    except Exception as e:
        logger.error(f"Initial scrape for {chat_id} failed: {e}", exc_info=True)
        send(
            chat_id,
            "Could not fetch current batch data right now, but I'm watching continuously.\n"
            "You'll be alerted automatically when batches appear or seats hit 10, 5, or 1.",
        )


def confirm_subscription(chat_id: str, course: str, state: dict, message_id: int):
    pending      = state["users"][chat_id].get("pending", {})
    region_label = pending.get("region_label")
    pou_label    = pending.get("pou_label")

    u = state["users"].setdefault(chat_id, {})

    # Ensure new watchlist format — migrate old format if present
    if "watchlist" not in u:
        if u.get("region") and u.get("pou") and u.get("course"):
            u["watchlist"] = [{
                "region":     u.pop("region"),
                "pou":        u.pop("pou"),
                "course":     u.pop("course"),
                "registered": u.pop("registered", False),
            }]
        else:
            u["watchlist"] = []

    # Prevent duplicate watches
    is_duplicate = any(
        w.get("region") == region_label
        and w.get("pou") == pou_label
        and w.get("course") == course
        for w in u["watchlist"]
    )

    if not is_duplicate:
        u["watchlist"].append({
            "region":     region_label,
            "pou":        pou_label,
            "course":     course,
            "registered": False,
        })

    u["active"] = True
    u.pop("pending",     None)
    # Clean up any stale old-format top-level fields
    u.pop("region",     None)
    u.pop("pou",        None)
    u.pop("course",     None)
    u.pop("registered", None)

    if is_duplicate:
        edit(
            chat_id, message_id,
            f"<b>Already watching!</b>\n\n"
            f"You're already monitoring <b>{html.escape(course)}</b> in {html.escape(pou_label)}.\n\n"
            f"Use /status to see all your active watches.",
        )
        return

    total_watches = sum(1 for w in u["watchlist"] if not w.get("registered"))
    edit(
        chat_id, message_id,
        f"<b>✅ Watch added!</b>\n\n"
        f"Region : {html.escape(region_label)}\n"
        f"City    : {html.escape(pou_label)}\n"
        f"Course  : {html.escape(course)}\n\n"
        f"You're now tracking <b>{total_watches}</b> batch(es) total.\n\n"
        f"⏳ Fetching current batch details...",
    )

    threading.Thread(
        target=_initial_scrape_notify,
        args=(chat_id, region_label, pou_label, course),
        daemon=True,
        name=f"InitScrape-{chat_id}",
    ).start()



# ═══════════════════════════════════════════════════════════════════════════════
# MODE SELECTION — /start entry point
# ═══════════════════════════════════════════════════════════════════════════════

def ask_mode(chat_id: str, state: dict):
    """
    /start handler: reset any in-progress flow and show a mode-selection
    keyboard so users can choose between ITT/OC batch tracking and SPOM
    exam-slot tracking. New users get a full intro; returning users get a
    shorter welcome-back message.
    """
    u = state["users"].setdefault(chat_id, {})
    is_new_user = not u.get("active") and not u.get("spom_watches") and not u.get("watchlist")

    # Reset any in-progress flows safely
    u.pop("pending",      None)
    u.pop("spom_pending", None)
    u["processing"]  = False
    u["mode_pending"] = True
    _save_user(chat_id, state)

    markup = ikb([
        [("📚 ITT / OC Batches",   "mode:itt")],
        [("🧾 SPOM Exam Slots",    "mode:spom")],
    ])

    if is_new_user:
        send(
            chat_id,
            "👋 <b>Welcome to ICAI Monitor Bot!</b>\n\n"
            "I help CA students get instant alerts so you never miss a batch or exam slot.\n\n"
            "<b>──────────────────────────────────────────</b>\n"
            "📚 <b>ITT / OC Batch Tracker</b>\n"
            "  → Pick your Region, PoU, and Course\n"
            "  → I'll alert you at <b>10 / 5 / 1</b> seats remaining\n"
            "  → Use /watch anytime to add more\n\n"
            "🧾 <b>SPOM Exam Slot Tracker</b>\n"
            "  → Pick your State and City\n"
            "  → I'll alert you the moment new green (available) dates appear\n"
            "  → Use /spom to add more watches\n\n"
            "<b>──────────────────────────────────────────</b>\n"
            "⚙️ <b>Quick Commands</b>\n"
            "  /watch      — add ITT/OC batch tracker\n"
            "  /spom       — add SPOM slot tracker\n"
            "  /status     — see your active batch watches\n"
            "  /stop       — remove ITT/OC tracker\n"
            "  /spomstop   — remove SPOM tracker\n"
            "  /help       — full command guide\n\n"
            "<i>✅ You can run ITT/OC and SPOM trackers at the same time.\n"
            "Optionally add /email to also get email alerts.</i>\n\n"
            "<b>What would you like to track today?</b>",
            markup,
        )
    else:
        send(
            chat_id,
            "👋 <b>Welcome back!</b>\n\n"
            "What would you like to track?\n\n"
            "📚 <b>ITT / OC Batches</b> — alerts when batch seats open\n"
            "🧾 <b>SPOM Exam Slots</b>  — alerts when new exam dates appear\n\n"
            "<i>Use /status to see your active watches, /help for all commands.</i>",
            markup,
        )


# --- Command handlers ---------------------------------------------------------

def handle_message(msg: dict, state: dict):
    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text", "").strip()

    if _is_processing(chat_id, state):
        return

    if text.startswith("/start"):
        ask_mode(chat_id, state)

    elif text.startswith("/watch"):
        start_setup(chat_id, state)

    elif text.startswith("/status"):
        u = state["users"].get(chat_id, {})
        watches = _get_watchlist(u)
        active_watches = [w for w in watches if not w.get("registered")]
        if active_watches:
            lines = "\n\n".join(
                f"<b>{i + 1}.</b> {html.escape(w['region'])} / {html.escape(w['pou'])}\n"
                f"   Course: {html.escape(w['course'])}"
                for i, w in enumerate(active_watches)
            )
            send(
                chat_id,
                f"<b>Currently watching {len(active_watches)} batch(es):</b>\n\n"
                f"{lines}\n\n"
                f"Alerts fire at <b>10 / 5 / 1</b> seats remaining.\n"
                f"Use /watch to add more, /stop to remove one, "
                f"or /registered once you've enrolled.",
            )
        else:
            send(chat_id, "No active watches. Use /watch to set one up.")

    elif text.startswith("/stop"):
        u = state["users"].get(chat_id, {})
        watches = _get_watchlist(u)
        active_watches = [(i, w) for i, w in enumerate(watches) if not w.get("registered")]

        if not active_watches:
            send(chat_id, "You don't have any active watches. Use /watch to set one up.")
        elif len(active_watches) == 1:
            _delete_user(chat_id, state)
            send(chat_id, "Watch stopped. Use /watch anytime to resubscribe.")
        else:
            rows = [
                [(
                    f"{html.escape(w['pou'])} — {w['course'][:35]}",
                    f"stop_watch:{orig_i}"
                )]
                for orig_i, w in active_watches
            ]
            rows.append([("🛑 Stop All Watches", "stop_watch:all")])
            markup = ikb(rows)
            send(chat_id, "<b>Which watch would you like to stop?</b>", markup)

    elif text.lower().startswith("/registered") or text.lower() == "registered":
        _handle_registered(chat_id, state)

    elif text.startswith("/help"):
        _send_help(chat_id)

    # ── Email management ──────────────────────────────────────────────────────
    elif text.lower().startswith("/emailoff"):
        _handle_emailoff(chat_id, state)

    elif text.lower().startswith("/email"):
        _handle_email_command(chat_id, text, state)

    # ── SPOM slot monitoring ──────────────────────────────────────────────────
    elif text.lower().startswith("/spomstop"):
        _handle_spomstop(chat_id, state)

    elif text.lower().startswith("/spom"):
        start_spom_setup(chat_id, state)

    else:
        send(chat_id, "Use /watch to set up a batch alert, /spom for exam slots, or /help for all commands.")


def _handle_registered(chat_id: str, state: dict):
    u = state["users"].get(chat_id, {})
    watches = _get_watchlist(u)
    active_watches = [(i, w) for i, w in enumerate(watches) if not w.get("registered")]

    if not active_watches:
        send(chat_id, "You don't have any active watches. Use /watch to get started.")
        return

    if len(active_watches) == 1:
        orig_idx, _ = active_watches[0]
        _remove_registered_watch(chat_id, orig_idx, state)
    else:
        rows = [
            [(
                f"{html.escape(w['pou'])} — {w['course'][:35]}",
                f"reg_watch:{orig_i}"
            )]
            for j, (orig_i, w) in enumerate(active_watches)
        ]
        markup = ikb(rows)
        send(
            chat_id,
            "<b>Which batch did you register for?</b>\n\n"
            "Select the course you enrolled in and I'll stop monitoring it for you.",
            markup,
        )


def _remove_registered_watch(chat_id: str, watch_idx: int, state: dict):
    """
    Remove a single watch from the user's watchlist (used after /registered).
    Deletes the user entirely if no watches remain.
    """
    u = state["users"].get(chat_id, {})
    if not u:
        send(chat_id, "No watches found. Use /watch to get started.")
        return

    watches = list(_get_watchlist(u))
    if watch_idx < 0 or watch_idx >= len(watches):
        send(chat_id, "Watch not found. Use /status to see your current watches.")
        return

    watch  = watches.pop(watch_idx)
    region = watch.get("region", "")
    pou    = watch.get("pou",    "")
    course = watch.get("course", "")
    key    = _make_key(region, pou, course)   # build key before watches list is mutated further

    if watches:
        u["watchlist"] = watches
        # Clean up any stale old-format fields
        u.pop("region", None)
        u.pop("pou",    None)
        u.pop("course", None)
        _save_user(chat_id, state)
    else:
        _delete_user(chat_id, state)

    # FIX: use targeted $unset instead of loading the entire batch collection,
    # modifying it in memory, then rewriting every document back to MongoDB.
    # clear_seat_alerts_for_key issues a single update_one against one document.
    try:
        clear_seat_alerts_for_key(key)
    except Exception as e:
        logger.warning(f"Could not clean seat alert state for {key}: {e}")

    remaining = len(watches)
    if remaining:
        extra = (
            f"\n\nYou still have <b>{remaining}</b> watch(es) active. "
            f"Use /status to see them."
        )
    else:
        extra = "\n\nUse /watch anytime to monitor another batch."

    send(
        chat_id,
        f"🎉 <b>Congratulations on registering!</b>\n\n"
        f"Removed watch for <b>{html.escape(course)}</b> in {html.escape(pou)}.{extra}",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# HELP
# ═══════════════════════════════════════════════════════════════════════════════

def _send_help(chat_id: str):
    send(
        chat_id,
        "📖 <b>ICAI Monitor Bot — Help Guide</b>\n\n"
        "Use /start anytime to return to the main menu.\n\n"

        "<b>──────────────────────────────────────────</b>\n"
        "📚 <b>ITT / OC Batch Tracker</b>\n"
        "  /watch       — add a new ITT/OC batch to monitor\n"
        "                 (choose Region → PoU → Course)\n"
        "  /status      — list all your active batch watches\n"
        "  /stop        — remove one or all batch watches\n"
        "  /registered  — mark a batch as enrolled (stops alerts)\n\n"
        "  <i>Alerts fire at 10, 5, and 1 seat remaining.</i>\n\n"

        "<b>──────────────────────────────────────────</b>\n"
        "🧾 <b>SPOM Exam Slot Tracker</b>\n"
        "  /spom        — add a new SPOM watch (choose State → City)\n"
        "  /spomstop    — stop one or all SPOM watches\n\n"
        "  <i>Alerts fire ONLY when new 🟢 available dates appear.\n"
        "  No spam on seat count changes.</i>\n\n"

        "<b>──────────────────────────────────────────</b>\n"
        "📧 <b>Email Notifications</b>\n"
        "  /email &lt;addr&gt;  — save email for alerts\n"
        "                 e.g. <code>/email you@gmail.com</code>\n"
        "  /emailoff      — disable email (keeps address saved)\n\n"
        "  <i>Email alerts work for both ITT/OC and SPOM.</i>\n\n"

        "<b>──────────────────────────────────────────</b>\n"
        "⚙️ <b>All Commands at a Glance</b>\n"
        "  /start      — main menu\n"
        "  /watch      — add ITT/OC batch watch\n"
        "  /spom       — add SPOM slot watch\n"
        "  /status     — view active ITT/OC watches\n"
        "  /stop       — remove ITT/OC watch\n"
        "  /spomstop   — remove SPOM watch\n"
        "  /registered — mark batch as enrolled\n"
        "  /email      — set email for alerts\n"
        "  /emailoff   — disable email alerts\n"
        "  /help       — show this guide\n\n"

        "<i>✅ ITT/OC and SPOM trackers run independently and simultaneously.\n"
        "You can have multiple watches active at once.</i>",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

# FIX: original regex rejected valid subdomain addresses like user@mail.co.uk
# or user@students.university.edu because the domain segment didn't allow dots.
# New pattern requires at least one dot in the domain and a 2+ char TLD.
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+(\.[a-zA-Z0-9\-]+)*\.[a-zA-Z]{2,}$"
)


def _handle_email_command(chat_id: str, text: str, state: dict):
    """
    /email <address>  — save email and enable notifications.
    Called when text starts with /email but NOT /emailoff.
    """
    parts = text.strip().split(None, 1)
    if len(parts) < 2 or not parts[1].strip():
        u     = state["users"].get(chat_id, {})
        cur   = u.get("email", "")
        ena   = u.get("email_enabled", False)
        extra = (
            f"\n\nCurrent: <code>{html.escape(cur)}</code> "
            f"({'enabled ✅' if ena else 'disabled 🔕'})"
            if cur else ""
        )
        send(
            chat_id,
            "📧 <b>Email Notifications</b>\n\n"
            "Send your email address to receive alerts:\n"
            "<code>/email youraddress@example.com</code>\n\n"
            "You'll receive emails for:\n"
            "  • ITT/OC batch updates\n"
            "  • SPOM new slot availability\n\n"
            "Use /emailoff to disable email alerts."
            + extra,
        )
        return

    email = parts[1].strip().lower()
    if not _EMAIL_RE.match(email):
        send(chat_id, "❌ That doesn't look like a valid email address. Please check and try again.")
        return

    u = state["users"].setdefault(chat_id, {})
    u["email"]         = email
    u["email_enabled"] = True
    _save_user(chat_id, state)
    set_user_email(chat_id, email, enabled=True)

    send(
        chat_id,
        f"✅ <b>Email saved!</b>\n\n"
        f"Alerts will be sent to: <code>{html.escape(email)}</code>\n\n"
        f"You'll receive emails for:\n"
        f"  • ITT/OC batch updates\n"
        f"  • SPOM new slot availability\n\n"
        f"Use <b>/emailoff</b> to disable email notifications anytime.",
    )


def _handle_emailoff(chat_id: str, state: dict):
    """
    /emailoff  — disable email notifications (keeps address stored).
    """
    u = state["users"].get(chat_id, {})
    if not u.get("email"):
        send(chat_id, "You haven't set an email address yet. Use /email &lt;address&gt; to save one.")
        return

    u["email_enabled"] = False
    _save_user(chat_id, state)
    set_user_email(chat_id, u["email"], enabled=False)

    send(
        chat_id,
        f"🔕 Email notifications disabled.\n\n"
        f"Your address <code>{html.escape(u['email'])}</code> is still saved.\n"
        f"Re-enable with: <code>/email {html.escape(u['email'])}</code>",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SPOM SETUP FLOW  (State → City → confirm)
# ═══════════════════════════════════════════════════════════════════════════════

def start_spom_setup(chat_id: str, state: dict, message_id=None):
    """Begin SPOM watch setup: fetch states and show keyboard.

    FIX: fetch_spom_states() is a slow AJAX call that must NOT block the
    polling thread. Show loading immediately, then fetch in background.
    """
    if _is_processing(chat_id, state):
        return

    _set_processing(chat_id, state, True)
    _save_user(chat_id, state)

    loading_text = "⏳ <b>Fetching states from SPOM portal...</b>\n\nThis takes a moment."
    if message_id:
        edit(chat_id, message_id, loading_text)
        bg_msg_id = message_id
    else:
        resp = tg("sendMessage", chat_id=chat_id, text=loading_text, parse_mode="HTML")
        bg_msg_id = (resp.get("result") or {}).get("message_id")

    def _bg():
        # Outer guard: ANY unhandled exception is logged clearly instead of
        # dying silently and leaving the user stuck on "⏳ Fetching states...".
        try:
            _fetch_error = None
            try:
                states = fetch_spom_states()
            except Exception as e:
                logger.error(f"fetch_spom_states raised: {e}", exc_info=True)
                _fetch_error = str(e)
                states = []
            finally:
                _set_processing(chat_id, state, False)
                _save_user(chat_id, state)

            if not states:
                retry_markup = ikb([[("🔄 Retry", "mode:spom")]])
                err_text = (
                    "❌ <b>Could not load states from SPOM portal.</b>\n\n"
                    + (f"<i>Error: {html.escape(str(_fetch_error))}</i>\n\n"
                       if _fetch_error else "")
                    + "The ICAI portal may be slow or temporarily down.\n"
                    "Tap <b>Retry</b> to try again, or come back in a minute."
                )
                if bg_msg_id:
                    edit(chat_id, bg_msg_id, err_text, retry_markup)
                else:
                    send(chat_id, err_text, retry_markup)
                return

            state["users"].setdefault(chat_id, {})
            # FIX: fetch_spom_states() returns list[dict] with keys "label" and "value".
            # Previous code did `{label: val for label, val in states}` which iterated
            # over dict *keys* ("value", "label") instead of the actual data, causing
            # every state button to render as "value" and all city lookups to fail.
            state_tuples = [(item["label"], item["value"]) for item in states]
            state["users"][chat_id]["spom_pending"] = {
                "step":      "state",
                "state_map": {item["label"]: item["value"] for item in states},
            }
            _save_user(chat_id, state)

            rows   = [state_tuples[i:i + 2] for i in range(0, len(state_tuples), 2)]
            markup = ikb([[(label, f"spom_state:{label}") for label, _ in row] for row in rows])

            existing_spom = state["users"][chat_id].get("spom_watches", [])
            if existing_spom:
                intro = (
                    f"You already have <b>{len(existing_spom)}</b> SPOM watch(es). "
                    f"Adding another one!\n\n"
                    f"<b>Step 1 of 2 — Select your State:</b>"
                )
            else:
                intro = "🗓️ <b>SPOM Slot Monitor Setup</b>\n\n<b>Step 1 of 2 — Select your State:</b>"

            if bg_msg_id:
                edit(chat_id, bg_msg_id, intro, markup)
            else:
                send(chat_id, intro, markup)

        except Exception as outer_e:
            logger.error(f"[SPOM _bg] Unhandled crash in state-fetch thread: {outer_e}", exc_info=True)
            try:
                retry_markup = ikb([[("🔄 Retry", "mode:spom")]])
                if bg_msg_id:
                    edit(chat_id, bg_msg_id,
                         "❌ An unexpected error occurred. Please tap Retry.", retry_markup)
            except Exception:
                pass

    threading.Thread(target=_bg, name=f"SpomStateFetch-{chat_id}", daemon=True).start()


def _spom_ask_city(chat_id: str, state_label: str, state: dict, message_id: int):
    """Second SPOM setup step: fetch cities and show keyboard.

    FIX: fetch_spom_cities() is a slow AJAX call — must NOT block the polling
    thread. Show loading immediately, then fetch in background.
    """
    if _is_processing(chat_id, state):
        answer_cb(message_id, "⏳ Still loading, please wait...")
        return

    spom_pending = state["users"][chat_id].get("spom_pending", {})
    state_map    = spom_pending.get("state_map", {})
    state_value  = state_map.get(state_label)

    if not state_value:
        send(chat_id, "State not recognised. Type /spom to restart.")
        return

    _set_processing(chat_id, state, True)
    _save_user(chat_id, state)

    edit(chat_id, message_id,
         "⏳ <b>Fetching cities for your state...</b>\n\nThis takes a moment.")

    def _bg():
        try:
            cities = fetch_spom_cities(state_value)
        except Exception as e:
            logger.error(f"fetch_spom_cities failed: {e}")
            cities = []
        finally:
            _set_processing(chat_id, state, False)
            _save_user(chat_id, state)

        if not cities:
            edit(chat_id, message_id,
                 "❌ Could not fetch city list for that state. Type /spom to try again.")
            return

        # FIX: Same dict-vs-tuple bug as state_map — cities is list[dict].
        city_tuples = [(item["label"], item["value"]) for item in cities]
        state["users"][chat_id]["spom_pending"] = {
            "step":        "city",
            "state_label": state_label,
            "state_value": state_value,
            "city_map":    {item["label"]: item["value"] for item in cities},
        }
        _save_user(chat_id, state)

        rows   = [city_tuples[i:i + 2] for i in range(0, len(city_tuples), 2)]
        markup = ikb([[(label, f"spom_city:{label}") for label, _ in row] for row in rows])
        edit(chat_id, message_id,
             f"<b>Step 2 of 2 — Select your City</b>\n(State: {html.escape(state_label)})",
             markup)

    threading.Thread(target=_bg, name=f"SpomCityFetch-{chat_id}", daemon=True).start()


def _spom_initial_scrape_notify(
    chat_id: str, state_value: str, city_value: str,
    state_label: str, city_label: str,
):
    """
    Background thread: immediately scrape SPOM availability right after a user
    subscribes and send them a snapshot of current slots.

    Mirrors _initial_scrape_notify() for ITT/OC batches.
    Always shows ALL currently available slots — NOT a diff — because this is
    the user's first look at the data.
    """
    try:
        centre_results = fetch_all_city_availability(state_value, city_value)

        available_centres = [r for r in centre_results if r.get("available")]

        if not centre_results:
            send(
                chat_id,
                f"🔍 <b>No exam centres found</b> for <b>{html.escape(city_label)}</b>.\n\n"
                f"I'll keep watching and alert you the moment slots open.",
            )
            return

        if not available_centres:
            # Centres exist but all are fully booked or have no dates
            booked_count = sum(1 for r in centre_results if r.get("booked"))
            send(
                chat_id,
                f"📋 <b>Current SPOM Slots — {html.escape(city_label)}, {html.escape(state_label)}</b>\n\n"
                f"<b>{len(centre_results)}</b> centre(s) found — "
                f"<b>no available (green) dates right now</b>.\n"
                + (f"{booked_count} centre(s) have fully booked dates.\n" if booked_count else "")
                + f"\nI'm watching continuously and will alert you the moment new slots open. 🔔",
            )
            return

        # Build snapshot message of all currently available slots
        lines = [
            f"📋 <b>Current SPOM Slots — {html.escape(city_label)}, {html.escape(state_label)}</b>\n",
        ]
        total_slots = 0
        for r in available_centres:
            lines.append(f"<b>🏛️ {html.escape(r['centre'])}</b>")
            for s in r["available"]:
                if isinstance(s, dict):
                    seat_str = f"  ({s['seats']} seat{'s' if s['seats'] != 1 else ''})"
                    lines.append(f"  ✅ {html.escape(s['date'])}{seat_str}")
                    total_slots += 1
                else:
                    lines.append(f"  ✅ {html.escape(s)}")
                    total_slots += 1
            lines.append("")

        lines.append(
            f"<b>{total_slots} slot(s) available right now.</b>\n\n"
            f"<a href='https://spmt.icai.org/ICAI/LoginAction_showSlotDetails.action'>"
            f"Book your slot now →</a>\n\n"
            f"<i>I'll alert you when new (green) dates appear.</i>"
        )
        send(chat_id, "\n".join(lines))

    except Exception as e:
        logger.error(f"[SPOM] Initial scrape for {chat_id} failed: {e}", exc_info=True)
        send(
            chat_id,
            "⚠️ Could not fetch current slot data right now, but I'm watching continuously.\n"
            "You'll be alerted automatically when new slots appear.",
        )


def _spom_confirm(chat_id: str, city_label: str, state: dict, message_id: int):
    """Final SPOM setup step: save the watch and clean up pending."""
    spom_pending = state["users"][chat_id].get("spom_pending", {})
    city_map     = spom_pending.get("city_map", {})
    city_value   = city_map.get(city_label)
    state_label  = spom_pending.get("state_label", "")
    state_value  = spom_pending.get("state_value", "")

    if not city_value:
        send(chat_id, "City not recognised. Type /spom to restart.")
        return

    u            = state["users"].setdefault(chat_id, {})
    spom_watches = u.setdefault("spom_watches", [])

    already = any(
        w.get("state_value") == state_value and w.get("city_value") == city_value
        for w in spom_watches
    )

    u.pop("spom_pending", None)

    if already:
        edit(
            chat_id, message_id,
            f"<b>Already watching!</b>\n\n"
            f"You're already monitoring SPOM slots for "
            f"<b>{html.escape(city_label)}, {html.escape(state_label)}</b>.\n\n"
            f"Use /spomstop to remove it.",
        )
        _save_user(chat_id, state)
        return

    spom_watches.append({
        "state_label": state_label,
        "state_value": state_value,
        "city_label":  city_label,
        "city_value":  city_value,
    })
    u["active"] = True
    _save_user(chat_id, state)

    email_note = ""
    if u.get("email_enabled") and u.get("email"):
        email_note = f" + email to <code>{html.escape(u['email'])}</code>"

    edit(
        chat_id, message_id,
        f"✅ <b>SPOM watch added!</b>\n\n"
        f"State : {html.escape(state_label)}\n"
        f"City  : {html.escape(city_label)}\n\n"
        f"I'll alert you via Telegram{email_note} the moment "
        f"new exam slots open in your city.\n\n"
        f"⏳ Fetching current slot availability...",
    )

    threading.Thread(
        target=_spom_initial_scrape_notify,
        args=(chat_id, state_value, city_value, state_label, city_label),
        daemon=True,
        name=f"SpomInitScrape-{chat_id}",
    ).start()


def _handle_spomstop(chat_id: str, state: dict):
    """
    /spomstop  — remove one or all SPOM watches interactively.
    """
    u            = state["users"].get(chat_id, {})
    spom_watches = u.get("spom_watches", [])

    if not spom_watches:
        send(chat_id, "You don't have any SPOM watches. Use /spom to set one up.")
        return

    if len(spom_watches) == 1:
        w = spom_watches[0]
        u["spom_watches"] = []
        _save_user(chat_id, state)
        send(
            chat_id,
            f"✅ Stopped watching SPOM slots for "
            f"<b>{html.escape(w['city_label'])}, {html.escape(w['state_label'])}</b>.\n\n"
            f"Use /spom to add a new watch.",
        )
        return

    rows = [
        [( f"{html.escape(w['city_label'])} ({html.escape(w['state_label'])})",
           f"spom_stop:{i}" )]
        for i, w in enumerate(spom_watches)
    ]
    rows.append([("🛑 Stop All SPOM Watches", "spom_stop:all")])
    send(chat_id, "<b>Which SPOM watch would you like to stop?</b>", ikb(rows))


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

    # ── Mode selection (from /start) ──────────────────────────────────────────
    if data == "mode:itt":
        u = state["users"].setdefault(chat_id, {})
        u.pop("mode_pending", None)
        u["mode"] = "itt"
        _save_user(chat_id, state)
        start_setup(chat_id, state, message_id=message_id)

    elif data == "mode:spom":
        u = state["users"].setdefault(chat_id, {})
        u.pop("mode_pending", None)
        u["mode"] = "spom"
        _save_user(chat_id, state)
        start_spom_setup(chat_id, state, message_id=message_id)

    # ── Setup flow callbacks ──────────────────────────────────────────────────
    elif data.startswith("region:") and pending_step == "region":
        ask_pou(chat_id, data[len("region:"):], state, message_id)

    elif data.startswith("pou:") and pending_step == "pou":
        ask_course(chat_id, data[len("pou:"):], state, message_id)

    elif data.startswith("course:") and pending_step == "course":
        confirm_subscription(chat_id, data[len("course:"):], state, message_id)

    # ── Stop-watch callbacks (multi-watch /stop) ──────────────────────────────
    elif data.startswith("stop_watch:"):
        idx_str = data[len("stop_watch:"):]
        if idx_str == "all":
            _delete_user(chat_id, state)
            send(chat_id, "All watches stopped. Use /watch anytime to resubscribe.")
        else:
            try:
                idx = int(idx_str)
                u   = state["users"].get(chat_id, {})
                watches = list(_get_watchlist(u))
                if 0 <= idx < len(watches):
                    removed = watches.pop(idx)
                    if watches:
                        u["watchlist"] = watches
                        u.pop("region", None)
                        u.pop("pou",    None)
                        u.pop("course", None)
                        _save_user(chat_id, state)
                        send(
                            chat_id,
                            f"✅ Stopped watching <b>{html.escape(removed.get('course', ''))}</b> "
                            f"in {html.escape(removed.get('pou', ''))}.\n\n"
                            f"You have <b>{len(watches)}</b> watch(es) remaining. "
                            f"Use /status to review them.",
                        )
                    else:
                        _delete_user(chat_id, state)
                        send(chat_id, "All watches removed. Use /watch anytime to resubscribe.")
                else:
                    send(chat_id, "Watch not found. Use /status to check your current watches.")
            except (ValueError, KeyError) as e:
                logger.error(f"stop_watch callback error: {e}")
                send(chat_id, "Something went wrong. Please try /stop again.")

    # ── Registered callbacks (multi-watch /registered) ────────────────────────
    elif data.startswith("reg_watch:"):
        try:
            idx = int(data[len("reg_watch:"):])
            u   = state["users"].get(chat_id, {})
            watches = _get_watchlist(u)
            if 0 <= idx < len(watches):
                _remove_registered_watch(chat_id, idx, state)
            else:
                send(chat_id, "Watch not found. Use /status to check your current watches.")
        except ValueError:
            send(chat_id, "Something went wrong. Please try /registered again.")

    # ── SPOM setup flow callbacks ─────────────────────────────────────────────
    elif data.startswith("spom_state:"):
        spom_step = state["users"].get(chat_id, {}).get("spom_pending", {}).get("step", "")
        if spom_step == "state":
            _spom_ask_city(chat_id, data[len("spom_state:"):], state, message_id)
        else:
            send(chat_id, "Unexpected state. Use /spom to restart.")

    elif data.startswith("spom_city:"):
        spom_step = state["users"].get(chat_id, {}).get("spom_pending", {}).get("step", "")
        if spom_step == "city":
            _spom_confirm(chat_id, data[len("spom_city:"):], state, message_id)
        else:
            send(chat_id, "Unexpected state. Use /spom to restart.")

    # ── SPOM stop callbacks ───────────────────────────────────────────────────
    elif data.startswith("spom_stop:"):
        idx_str      = data[len("spom_stop:"):]
        u            = state["users"].get(chat_id, {})
        spom_watches = u.get("spom_watches", [])

        if idx_str == "all":
            u["spom_watches"] = []
            _save_user(chat_id, state)
            send(chat_id, "All SPOM watches stopped. Use /spom anytime to add new ones.")
        else:
            try:
                idx = int(idx_str)
                if 0 <= idx < len(spom_watches):
                    removed = spom_watches.pop(idx)
                    u["spom_watches"] = spom_watches
                    _save_user(chat_id, state)
                    send(
                        chat_id,
                        f"✅ Stopped watching SPOM slots for "
                        f"<b>{html.escape(removed['city_label'])}, "
                        f"{html.escape(removed['state_label'])}</b>.\n\n"
                        f"{'Use /spom to add another watch.' if not spom_watches else f'You have {len(spom_watches)} SPOM watch(es) remaining.'}",
                    )
                else:
                    send(chat_id, "Watch not found. Please try /spomstop again.")
            except (ValueError, KeyError) as e:
                logger.error(f"spom_stop callback error: {e}")
                send(chat_id, "Something went wrong. Please try /spomstop again.")


# --- Poll Telegram updates ----------------------------------------------------

def process_updates(state: dict):
    """Fetch pending Telegram updates and dispatch to handlers."""
    offset = state.get("_offset", 0)
    try:
        # FIX C: True long polling — Telegram holds the connection for up to 30 s
        # and returns the moment an update arrives, so the bot reacts instantly.
        # The previous timeout=5 was effectively short-polling: Telegram
        # returned every 5 s regardless of whether any update had come in,
        # causing a 0-5 s artificial lag on every message.
        # requests timeout must exceed the Telegram timeout so we don't hit a
        # spurious ReadTimeout before Telegram finishes its 30-second wait.
        resp = requests.get(
            f"{API_BASE}/getUpdates",
            params={"offset": offset, "timeout": 30},
            timeout=35,
        )
        # FIX: check HTTP status before parsing. A 401 (bad token), 409
        # (webhook conflict), or 5xx returns a body without "result", so
        # resp.json().get("result", []) silently returns [] and no error is logged.
        if not resp.ok:
            logger.error(
                f"getUpdates failed: HTTP {resp.status_code} — {resp.text[:200]}"
            )
            return
        updates = resp.json().get("result", [])
    except requests.RequestException as e:
        logger.error(f"getUpdates failed: {_redact(str(e))}")
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
    """
    Return the subset of SEAT_THRESHOLDS that should fire for this batch.

    FIX: returns [] when seats <= 0 — prevents zero-seat notifications.
    FIX: SEAT_THRESHOLDS is now [10, 5, 1] — removed the 15-seat alert.
    """
    seats = _seats_int(batch)
    if seats is None or seats <= 0:   # guard: never alert on 0 or negative seats
        return []
    return [t for t in SEAT_THRESHOLDS if seats <= t and t not in already_sent]


# --- Scrape and alert ---------------------------------------------------------

def _make_key(region: str, pou: str, course: str) -> str:
    return f"{region}|{pou}|{course}"


def scrape_and_alert(state: dict):
    """
    For every active subscribed user:
      1. Collect all unique watchlist keys (supports multiple watches per user)
      2. Scrape current batches from ICAI (all keys in parallel)
      3. Alert on structural changes (new/removed batches, date/timing changes)
         — does NOT alert on seat-count-only changes (FIX for spam issue)
      4. Independently check seat thresholds (10/5/1) and alert per batch
         — never fires for 0-seat batches (FIX for zero-seat alert issue)
      5. Persist updated batch state to MongoDB

    FIX: guarded by _SCRAPE_LOCK so that if a scrape cycle takes longer than
    MONITOR_INTERVAL_SEC (60 s), the next tick is skipped rather than running
    a second concurrent cycle that reads stale state and double-alerts users.
    """
    if not _SCRAPE_LOCK.acquire(blocking=False):
        logger.warning("Previous batch scrape cycle still running — skipping this tick.")
        return
    try:
        _scrape_and_alert_impl(state)
    finally:
        _SCRAPE_LOCK.release()


def _scrape_and_alert_impl(state: dict):
    """Inner implementation — called only from scrape_and_alert under _SCRAPE_LOCK."""
    with STATE_LOCK:
        users_snapshot = copy.deepcopy(state.get("users", {}))

    batch_state = load_batch_state()

    # Build watchlist: unique key → list of chat_ids
    # Handles both old single-watch format and new watchlist array format
    watchlist: dict = {}
    for chat_id, u in users_snapshot.items():
        if not u.get("active"):
            continue
        if "pending" in u:
            continue

        watches = _get_watchlist(u)
        for watch in watches:
            if watch.get("registered"):
                continue
            if not (watch.get("region") and watch.get("pou") and watch.get("course")):
                continue
            key = _make_key(watch["region"], watch["pou"], watch["course"])
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

    # FIX: differentiate between a TOTAL failure (no results at all — real outage)
    # and a PARTIAL failure (some keys failed — intermittent network issue).
    # The original code called record_heartbeat_fail for both cases, causing
    # admin panic alerts after any single flaky scrape. Now:
    #   - All failed  → heartbeat_fail  → admin alert after threshold
    #   - Some failed → heartbeat_ok    → log a warning, don't alert admin
    #   - None failed → heartbeat_ok
    if scrape_errors:
        first_err = next(iter(scrape_errors.values()))
        if scrape_results:
            # Partial failure — at least some keys scraped successfully
            logger.warning(
                f"Partial scrape failure: {len(scrape_errors)} key(s) failed, "
                f"{len(scrape_results)} succeeded — {first_err}"
            )
            record_heartbeat_ok()   # don't alarm admin for transient per-key failures
        else:
            # Total failure — nothing scraped at all
            record_heartbeat_fail(first_err)
            _check_and_alert_heartbeat()
    else:
        record_heartbeat_ok()

    # ── Phase 2: Process results and send alerts ───────────────────────────────
    for key, chat_ids in watchlist.items():
        if key not in scrape_results:
            continue

        batches  = scrape_results[key]
        new_hash = compute_hash(batches)
        # FIX: structural hash ignores seat counts — used for change notifications
        new_struct_hash = compute_structural_hash(batches)
        region, pou, course = key.split("|", 2)

        old_entry        = batch_state.get(key, {})
        old_hash         = old_entry.get("hash", "")
        old_struct_hash  = old_entry.get("struct_hash", "")
        old_batch_nos    = {b.get("Batch No", "") for b in old_entry.get("batches", [])}
        seat_alerts_sent = old_entry.get("seat_alerts_sent", {})

        # First run if we've never stored state for this key
        is_first = not old_entry
        # FIX: only flag as "changed" when the STRUCTURE changed, not seat counts
        struct_changed = (new_struct_hash != old_struct_hash)

        # Prune seat_alerts_sent for batches that no longer exist
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
            fires    = _new_threshold_fires(b, already)  # FIX: guards 0-seat & uses [10,5,1]
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
            "struct_hash":      new_struct_hash,   # FIX: store structural hash separately
            "batches":          batches,
            "last_checked":     datetime.now(timezone.utc).isoformat(),
            "region":           region,
            "pou":              pou,
            "course":           course,
            "seat_alerts_sent": seat_alerts_sent,
        }

        # ── Change-based notifications (structural changes only) ──────────────
        if struct_changed or is_first:
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
                logger.info(f"  Alerting {len(chat_ids)} user(s) — structural change detected ({key})")
                for chat_id in chat_ids:
                    send(chat_id, change_msg)
                    # ── Email notification ────────────────────────────────────
                    u_snap = users_snapshot.get(chat_id, {})
                    if u_snap.get("email_enabled") and u_snap.get("email"):
                        try:
                            send_itt_oc_alert(
                                batches_with_seats or newly_added or batches,
                                region   = region,
                                pou      = pou,
                                course   = course,
                                to_email = u_snap["email"],
                            )
                        except Exception as email_err:
                            logger.error(
                                f"  Email send failed for {chat_id}: {email_err}"
                            )
        else:
            logger.info(f"  No structural change ({len(batches)} batch(es)) — {key}")

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

            # FIX: persist seat_alerts_sent immediately after sending alerts for
            # this key — do NOT wait until the end of the full cycle.
            # If Railway restarts between the alert send and the end-of-cycle
            # save_batch_state call, seat_alerts_sent is lost, and the same
            # threshold fires again on the next startup.
            try:
                save_batch_state({key: batch_state[key]})
            except Exception as e:
                logger.error(f"  Failed to persist seat_alerts_sent for {key}: {e}")

    save_batch_state(batch_state)


# ═══════════════════════════════════════════════════════════════════════════════
# SPOM MONITOR — scrape slots and alert on NEW available dates only
# ═══════════════════════════════════════════════════════════════════════════════

def spom_scrape_and_alert(state: dict):
    """
    For every unique (state_value, city_value) pair across all active SPOM
    subscribers:
      1. Fetch current slot availability from spmt.icai.org
      2. Compare with persisted availability
      3. Notify via Telegram + email ONLY when new available (green) dates appear
      4. Persist updated SPOM state to MongoDB

    Users are notified ONLY for the state+city they subscribed to.
    Fully-booked (red) dates and unchanged dates never trigger alerts.

    FIX: guarded by _SPOM_SCRAPE_LOCK — SPOM scraping is sequential and can
    take several minutes for cities with many test centres. Without the lock
    a new 5-minute tick could start before the previous one completes.
    """
    if not _SPOM_SCRAPE_LOCK.acquire(blocking=False):
        logger.warning("Previous SPOM scrape cycle still running — skipping this tick.")
        return
    try:
        _spom_scrape_and_alert_impl(state)
    finally:
        _SPOM_SCRAPE_LOCK.release()


def _spom_scrape_and_alert_impl(state: dict):
    """Inner implementation — called only from spom_scrape_and_alert under _SPOM_SCRAPE_LOCK."""
    with STATE_LOCK:
        users_snapshot = copy.deepcopy(state.get("users", {}))

    spom_state = load_spom_state()

    # Build a map: (state_value, city_value) → list of (chat_id, state_label, city_label)
    city_watchers: dict = {}
    for chat_id, u in users_snapshot.items():
        if not u.get("active"):
            continue
        for w in u.get("spom_watches", []):
            sv = w.get("state_value", "")
            cv = w.get("city_value",  "")
            if not (sv and cv):
                continue
            key = f"{sv}|{cv}"
            city_watchers.setdefault(key, []).append({
                "chat_id":     chat_id,
                "state_label": w.get("state_label", sv),
                "city_label":  w.get("city_label",  cv),
                "email":       u.get("email"),
                "email_enabled": u.get("email_enabled", False),
            })

    if not city_watchers:
        return

    logger.info(f"[SPOM] Checking {len(city_watchers)} city/state pair(s)...")

    for city_key, watchers in city_watchers.items():
        state_value = city_key.split("|", 1)[0]
        city_value  = city_key.split("|", 1)[1]
        state_label = watchers[0]["state_label"]
        city_label  = watchers[0]["city_label"]

        try:
            centre_results = fetch_all_city_availability(state_value, city_value)
        except Exception as e:
            logger.error(f"[SPOM] fetch_all_city_availability failed ({city_key}): {e}")
            continue

        if not centre_results:
            logger.info(f"[SPOM] No centres found for {city_label}, {state_label}")
            continue

        new_hash = compute_spom_hash(centre_results)

        # Rebuild old results list from persisted state for this city
        old_results: list[dict] = []
        for item in centre_results:
            centre_key  = f"{state_value}|{city_value}|{item['centre']}"
            old_entry   = spom_state.get(centre_key, {})
            old_results.append({
                "centre":    item["centre"],
                "available": old_entry.get("available_dates", []),
                "booked":    old_entry.get("booked_dates",    []),
            })

        # Find dates that are NEWLY available (not in old state)
        new_dates_by_centre = find_new_available_dates(old_results, centre_results)

        # Persist updated state for every centre in this city
        for item in centre_results:
            centre_key = f"{state_value}|{city_value}|{item['centre']}"
            spom_state[centre_key] = {
                "available_dates": item.get("available", []),
                "booked_dates":    item.get("booked",    []),
                "hash":            new_hash,
                "state_label":     state_label,
                "city_label":      city_label,
                "centre_label":    item["centre"],
            }

        if not new_dates_by_centre:
            avail_count = sum(len(i.get("available", [])) for i in centre_results)
            logger.info(
                f"[SPOM] No new slots for {city_label}, {state_label} "
                f"({avail_count} existing available date(s), no change)"
            )
            continue

        # ── Send Telegram + email alerts ──────────────────────────────────────
        total_new   = sum(len(v) for v in new_dates_by_centre.values())
        logger.info(
            f"[SPOM] {total_new} new slot(s) detected in {city_label}, {state_label} "
            f"— alerting {len(watchers)} subscriber(s)"
        )

        # Build Telegram message
        tg_lines = [f"📅 <b>New SPOM Slots Available!</b>\n",
                    f"State : {_esc(state_label)}",
                    f"City  : {_esc(city_label)}\n"]

        for centre, slots in new_dates_by_centre.items():
            tg_lines.append(f"<b>🏛️ {_esc(centre)}</b>")
            for s in slots:
                if isinstance(s, dict):
                    seat_str = f"  ({s['seats']} seat{'s' if s['seats'] != 1 else ''})"
                    tg_lines.append(f"  ✅ {_esc(s['date'])}{seat_str}")
                else:
                    tg_lines.append(f"  ✅ {_esc(s)}")
            tg_lines.append("")

        tg_lines.append(
            "<a href='https://spmt.icai.org/ICAI/LoginAction_showSlotDetails.action'>"
            "Book your slot now →</a>"
        )
        tg_msg = "\n".join(tg_lines)

        notified_emails: set = set()   # de-dup: one email per address per cycle

        for watcher in watchers:
            chat_id = watcher["chat_id"]
            send(chat_id, tg_msg)

            if watcher.get("email_enabled") and watcher.get("email"):
                email = watcher["email"]
                if email not in notified_emails:
                    try:
                        send_spom_alert(
                            new_dates_by_centre,
                            state_label = state_label,
                            city_label  = city_label,
                            to_email    = email,
                        )
                        notified_emails.add(email)
                    except Exception as email_err:
                        logger.error(
                            f"[SPOM] Email send failed for {chat_id} "
                            f"({email}): {email_err}"
                        )

    save_spom_state(spom_state)
    logger.info("[SPOM] Cycle complete.")
