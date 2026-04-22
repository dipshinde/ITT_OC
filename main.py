"""
main.py
-------
Persistent entry point for the ICAI Telegram Bot.

Runs three concurrent loops:
  1. Telegram polling loop  — checks for user messages every 2 s
  2. Batch monitor          — scrapes ICAI ITT/OC batches every 60 s
  3. SPOM monitor           — scrapes SPOM exam slots every 300 s (5 min)

Deploy on Railway:
  Set start command to `python main.py`

Environment variables (set in Railway dashboard or .env):
  Required:
    TELEGRAM_BOT_TOKEN  — Telegram bot token
    MONGODB_URI         — MongoDB connection string

  Optional:
    MONGODB_DB          — database name (default: icai_bot)
    GMAIL_USER          — Gmail address for sending email alerts
    GMAIL_APP_PASS      — 16-char Gmail App Password
    ALERT_EMAIL         — fallback recipient for email alerts
    ADMIN_CHAT_ID       — Telegram chat ID for heartbeat admin alerts
"""

import logging
import signal
import sys
import threading
import time

from db import ensure_indexes, load_state, save_state
from bot import (
    process_updates,
    scrape_and_alert,
    spom_scrape_and_alert,
    start_cleanup_scheduler,
    start_message_worker,       # FIX: import explicit starter instead of relying on module-level side-effect
    STATE_LOCK,
    _MSG_QUEUE,                 # FIX: needed to send shutdown sentinel to queue worker
    _reset_stuck_processing,
    migrate_users_to_watchlist,
)

# ─── Config ───────────────────────────────────────────────────────────────────

POLL_INTERVAL_SEC         = 0    # sleep between getUpdates calls (long-poll handles idle)
MONITOR_INTERVAL_SEC      = 60   # how often to scrape ITT/OC batches (1 min)
SPOM_MONITOR_INTERVAL_SEC = 300  # how often to scrape SPOM slots (5 min)

# FIX: give threads enough time to finish their current scrape cycle on shutdown.
# scrape_and_alert can take up to 120 s (futures timeout) + buffer.
THREAD_SHUTDOWN_TIMEOUT = 135

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  [%(threadName)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Graceful shutdown ────────────────────────────────────────────────────────

_shutdown = threading.Event()


def _handle_signal(signum, frame):
    logger.info(f"Signal {signum} received — shutting down gracefully...")
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)


# ─── ITT / OC batch monitor thread ───────────────────────────────────────────

def background_monitor(state: dict):
    """
    Wakes up every MONITOR_INTERVAL_SEC and scrapes ICAI ITT/OC batches.
    Sends Telegram + email alerts on structural changes or seat thresholds.
    """
    logger.info("Batch monitor started.")
    first_run = True

    while not _shutdown.is_set():
        if not first_run:
            for _ in range(MONITOR_INTERVAL_SEC):
                if _shutdown.is_set():
                    break
                time.sleep(1)
        first_run = False

        if _shutdown.is_set():
            break

        logger.info("═══ Batch monitor cycle starting ═══")
        try:
            scrape_and_alert(state)
        except Exception as e:
            logger.error(f"Batch monitor cycle error: {e}", exc_info=True)

    logger.info("Batch monitor stopped.")


# ─── SPOM slot monitor thread ─────────────────────────────────────────────────

def spom_monitor(state: dict):
    """
    Wakes up every SPOM_MONITOR_INTERVAL_SEC and checks SPOM portal for new
    exam slot availability.  Alerts ONLY when new available (green) dates
    appear in a subscribed city.  Sends both Telegram + email if configured.
    """
    logger.info("SPOM monitor started.")

    # Offset first run by 30 s so it doesn't collide with the batch monitor start
    for _ in range(30):
        if _shutdown.is_set():
            break
        time.sleep(1)

    while not _shutdown.is_set():
        logger.info("─── SPOM monitor cycle starting ───")
        try:
            spom_scrape_and_alert(state)
        except Exception as e:
            logger.error(f"SPOM monitor cycle error: {e}", exc_info=True)

        for _ in range(SPOM_MONITOR_INTERVAL_SEC):
            if _shutdown.is_set():
                break
            time.sleep(1)

    logger.info("SPOM monitor stopped.")


# ─── Telegram polling loop ────────────────────────────────────────────────────

def telegram_polling_loop(state: dict):
    """
    Continuously polls Telegram for new messages / button taps.
    Saves state to MongoDB only when updates were actually processed.
    """
    logger.info("Telegram polling loop started.")

    while not _shutdown.is_set():
        try:
            offset_before = state.get("_offset", 0)
            process_updates(state)

            if state.get("_offset", 0) != offset_before:
                with STATE_LOCK:
                    save_state(state)

        except Exception as e:
            logger.error(f"Polling error: {e}", exc_info=True)

        for _ in range(POLL_INTERVAL_SEC):
            if _shutdown.is_set():
                break
            time.sleep(1)

    logger.info("Telegram polling loop stopped.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("╔══════════════════════════════════════════════════╗")
    logger.info("║   ICAI Batch + SPOM Monitor Bot — starting up   ║")
    logger.info("╚══════════════════════════════════════════════════╝")

    # --- DB setup ---
    logger.info("Ensuring MongoDB indexes...")
    try:
        ensure_indexes()
    except Exception as e:
        logger.warning(f"ensure_indexes failed (non-fatal): {e}")

    # --- Load state once ---
    logger.info("Loading state from MongoDB...")
    state = load_state()
    logger.info(
        f"Loaded {len(state.get('users', {}))} user(s), "
        f"offset={state.get('_offset', 0)}"
    )

    # --- Migrate existing users from old single-watch to new watchlist format ---
    if migrate_users_to_watchlist(state):
        logger.info("User migration complete — saving updated state to MongoDB.")
        with STATE_LOCK:
            save_state(state)

    # --- Clear any processing flags left from a previous crash ---
    if _reset_stuck_processing(state):
        with STATE_LOCK:
            save_state(state)

    # FIX: Start queue worker explicitly here instead of at bot.py import time.
    # This keeps bot.py importable in tests without starting background threads.
    worker_thread = start_message_worker()

    # --- Start background services ---
    start_cleanup_scheduler(state)

    monitor_thread = threading.Thread(
        target=background_monitor,
        args=(state,),
        name="BatchMonitor",
        daemon=True,
    )
    monitor_thread.start()

    spom_thread = threading.Thread(
        target=spom_monitor,
        args=(state,),
        name="SpomMonitor",
        daemon=True,
    )
    spom_thread.start()

    # --- Main loop (blocks until SIGTERM / Ctrl-C) ---
    telegram_polling_loop(state)

    logger.info("Main thread exiting. Waiting for background threads...")
    monitor_thread.join(timeout=THREAD_SHUTDOWN_TIMEOUT)
    spom_thread.join(timeout=THREAD_SHUTDOWN_TIMEOUT)

    # FIX: Send sentinel to queue worker so it flushes remaining messages
    # before the process exits, instead of being killed mid-send.
    logger.info("Draining message queue...")
    _MSG_QUEUE.put(None)
    worker_thread.join(timeout=30)

    logger.info("Shutdown complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
