"""
main.py
-------
Persistent entry point for the ICAI Telegram Bot.

Runs two concurrent loops:
  1. Telegram polling loop  — checks for user messages every 2 s
  2. Background monitor     — scrapes ICAI and sends alerts every 60 s

Deploy on Railway:
  Set start command to `python main.py`

Changes from original
─────────────────────
  - State is loaded from MongoDB ONCE at startup and kept in memory.
    The polling loop no longer hits MongoDB on every 2-second tick;
    it saves only when a Telegram update is actually processed.

  - ensure_indexes() is called at startup to create MongoDB indexes
    (safe to call repeatedly — skips existing indexes).

  - _reset_stuck_processing() is called at startup to clear any
    processing=True flags left from a previous crash or Railway restart,
    preventing users from being permanently silenced.
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
    start_cleanup_scheduler,
    STATE_LOCK,
    _reset_stuck_processing,
)

# ─── Config ───────────────────────────────────────────────────────────────────

POLL_INTERVAL_SEC    = 2     # how often to check Telegram for new messages
MONITOR_INTERVAL_SEC = 60    # how often to scrape ICAI (1 minute)

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

# ─── Background monitor thread ────────────────────────────────────────────────

def background_monitor(state: dict):
    """
    Wakes up every MONITOR_INTERVAL_SEC, scrapes ICAI for every active
    user, and sends Telegram alerts if batches changed or seat thresholds
    were crossed. Runs immediately on first iteration.
    """
    logger.info("Background monitor started.")
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
            # scrape_and_alert() takes a deep copy of state["users"] under
            # STATE_LOCK internally, so it never races with the polling thread.
            scrape_and_alert(state)
        except Exception as e:
            logger.error(f"Monitor cycle error: {e}", exc_info=True)

    logger.info("Background monitor stopped.")


# ─── Telegram polling loop ────────────────────────────────────────────────────

def telegram_polling_loop(state: dict):
    """
    Continuously polls Telegram for new messages / button taps.
    Saves state to MongoDB only when updates were actually processed
    (i.e. when the offset advanced), rather than on every 2-second tick.
    """
    logger.info("Telegram polling loop started.")

    while not _shutdown.is_set():
        try:
            offset_before = state.get("_offset", 0)
            process_updates(state)

            # Only write to MongoDB if something actually changed
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
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║   ICAI Batch Monitor Bot — starting up   ║")
    logger.info("╚══════════════════════════════════════════╝")

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

    # --- Clear any processing flags left from a previous crash ---
    if _reset_stuck_processing(state):
        with STATE_LOCK:
            save_state(state)

    # --- Start background services ---
    start_cleanup_scheduler(state)

    monitor_thread = threading.Thread(
        target=background_monitor,
        args=(state,),
        name="BatchMonitor",
        daemon=True,
    )
    monitor_thread.start()

    # --- Main loop (blocks until SIGTERM / Ctrl-C) ---
    telegram_polling_loop(state)

    logger.info("Main thread exiting. Waiting for monitor thread...")
    monitor_thread.join(timeout=10)
    logger.info("Shutdown complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
