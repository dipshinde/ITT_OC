"""
main.py
-------
Persistent entry point for the ICAI Telegram Bot.

Runs two concurrent loops:
  1. Telegram polling loop  — checks for user messages every 2 s
  2. Background monitor     — scrapes ICAI and sends alerts every 60 s

Deploy on Railway / Render / any VPS:
  railway run python main.py
  OR: set start command to `python main.py`
"""

import threading
import time
import logging
import signal
import sys

from bot import load_state, save_state, process_updates, scrape_and_alert, STATE_LOCK

# ─── Config ───────────────────────────────────────────────────────────────────

POLL_INTERVAL_SEC    = 2    # how often to check Telegram for new messages
MONITOR_INTERVAL_SEC = 60   # how often to scrape ICAI (1 minute = continuous)

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

def background_monitor():
    """
    Wakes up every MONITOR_INTERVAL_SEC, scrapes ICAI for every active user,
    and sends Telegram alerts if batches changed or seat thresholds were crossed.
    Runs immediately on first iteration (no initial wait).
    """
    logger.info("Background monitor started.")
    first_run = True

    while not _shutdown.is_set():
        if not first_run:
            # Sleep in 1-second chunks so shutdown is responsive
            for _ in range(MONITOR_INTERVAL_SEC):
                if _shutdown.is_set():
                    break
                time.sleep(1)
        first_run = False

        if _shutdown.is_set():
            break

        logger.info("═══ Batch monitor cycle starting ═══")
        try:
            with STATE_LOCK:
                state = load_state()

            # NOTE: scrape_and_alert only READS from state (builds watchlist) and
            # writes its own data via save_batch_state(). It never modifies the
            # state dict itself. We must NOT call save_state(state) here because
            # this thread loaded state at the START of a 60-second cycle — saving
            # it back at the END would clobber the _offset that the polling thread
            # has advanced in the meantime, causing Telegram to re-deliver already-
            # processed updates (e.g. /start) and trigger duplicate messages.
            scrape_and_alert(state)   # sends Telegram messages internally

        except Exception as e:
            logger.error(f"Monitor cycle error: {e}", exc_info=True)

    logger.info("Background monitor stopped.")

# ─── Telegram polling loop ────────────────────────────────────────────────────

def telegram_polling_loop():
    """
    Continuously polls Telegram for new messages / button taps.
    Saves updated state (offset + user prefs) after every batch of updates.
    """
    logger.info("Telegram polling loop started.")

    while not _shutdown.is_set():
        try:
            with STATE_LOCK:
                state = load_state()

            process_updates(state)

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

    monitor_thread = threading.Thread(
        target=background_monitor,
        name="BatchMonitor",
        daemon=True,
    )
    monitor_thread.start()

    telegram_polling_loop()

    logger.info("Main thread exiting. Waiting for monitor thread...")
    monitor_thread.join(timeout=10)
    logger.info("Shutdown complete.")
    sys.exit(0)


if __name__ == "__main__":
    main()
