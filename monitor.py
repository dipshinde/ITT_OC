"""
monitor.py
----------
Entry point for the ICAI batch email monitor.
Run by GitHub Actions every 10 minutes.

Uses MongoDB (via db.py) for state persistence instead of state.json so
that state survives across GitHub Actions runs and stays in sync with the
Railway Telegram bot's batch state.

Usage:
  python monitor.py              → normal monitoring run
  python monitor.py --test-email → send a test email and exit
  python monitor.py --debug      → print discovered batches without sending email

Required environment variables (set as GitHub Secrets):
  MONGODB_URI     → your MongoDB connection string
  GMAIL_USER      → your Gmail address
  GMAIL_APP_PASS  → 16-char Gmail App Password
  ALERT_EMAIL     → where to send alerts (can be same as GMAIL_USER)

Optional:
  MONGODB_DB      → database name (default: icai_bot)
"""

import logging
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ╔══════════════════════════════════════════════════════════════════╗
# ║                    YOUR PREFERENCES                              ║
# ║  Edit these three values to match your requirements.             ║
# ╚══════════════════════════════════════════════════════════════════╝

REGION = "Western"
POU    = "Pune"
COURSE = "Advanced (ICITSS) MCS Course"

# To monitor multiple combinations, add more dicts to this list:
# WATCHLIST = [
#     {"region": "Western", "pou": "Pune", "course": "AICITSS - Advanced Information Technology"},
#     {"region": "Western", "pou": "Pune", "course": "Advanced (ICITSS) MCS Course"},
# ]

WATCHLIST = [
    {"region": REGION, "pou": POU, "course": COURSE},
]


def make_key(region: str, pou: str, course: str) -> str:
    return f"{region}|{pou}|{course}"


def run_monitor():
    from scraper import scrape_batches, compute_hash
    from notifier import send_alert
    from db import load_batch_state, save_batch_state

    # Load state from MongoDB — shared with the Railway Telegram bot
    full_state  = load_batch_state()
    any_error   = False
    alerts_sent = 0
    updates     = {}

    for pref in WATCHLIST:
        region = pref["region"]
        pou    = pref["pou"]
        course = pref["course"]
        key    = make_key(region, pou, course)

        logger.info(f"─── Checking: {region} / {pou} / {course}")

        try:
            batches = scrape_batches(region, pou, course)
        except Exception as e:
            logger.error(f"Scrape failed for {key}: {e}", exc_info=True)
            any_error = True
            continue

        new_hash  = compute_hash(batches)
        old_entry = full_state.get(key, {})
        old_hash  = old_entry.get("hash", "")

        changed  = new_hash != old_hash
        is_first = old_hash == ""

        if is_first and batches:
            logger.info(f"FIRST RUN — {len(batches)} batch(es) already listed, sending alert")
            try:
                send_alert(batches, region, pou, course)
                alerts_sent += 1
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
                any_error = True

        elif is_first and not batches:
            logger.info("First run — no batches found yet, baseline saved")

        elif changed and not is_first:
            logger.info(f"CHANGE DETECTED for {key} — {len(batches)} batch(es)")
            try:
                send_alert(batches, region, pou, course)
                alerts_sent += 1
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
                any_error = True

        else:
            logger.info(f"No change ({len(batches)} batch(es), hash={new_hash[:12]}...)")

        # Preserve seat_alerts_sent from the existing entry so we don't re-fire
        # seat threshold alerts that the Telegram bot already sent.
        existing_seat_alerts = old_entry.get("seat_alerts_sent", {})

        updates[key] = {
            "hash":             new_hash,
            "batches":          batches,
            "last_checked":     datetime.now(timezone.utc).isoformat(),
            "region":           region,
            "pou":              pou,
            "course":           course,
            "seat_alerts_sent": existing_seat_alerts,
        }

    if updates:
        save_batch_state(updates)
        logger.info(f"State saved to MongoDB ({len(updates)} key(s) updated)")

    if alerts_sent:
        logger.info(f"{alerts_sent} alert(s) sent")
    if any_error:
        sys.exit(1)


def run_debug():
    from scraper import scrape_batches

    print("\n=== DEBUG MODE — no email will be sent ===\n")
    for pref in WATCHLIST:
        region, pou, course = pref["region"], pref["pou"], pref["course"]
        print(f"Scraping: {region} / {pou} / {course}")
        try:
            batches = scrape_batches(region, pou, course)
            print(f"  Found {len(batches)} batch(es):")
            for i, b in enumerate(batches, 1):
                print(f"\n  Batch #{i}:")
                for k, v in b.items():
                    print(f"    {k}: {v}")
        except Exception as e:
            print(f"  ERROR: {e}")
    print()


if __name__ == "__main__":
    if "--test-email" in sys.argv:
        logger.info("Sending test email...")
        from notifier import send_test_email
        send_test_email()

    elif "--debug" in sys.argv:
        run_debug()

    else:
        run_monitor()
