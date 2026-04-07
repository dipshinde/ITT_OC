"""
monitor.py
----------
Entry point for the ICAI batch monitor.
Run by GitHub Actions every 10 minutes.

Usage:
  python monitor.py              → normal monitoring run
  python monitor.py --test-email → send a test email and exit
  python monitor.py --debug      → print discovered batches without sending email
"""

import sys
import os
import json
import logging
from datetime import datetime

# ── Configure logging (GitHub Actions shows stdout) ───────────────────────────
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
COURSE = "AICITSS - Advanced Information Technology"

# To monitor multiple combinations, add more dicts to this list:
# WATCHLIST = [
#     {"region": "Western", "pou": "Pune", "course": "AICITSS - Advanced Information Technology"},
#     {"region": "Western", "pou": "Pune", "course": "Advanced (ICITSS) MCS Course"},
# ]

WATCHLIST = [
    {"region": REGION, "pou": POU, "course": COURSE},
]

STATE_FILE = "state.json"

# ─────────────────────────────────────────────────────────────────────────────


def load_full_state() -> dict:
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_full_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def make_key(region: str, pou: str, course: str) -> str:
    return f"{region}|{pou}|{course}"


def run_monitor():
    from scraper import scrape_batches, compute_hash
    from notifier import send_alert

    full_state = load_full_state()
    any_error = False
    alerts_sent = 0

    for pref in WATCHLIST:
        region = pref["region"]
        pou    = pref["pou"]
        course = pref["course"]
        key    = make_key(region, pou, course)

        logger.info(f"─── Checking: {region} / {pou} / {course}")

        try:
            batches = scrape_batches(region, pou, course)
        except Exception as e:
            logger.error(f"Scrape failed for {key}: {e}")
            any_error = True
            continue

        new_hash = compute_hash(batches)
        old_entry = full_state.get(key, {})
        old_hash  = old_entry.get("hash", "")

        changed   = new_hash != old_hash
        is_first  = old_hash == ""

        if changed and not is_first:
            logger.info(f"🔔 CHANGE DETECTED for {key}")
            logger.info(f"   Batches found: {len(batches)}")
            try:
                send_alert(batches, region, pou, course)
                alerts_sent += 1
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
                any_error = True

        elif is_first:
            logger.info(f"📋 First run — baseline saved ({len(batches)} batch(es) currently listed)")

        else:
            logger.info(f"✓  No change ({len(batches)} batch(es), hash={new_hash[:12]}...)")

        # Always update state with latest data
        full_state[key] = {
            "hash": new_hash,
            "batches": batches,
            "last_checked": datetime.utcnow().isoformat() + "Z",
            "region": region,
            "pou": pou,
            "course": course,
        }

    save_full_state(full_state)
    logger.info(f"State saved to {STATE_FILE}")

    if alerts_sent:
        logger.info(f"✅ {alerts_sent} alert(s) sent")
    if any_error:
        sys.exit(1)  # Non-zero exit → GitHub Actions marks run as failed → visible in UI


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
    print("\n")


if __name__ == "__main__":
    if "--test-email" in sys.argv:
        logger.info("Sending test email...")
        from notifier import send_test_email
        send_test_email()

    elif "--debug" in sys.argv:
        run_debug()

    else:
        run_monitor()
