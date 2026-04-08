"""
bot.py
------
GitHub Actions-compatible Telegram bot for ICAI batch monitoring.

Each run (every 10 min):
  1. Polls Telegram for new messages / button taps and processes them
  2. Scrapes ICAI for every subscribed user
  3. Sends a Telegram alert if batches changed since last run
  4. Saves updated users.json (committed back to repo by monitor.yml)
"""

import os
import json
import logging
import requests
from bs4 import BeautifulSoup

from scraper import scrape_batches, compute_hash

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

TOKEN    = os.environ["TELEGRAM_BOT_TOKEN"]
API_BASE = f"https://api.telegram.org/bot{TOKEN}"
ICAI_URL = "https://www.icaionlineregistration.org/launchbatchdetail.aspx"
ICAI_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0 Safari/537.36"
}

USERS_FILE = "users.json"

COURSES = [
    "Advanced (ICITSS) MCS Course",
    "Advanced (ICITSS) MCS Course - Weekend",
    "AICITSS - Advanced Information Technology",
    "ICITSS - Information Technology",
    "ICITSS - Orientation Course",
]

# ─── Telegram helpers ─────────────────────────────────────────────────────────

def tg(method: str, **data):
    r = requests.post(f"{API_BASE}/{method}", json=data, timeout=15)
    return r.json()

def send(chat_id, text, markup=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = markup
    return tg("sendMessage", **payload)

def edit(chat_id, message_id, text, markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id,
               "text": text, "parse_mode": "HTML"}
    if markup:
        payload["reply_markup"] = markup
    return tg("editMessageText", **payload)

def answer_cb(cb_id, text="✅"):
    tg("answerCallbackQuery", callback_query_id=cb_id, text=text)

def ikb(rows):
    """Build an inline keyboard. rows = list of list of (label, callback_data)."""
    return {"inline_keyboard": [
        [{"text": t, "callback_data": d} for t, d in row]
        for row in rows
    ]}

# ─── ICAI helpers ─────────────────────────────────────────────────────────────

def fetch_regions():
    """Return list of (label, value) for the Region dropdown."""
    r = requests.get(ICAI_URL, headers=ICAI_HEADERS, timeout=20)
    soup = BeautifulSoup(r.text, "lxml")
    sel = soup.find("select", {"id": "ddl_reg"})
    if not sel:
        return []
    return [(o.text.strip(), o["value"])
            for o in sel.find_all("option")
            if o.get("value") and o["value"] != "Select"]

def fetch_pous(region_value: str):
    """POST region selection and return PoU options as (label, value)."""
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
    headers = {**ICAI_HEADERS,
               "Content-Type": "application/x-www-form-urlencoded",
               "Referer": ICAI_URL}
    r1 = requests.post(ICAI_URL, data=payload, headers=headers, timeout=20)
    soup1 = BeautifulSoup(r1.text, "lxml")
    sel = soup1.find("select", {"id": "ddlPou"})
    if not sel:
        return []
    return [(o.text.strip(), o["value"])
            for o in sel.find_all("option")
            if o.get("value")]

# ─── State (users.json) ───────────────────────────────────────────────────────

def load_state() -> dict:
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE) as f:
            return json.load(f)
    return {"_offset": 0, "users": {}}

def save_state(state: dict):
    with open(USERS_FILE, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)

# ─── Setup flow helpers ───────────────────────────────────────────────────────

def start_setup(chat_id: str, state: dict, message_id=None):
    """Begin the Region → PoU → Course selection flow."""
    regions = fetch_regions()
    if not regions:
        send(chat_id, "❌ Could not reach the ICAI site. Please try again in a minute.")
        return

    # Store regions in pending so we can look up value from label later
    state["users"].setdefault(chat_id, {})
    state["users"][chat_id]["pending"] = {
        "step": "region",
        "region_map": {label: val for label, val in regions}
    }

    rows = [[regions[i], regions[i+1]] if i+1 < len(regions) else [regions[i]]
            for i in range(0, len(regions), 2)]
    markup = ikb([[(label, f"region:{label}") for label, _ in row] for row in rows])

    text = "👋 Welcome! Let's set up your batch alert.\n\n📍 <b>Step 1 of 3 — Select your Region:</b>"
    if message_id:
        edit(chat_id, message_id, text, markup)
    else:
        send(chat_id, text, markup)


def ask_pou(chat_id: str, region_label: str, state: dict, message_id: int):
    pending = state["users"][chat_id].get("pending", {})
    region_map = pending.get("region_map", {})
    region_value = region_map.get(region_label)
    if not region_value:
        send(chat_id, "❌ Region not recognised. Type /watch to restart.")
        return

    pous = fetch_pous(region_value)
    if not pous:
        send(chat_id, "❌ Could not fetch city list. Type /watch to try again.")
        return

    state["users"][chat_id]["pending"] = {
        "step": "pou",
        "region_label": region_label,
        "region_value": region_value,
        "pou_map": {label: val for label, val in pous}
    }

    rows = [[pous[i], pous[i+1]] if i+1 < len(pous) else [pous[i]]
            for i in range(0, len(pous), 2)]
    markup = ikb([[(label, f"pou:{label}") for label, _ in row] for row in rows])
    edit(chat_id, message_id,
         f"🏙 <b>Step 2 of 3 — Select your City/PoU</b>\n(Region: {region_label})",
         markup)


def ask_course(chat_id: str, pou_label: str, state: dict, message_id: int):
    pending = state["users"][chat_id].get("pending", {})
    pou_map = pending.get("pou_map", {})
    pou_value = pou_map.get(pou_label)
    if not pou_value:
        send(chat_id, "❌ City not recognised. Type /watch to restart.")
        return

    state["users"][chat_id]["pending"]["step"]      = "course"
    state["users"][chat_id]["pending"]["pou_label"] = pou_label
    state["users"][chat_id]["pending"]["pou_value"] = pou_value

    markup = ikb([[(c, f"course:{c}")] for c in COURSES])
    edit(chat_id, message_id,
         f"📚 <b>Step 3 of 3 — Select your Course</b>\n(City: {pou_label})",
         markup)


def confirm_subscription(chat_id: str, course: str, state: dict, message_id: int):
    pending = state["users"][chat_id].get("pending", {})
    region_label = pending.get("region_label")
    pou_label    = pending.get("pou_label")

    state["users"][chat_id] = {
        "region":    region_label,
        "pou":       pou_label,
        "course":    course,
        "last_hash": "",   # force first alert on next scrape
        "active":    True,
    }

    edit(chat_id, message_id,
         f"✅ <b>You're all set!</b>\n\n"
         f"📍 Region : {region_label}\n"
         f"🏙 City    : {pou_label}\n"
         f"📚 Course  : {course}\n\n"
         f"I'll check every 10 minutes and alert you the moment a seat opens. 🚨\n\n"
         f"Commands: /status · /stop · /watch (change)")

# ─── Command & callback handlers ─────────────────────────────────────────────

def handle_message(msg: dict, state: dict):
    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text", "").strip()

    if text.startswith("/start") or text.startswith("/watch"):
        start_setup(chat_id, state)

    elif text.startswith("/status"):
        u = state["users"].get(chat_id, {})
        if u.get("active"):
            send(chat_id,
                 f"👁 <b>Currently watching:</b>\n\n"
                 f"📍 {u['region']} / {u['pou']}\n"
                 f"📚 {u['course']}\n\n"
                 f"Use /watch to change or /stop to unsubscribe.")
        else:
            send(chat_id, "You have no active watch. Use /watch to set one up.")

    elif text.startswith("/stop"):
        if chat_id in state["users"]:
            state["users"][chat_id]["active"] = False
        send(chat_id, "🔕 Alerts stopped. Use /watch anytime to resubscribe.")

    elif text.startswith("/help"):
        send(chat_id,
             "<b>ICAI Batch Monitor Bot</b>\n\n"
             "/watch  — set up or change your alert\n"
             "/status — see your current watch\n"
             "/stop   — pause alerts\n"
             "/help   — this message")

    else:
        send(chat_id, "Use /watch to set up your batch alert, or /help for commands.")


def handle_callback(cb: dict, state: dict):
    chat_id    = str(cb["message"]["chat"]["id"])
    message_id = cb["message"]["message_id"]
    data       = cb.get("data", "")
    cb_id      = cb["id"]

    answer_cb(cb_id)
    state["users"].setdefault(chat_id, {})

    if data.startswith("region:"):
        region_label = data[len("region:"):]
        ask_pou(chat_id, region_label, state, message_id)

    elif data.startswith("pou:"):
        pou_label = data[len("pou:"):]
        ask_course(chat_id, pou_label, state, message_id)

    elif data.startswith("course:"):
        course = data[len("course:"):]
        confirm_subscription(chat_id, course, state, message_id)

# ─── Poll Telegram updates ────────────────────────────────────────────────────

def process_updates(state: dict):
    offset  = state.get("_offset", 0)
    resp    = requests.get(f"{API_BASE}/getUpdates",
                           params={"offset": offset, "timeout": 5},
                           timeout=20)
    updates = resp.json().get("result", [])

    for upd in updates:
        state["_offset"] = upd["update_id"] + 1
        if "message" in upd:
            try:
                handle_message(upd["message"], state)
            except Exception as e:
                logger.error(f"Error handling message: {e}")
        elif "callback_query" in upd:
            try:
                handle_callback(upd["callback_query"], state)
            except Exception as e:
                logger.error(f"Error handling callback: {e}")

# ─── Scrape & alert loop ──────────────────────────────────────────────────────

def format_batches(batches: list[dict]) -> str:
    if not batches:
        return "No batches currently listed."
    lines = []
    for b in batches:
        seats = b.get("Available Seats", b.get("AvailableSeats", "?"))
        batch_no = b.get("Batch No", b.get("BatchNo", "?"))
        from_d   = b.get("From Date", b.get("FromDate", ""))
        to_d     = b.get("To Date",   b.get("ToDate",   ""))
        timing   = b.get("Batch Time", b.get("BatchTime", ""))
        lines.append(
            f"  🔹 <b>{batch_no}</b>\n"
            f"     {from_d} → {to_d}  |  {timing}\n"
            f"     Seats available: <b>{seats}</b>"
        )
    return "\n\n".join(lines)


def scrape_and_alert(state: dict):
    users = state.get("users", {})
    for chat_id, u in users.items():
        if not u.get("active"):
            continue
        if "pending" in u:
            continue  # still in setup flow

        region = u.get("region", "")
        pou    = u.get("pou",    "")
        course = u.get("course", "")

        logger.info(f"Scraping for {chat_id}: {region} / {pou} / {course}")
        try:
            batches  = scrape_batches(region, pou, course)
            new_hash = compute_hash(batches)
        except Exception as e:
            logger.error(f"Scrape failed for {chat_id}: {e}")
            continue

        if new_hash != u.get("last_hash", ""):
            logger.info(f"  Change detected for {chat_id} — sending alert")
            u["last_hash"] = new_hash

            has_seats = [b for b in batches
                         if str(b.get("Available Seats", b.get("AvailableSeats", "0"))) not in ("0", "")]

            if has_seats:
                msg = (f"🚨 <b>ICAI Batch Update!</b>\n\n"
                       f"📍 {region} / {pou}\n"
                       f"📚 {course}\n\n"
                       f"<b>Batches with open seats:</b>\n\n"
                       f"{format_batches(has_seats)}\n\n"
                       f"🔗 <a href='https://www.icaionlineregistration.org/launchbatchdetail.aspx'>Register now</a>")
            else:
                msg = (f"ℹ️ <b>ICAI Batch Update</b> — no seats available yet\n\n"
                       f"📍 {region} / {pou}\n"
                       f"📚 {course}\n\n"
                       f"{format_batches(batches)}\n\n"
                       f"I'll keep watching every 10 minutes. 👀")

            send(chat_id, msg)
        else:
            logger.info(f"  No change for {chat_id}")

# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    logger.info("═══ ICAI Telegram Bot — GitHub Actions run ═══")
    state = load_state()

    logger.info("── Processing Telegram updates")
    process_updates(state)
    save_state(state)

    logger.info("── Scraping ICAI for all subscribed users")
    scrape_and_alert(state)
    save_state(state)

    logger.info("── Done. users.json will be committed by workflow.")


if __name__ == "__main__":
    main()
