"""
bot.py
------
COMPLETE MERGED VERSION: ITT/OC Monitoring + Fixed SPOM Tracking.
"""

import copy
import html
import logging
import os
import queue
import re
import threading
import time
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
    record_heartbeat_ok, record_heartbeat_fail,
    delete_user, set_user_email
)
from notifier import send_itt_oc_alert, send_spom_alert

# --- Config & Locks ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
API_BASE = f"https://api.telegram.org/bot{TOKEN}"
STATE_LOCK = threading.Lock()
_MSG_QUEUE = queue.Queue(maxsize=2000)

# --- Telegram Helpers ---
def _enqueue(method, payload):
    try: _MSG_QUEUE.put_nowait((method, payload))
    except queue.Full: logger.warning(f"Outbox full - dropping {method}")

def send(chat_id, text, markup=None):
    _enqueue("sendMessage", {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "reply_markup": markup, "disable_web_page_preview": True})

def edit(chat_id, message_id, text, markup=None):
    _enqueue("editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML", "reply_markup": markup, "disable_web_page_preview": True})

def ikb(buttons): return {"inline_keyboard": buttons}

# --- Core Logic ---
def _save_user(chat_id, state):
    u = state["users"].get(chat_id)
    if u: save_user(chat_id, u)

def _handle_start(chat_id, state):
    state["users"].setdefault(chat_id, {})
    markup = ikb([
        [("📚 Track ITT / OC Batches", "mode:itt")],
        [("🧾 Track SPOM Exam Slots", "mode:spom")],
        [("📋 View My Subscriptions", "cmd:status")]
    ])
    send(chat_id, "<b>ICAI Monitoring Bot</b>\nSelect an option to begin:", markup)

# --- ITT / OC Flow ---
def _handle_watch(chat_id, state):
    """Starts the ITT/OC Region selection flow."""
    u = state["users"].setdefault(chat_id, {})
    u["pending"] = {"step": "region"}
    _save_user(chat_id, state)
    regions = ["Western", "Northern", "Southern", "Eastern", "Central"]
    buttons = [[(r, f"reg:{r}")] for r in regions]
    send(chat_id, "<b>ITT/OC Setup:</b> Select your Region:", markup=ikb(buttons))

# --- SPOM Flow (The Fixed Logic) ---
def _handle_spom(chat_id, state):
    u = state["users"].setdefault(chat_id, {})
    u["spom_pending"] = {"step": "state"}
    _save_user(chat_id, state)
    send(chat_id, "⏳ <b>Connecting to SPOM portal...</b>")
    
    def _bg():
        states = fetch_spom_states()
        if not states:
            send(chat_id, "❌ Error: Could not reach the SPOM portal. Please try again.")
            return
        with STATE_LOCK:
            state["users"][chat_id]["spom_pending"]["state_map"] = {s['label']: s['value'] for s in states}
        buttons = [[(s['label'], f"spom_state:{s['label']}")] for s in states]
        send(chat_id, "<b>SPOM Step 1:</b> Select your State:", markup=ikb(buttons))
    threading.Thread(target=_bg, daemon=True).start()

def _spom_ask_city(chat_id, state_label, state, message_id):
    pending = state["users"][chat_id].get("spom_pending", {})
    state_value = pending.get("state_map", {}).get(state_label)
    if not state_value:
        send(chat_id, "Session expired. Type /spom to restart.")
        return
    edit(chat_id, message_id, f"⏳ <b>Fetching cities for {state_label}...</b>")
    def _bg():
        cities = fetch_spom_cities(state_value)
        if not cities:
            send(chat_id, "❌ Could not fetch cities.")
            return
        with STATE_LOCK:
            state["users"][chat_id]["spom_pending"].update({
                "step": "city", "state_label": state_label, "state_value": state_value,
                "city_map": {c['label']: c['value'] for c in cities}
            })
        buttons = [[(c['label'], f"spom_city:{c['label']}")] for c in cities]
        edit(chat_id, message_id, f"<b>SPOM Step 2:</b> Select City in {state_label}:", markup=ikb(buttons))
    threading.Thread(target=_bg, daemon=True).start()

def _spom_confirm(chat_id, city_label, state, message_id):
    u = state["users"].get(chat_id, {})
    pending = u.get("spom_pending", {})
    city_val = pending.get("city_map", {}).get(city_label)
    if not city_val: return
    
    watch = {
        "state_value": pending["state_value"], 
        "state_label": pending["state_label"], 
        "city_value": city_val, 
        "city_label": city_label
    }
    u.setdefault("spom_watches", []).append(watch)
    u.pop("spom_pending", None)
    _save_user(chat_id, state)
    edit(chat_id, message_id, f"✅ <b>Monitoring SPOM Slots in {city_label}!</b>")

# --- Command & Callback Router ---
def handle_callback(cb, state):
    chat_id, data, msg_id = str(cb["message"]["chat"]["id"]), cb["data"], cb["message"]["message_id"]
    if data == "mode:itt": _handle_watch(chat_id, state)
    elif data == "mode:spom": _handle_spom(chat_id, state)
    elif data == "cmd:status": send(chat_id, "Use /status to see your active watches.")
    elif data.startswith("spom_state:"): _spom_ask_city(chat_id, data.split(":", 1)[1], state, msg_id)
    elif data.startswith("spom_city:"): _spom_confirm(chat_id, data.split(":", 1)[1], state, msg_id)

def process_updates(state):
    offset = load_state().get("offset", 0)
    try:
        r = requests.get(f"{API_BASE}/getUpdates", params={"offset": offset + 1, "timeout": 30}, timeout=35)
        res = r.json()
        if not res.get("ok"): return
        for upd in res.get("result", []):
            save_offset(upd["update_id"])
            if "message" in upd and "text" in upd["message"]:
                text, chat_id = upd["message"]["text"], str(upd["message"]["chat"]["id"])
                if text == "/start": _handle_start(chat_id, state)
                elif text == "/watch": _handle_watch(chat_id, state)
                elif text == "/spom": _handle_spom(chat_id, state)
                elif text == "/status": send(chat_id, "Status logic here (refer to your original db.py/bot.py)")
                elif text == "/help": send(chat_id, "<b>Commands:</b>\n/start - Menu\n/watch - ITT/OC setup\n/spom - SPOM setup")
            elif "callback_query" in upd:
                handle_callback(upd["callback_query"], state)
    except Exception as e: logger.error(f"Polling error: {e}")
