"""
db.py
-----
MongoDB persistence layer for the ICAI Batch Monitor.

Replaces users.json  → collection: 'users'
         state.json  → collection: 'batch_state'

Collections layout
──────────────────
users:
  { _id: chat_id (str), region, pou, course, active, registered, pending, ... }
  + one special doc: { _id: "__meta__", offset: int }

batch_state:
  { _id: key (str), hash, batches, last_checked, region, pou, course,
    seat_alerts_sent }

heartbeat:
  { _id: "status", last_success: ISO str, last_error: ISO str,
    error_msg: str, consecutive_failures: int }

Public API
──────────
  load_state()            → dict   {"_offset": int, "users": {chat_id: {...}}}
  save_state(state)
  load_batch_state()      → dict   {key: {...}}
  save_batch_state(batch_state)
  record_heartbeat_ok()
  record_heartbeat_fail(error_msg)
  get_heartbeat()         → dict
"""

import os
import logging
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

_MONGO_URI = os.environ["MONGODB_URI"]
_DB_NAME   = os.environ.get("MONGODB_DB", "icai_bot")

_client = None


def _db():
    global _client
    if _client is None:
        _client = MongoClient(_MONGO_URI, serverSelectionTimeoutMS=10_000)
    return _client[_DB_NAME]


def _users_col():
    return _db()["users"]


def _batch_col():
    return _db()["batch_state"]


def _heartbeat_col():
    return _db()["heartbeat"]


# ---------------------------------------------------------------------------
# Users / offset  (replaces users.json)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        col  = _users_col()
        docs = list(col.find({}))

        offset = 0
        users  = {}

        for doc in docs:
            doc_id = doc["_id"]
            if doc_id == "__meta__":
                offset = doc.get("offset", 0)
            else:
                users[doc_id] = {k: v for k, v in doc.items() if k != "_id"}

        return {"_offset": offset, "users": users}

    except PyMongoError as e:
        logger.error(f"load_state DB error: {e}")
        return {"_offset": 0, "users": {}}


def save_state(state: dict):
    try:
        col = _users_col()
        ops = []

        ops.append(UpdateOne(
            {"_id": "__meta__"},
            {"$set": {"offset": state.get("_offset", 0)}},
            upsert=True,
        ))

        for chat_id, user_data in state.get("users", {}).items():
            ops.append(UpdateOne(
                {"_id": chat_id},
                {"$set": user_data},
                upsert=True,
            ))

        if ops:
            col.bulk_write(ops, ordered=False)

    except PyMongoError as e:
        logger.error(f"save_state DB error: {e}")


# ---------------------------------------------------------------------------
# Batch state  (replaces state.json)
# ---------------------------------------------------------------------------

def load_batch_state() -> dict:
    try:
        col  = _batch_col()
        docs = list(col.find({}))
        return {doc["_id"]: {k: v for k, v in doc.items() if k != "_id"} for doc in docs}

    except PyMongoError as e:
        logger.error(f"load_batch_state DB error: {e}")
        return {}


def save_batch_state(batch_state: dict):
    try:
        col = _batch_col()
        ops = []

        for key, data in batch_state.items():
            ops.append(UpdateOne(
                {"_id": key},
                {"$set": {
                    **data,
                    "last_checked": datetime.now(timezone.utc).isoformat(),
                }},
                upsert=True,
            ))

        if ops:
            col.bulk_write(ops, ordered=False)

    except PyMongoError as e:
        logger.error(f"save_batch_state DB error: {e}")


# ---------------------------------------------------------------------------
# Heartbeat  (scraper health tracking)
# ---------------------------------------------------------------------------

def record_heartbeat_ok():
    """Call this after every successful scrape cycle."""
    try:
        _heartbeat_col().update_one(
            {"_id": "status"},
            {"$set": {
                "last_success":         datetime.now(timezone.utc).isoformat(),
                "consecutive_failures": 0,
                "error_msg":            None,
            },
             "$setOnInsert": {"last_error": None}},
            upsert=True,
        )
    except PyMongoError as e:
        logger.error(f"record_heartbeat_ok DB error: {e}")


def record_heartbeat_fail(error_msg: str):
    """Call this when a scrape cycle throws an exception."""
    try:
        _heartbeat_col().update_one(
            {"_id": "status"},
            {"$set": {
                "last_error": datetime.now(timezone.utc).isoformat(),
                "error_msg":  str(error_msg),
            },
             "$inc": {"consecutive_failures": 1}},
            upsert=True,
        )
    except PyMongoError as e:
        logger.error(f"record_heartbeat_fail DB error: {e}")


def get_heartbeat() -> dict:
    try:
        doc = _heartbeat_col().find_one({"_id": "status"})
        return doc or {}
    except PyMongoError as e:
        logger.error(f"get_heartbeat DB error: {e}")
        return {}
