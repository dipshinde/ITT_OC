"""
db.py
-----
MongoDB persistence layer for the ICAI Batch Monitor.

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
    error_msg: str, consecutive_failures: int, admin_alerted: bool }

Public API
──────────
  ensure_indexes()
  load_state()                       → dict
  save_state(state)
  load_batch_state()                 → dict
  save_batch_state(batch_state)
  record_heartbeat_ok()
  record_heartbeat_fail(error_msg)
  get_heartbeat()                    → dict
  set_heartbeat_admin_alerted(bool)
  delete_user(chat_id)               → bool
  delete_stuck_users()               → int
"""

import os
import logging
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

# --- Validate env vars at import time with a clear error ----------------------

_MONGO_URI = os.environ.get("MONGODB_URI")
if not _MONGO_URI:
    raise EnvironmentError(
        "MONGODB_URI environment variable is not set. "
        "Add it to your Railway service variables."
    )

_DB_NAME = os.environ.get("MONGODB_DB", "icai_bot")

_client = None


def _db():
    global _client
    if _client is None:
        _client = MongoClient(
            _MONGO_URI,
            # Connection timeouts
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=30_000,
            # Automatic retry on transient errors (Atlas default, explicit here)
            retryWrites=True,
            retryReads=True,
            # Connection pool — small pool is fine for a single-process bot
            maxPoolSize=10,
            minPoolSize=1,
        )
    return _client[_DB_NAME]


def _users_col():
    return _db()["users"]


def _batch_col():
    return _db()["batch_state"]


def _heartbeat_col():
    return _db()["heartbeat"]


# ---------------------------------------------------------------------------
# Index management — call once at startup
# ---------------------------------------------------------------------------

def ensure_indexes():
    """
    Create indexes for all collections if they don't already exist.
    Safe to call on every startup (MongoDB skips existing indexes).
    """
    try:
        # users: queried by active + course in cleanup, and by pending
        _users_col().create_index([("active", ASCENDING), ("course", ASCENDING)])
        _users_col().create_index([("pending", ASCENDING)])

        # batch_state: queried by last_checked for monitoring dashboards
        _batch_col().create_index([("last_checked", DESCENDING)])

        logger.info("[DB] Indexes ensured.")
    except PyMongoError as e:
        logger.warning(f"ensure_indexes failed (non-fatal): {e}")


# ---------------------------------------------------------------------------
# Users / offset
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
# Batch state
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
# Heartbeat
# ---------------------------------------------------------------------------

def record_heartbeat_ok():
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


def set_heartbeat_admin_alerted(value: bool):
    """Set or clear the admin_alerted flag after sending/clearing an alert."""
    try:
        _heartbeat_col().update_one(
            {"_id": "status"},
            {"$set": {"admin_alerted": value}},
            upsert=True,
        )
    except PyMongoError as e:
        logger.error(f"set_heartbeat_admin_alerted DB error: {e}")


# ---------------------------------------------------------------------------
# User management helpers
# ---------------------------------------------------------------------------

def delete_user(chat_id: str) -> bool:
    """Fully remove a user document. Returns True if a document was deleted."""
    try:
        result = _users_col().delete_one({"_id": chat_id})
        return result.deleted_count > 0
    except PyMongoError as e:
        logger.error(f"delete_user DB error (chat_id={chat_id}): {e}")
        return False


def delete_stuck_users() -> int:
    """
    Remove user documents that are stuck mid-setup or are dead entries.
    Returns the total number of documents deleted.
    """
    try:
        col = _users_col()

        # Stuck mid-setup: has a 'pending' key but no 'course' yet
        result1 = col.delete_many({
            "pending": {"$exists": True},
            "course":  {"$exists": False},
            "_id":     {"$ne": "__meta__"},
        })

        # Dead entries: inactive, not registered, and no course
        result2 = col.delete_many({
            "active":     False,
            "registered": {"$in": [False, None]},
            "course":     {"$exists": False},
            "_id":        {"$ne": "__meta__"},
        })

        return result1.deleted_count + result2.deleted_count

    except PyMongoError as e:
        logger.error(f"delete_stuck_users DB error: {e}")
        return 0
