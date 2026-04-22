"""
db.py
-----
MongoDB persistence layer for the ICAI Batch Monitor.

Collections layout
──────────────────
users:
  { _id: chat_id (str), watchlist, active, email, email_enabled,
    spom_watches, pending, ... }
  + one special doc: { _id: "__meta__", offset: int }

batch_state:
  { _id: "region|pou|course", hash, struct_hash, batches, last_checked,
    region, pou, course, seat_alerts_sent }

spom_state:
  { _id: "state_val|city_val|centre_val",
    available_dates, booked_dates, hash,
    last_checked, state_label, city_label, centre_label }

heartbeat:
  { _id: "status", last_success, last_error, error_msg,
    consecutive_failures, admin_alerted }

Public API
──────────
  ensure_indexes()
  load_state()                       -> dict
  save_state(state)
  load_batch_state()                 -> dict
  save_batch_state(batch_state)
  load_spom_state()                  -> dict
  save_spom_state(spom_state)
  record_heartbeat_ok()
  record_heartbeat_fail(error_msg)
  get_heartbeat()                    -> dict
  set_heartbeat_admin_alerted(bool)
  delete_user(chat_id)               -> bool
  delete_stuck_users()               -> int
  set_user_email(chat_id, email, enabled)
  get_user_email(chat_id)            -> str | None
  get_users_with_email()             -> list[dict]
"""

import os
import logging
import threading
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne, ReplaceOne, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)

_MONGO_URI = os.environ.get("MONGODB_URI")
if not _MONGO_URI:
    raise EnvironmentError(
        "MONGODB_URI environment variable is not set. "
        "Add it to your Railway service variables."
    )

_DB_NAME    = os.environ.get("MONGODB_DB", "icai_bot")
_client     = None
# FIX: double-checked locking so multiple threads don't race to create MongoClient
_client_lock = threading.Lock()


def _db():
    global _client
    # Fast path — avoid lock on every call once initialised
    if _client is None:
        with _client_lock:
            if _client is None:   # re-check inside lock
                _client = MongoClient(
                    _MONGO_URI,
                    serverSelectionTimeoutMS=10_000,
                    connectTimeoutMS=10_000,
                    socketTimeoutMS=30_000,
                    retryWrites=True,
                    retryReads=True,
                    maxPoolSize=10,
                    minPoolSize=1,
                )
    return _client[_DB_NAME]


def _users_col():     return _db()["users"]
def _batch_col():     return _db()["batch_state"]
def _spom_col():      return _db()["spom_state"]
def _heartbeat_col(): return _db()["heartbeat"]


# ---------------------------------------------------------------------------
# Index management
# ---------------------------------------------------------------------------

def ensure_indexes():
    try:
        _users_col().create_index([("active",       ASCENDING), ("course", ASCENDING)])
        _users_col().create_index([("pending",       ASCENDING)])
        _users_col().create_index([("email_enabled", ASCENDING)])
        _batch_col().create_index([("last_checked",  DESCENDING)])
        _spom_col().create_index( [("last_checked",  DESCENDING)])
        logger.info("[DB] Indexes ensured.")
    except PyMongoError as e:
        logger.warning(f"ensure_indexes failed (non-fatal): {e}")


# ---------------------------------------------------------------------------
# Users / offset
# ---------------------------------------------------------------------------

def load_state() -> dict:
    try:
        docs   = list(_users_col().find({}))
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
    """
    Persist ALL user state to MongoDB in one bulk_write.

    Use this ONLY at startup (migrations) or shutdown.
    For runtime updates prefer save_user() or save_offset() — they write
    exactly one document instead of the entire collection.

    FIX: Uses ReplaceOne instead of UpdateOne/$set so that fields removed
    from the in-memory dict (e.g. legacy region/pou/course after migration)
    are also removed from MongoDB.  UpdateOne/$set only adds/updates fields
    — it never deletes stale ones, causing ghost fields that survive restarts.
    """
    try:
        ops = [UpdateOne(
            {"_id": "__meta__"},
            {"$set": {"offset": state.get("_offset", 0)}},
            upsert=True,
        )]
        for chat_id, user_data in state.get("users", {}).items():
            # ReplaceOne fully replaces the document — no ghost fields survive.
            ops.append(ReplaceOne(
                {"_id": chat_id},
                {"_id": chat_id, **user_data},
                upsert=True,
            ))
        if ops:
            _users_col().bulk_write(ops, ordered=False)
    except PyMongoError as e:
        logger.error(f"save_state DB error: {e}")


def save_user(chat_id: str, user_data: dict):
    """
    Granular single-user write — replaces only that user's MongoDB document.

    Use this instead of save_state() whenever a handler modifies one user.
    Writes exactly ONE document vs. N documents (one per user) for save_state,
    cutting MongoDB write load proportionally to the number of active users.

    ReplaceOne is used (not UpdateOne/$set) so stale/removed fields are
    purged from the DB, exactly like save_state() does in its bulk path.
    """
    try:
        _users_col().replace_one(
            {"_id": chat_id},
            {"_id": chat_id, **user_data},
            upsert=True,
        )
    except PyMongoError as e:
        logger.error(f"save_user DB error (chat_id={chat_id}): {e}")


def save_offset(offset: int):
    """
    Persist only the Telegram update offset — one tiny $set, no user data touched.

    Use in the polling loop instead of save_state() — the loop only needs to
    persist the offset, not re-serialise every user document on every tick.
    """
    try:
        _users_col().update_one(
            {"_id": "__meta__"},
            {"$set": {"offset": offset}},
            upsert=True,
        )
    except PyMongoError as e:
        logger.error(f"save_offset DB error: {e}")


# ---------------------------------------------------------------------------
# Batch state (ITT / OC)
# ---------------------------------------------------------------------------

def load_batch_state() -> dict:
    try:
        docs = list(_batch_col().find({}))
        return {d["_id"]: {k: v for k, v in d.items() if k != "_id"} for d in docs}
    except PyMongoError as e:
        logger.error(f"load_batch_state DB error: {e}")
        return {}


def save_batch_state(batch_state: dict):
    try:
        ops = []
        for key, data in batch_state.items():
            ops.append(UpdateOne(
                {"_id": key},
                {"$set": {**data, "last_checked": datetime.now(timezone.utc).isoformat()}},
                upsert=True,
            ))
        if ops:
            _batch_col().bulk_write(ops, ordered=False)
    except PyMongoError as e:
        logger.error(f"save_batch_state DB error: {e}")


def clear_seat_alerts_for_key(key: str):
    """
    FIX: Targeted $unset for seat_alerts_sent on one key — avoids loading
    the entire batch collection just to clear one field.
    """
    try:
        _batch_col().update_one({"_id": key}, {"$unset": {"seat_alerts_sent": ""}})
    except PyMongoError as e:
        logger.warning(f"clear_seat_alerts_for_key DB error ({key}): {e}")


# ---------------------------------------------------------------------------
# SPOM state
# ---------------------------------------------------------------------------

def load_spom_state() -> dict:
    """
    Load SPOM slot availability state from MongoDB.
    Returns dict keyed by "state_val|city_val|centre_val".
    Each entry:
      { available_dates: [...], booked_dates: [...], hash: str,
        last_checked: ISO str, state_label, city_label, centre_label }
    """
    try:
        docs = list(_spom_col().find({}))
        return {d["_id"]: {k: v for k, v in d.items() if k != "_id"} for d in docs}
    except PyMongoError as e:
        logger.error(f"load_spom_state DB error: {e}")
        return {}


def save_spom_state(spom_state: dict):
    """
    Persist SPOM slot availability state to MongoDB.
    spom_state is keyed by "state_val|city_val|centre_val".
    """
    try:
        ops = []
        for key, data in spom_state.items():
            ops.append(UpdateOne(
                {"_id": key},
                {"$set": {**data, "last_checked": datetime.now(timezone.utc).isoformat()}},
                upsert=True,
            ))
        if ops:
            _spom_col().bulk_write(ops, ordered=False)
    except PyMongoError as e:
        logger.error(f"save_spom_state DB error: {e}")


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
            }, "$setOnInsert": {"last_error": None}},
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
            }, "$inc": {"consecutive_failures": 1}},
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
    try:
        _heartbeat_col().update_one(
            {"_id": "status"},
            {"$set": {"admin_alerted": value}},
            upsert=True,
        )
    except PyMongoError as e:
        logger.error(f"set_heartbeat_admin_alerted DB error: {e}")


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------

def delete_user(chat_id: str) -> bool:
    try:
        result = _users_col().delete_one({"_id": chat_id})
        return result.deleted_count > 0
    except PyMongoError as e:
        logger.error(f"delete_user DB error (chat_id={chat_id}): {e}")
        return False


def delete_stuck_users() -> int:
    try:
        col = _users_col()
        r1  = col.delete_many({
            "pending":   {"$exists": True},
            "course":    {"$exists": False},
            "watchlist": {"$exists": False},
            "_id":       {"$ne": "__meta__"},
        })
        r2  = col.delete_many({
            "active":     False,
            "registered": {"$in": [False, None]},
            "course":     {"$exists": False},
            "_id":        {"$ne": "__meta__"},
        })
        return r1.deleted_count + r2.deleted_count
    except PyMongoError as e:
        logger.error(f"delete_stuck_users DB error: {e}")
        return 0


# ---------------------------------------------------------------------------
# Email helpers
# ---------------------------------------------------------------------------

def set_user_email(chat_id: str, email: str, enabled: bool = True):
    """
    Persist a user's email address + enabled flag to MongoDB.
    The caller is also responsible for updating the in-memory state dict.
    """
    try:
        _users_col().update_one(
            {"_id": chat_id},
            {"$set": {"email": email.strip().lower(), "email_enabled": enabled}},
            upsert=True,
        )
    except PyMongoError as e:
        logger.error(f"set_user_email DB error (chat_id={chat_id}): {e}")


def get_user_email(chat_id: str) -> str | None:
    """Return the stored email address for a user, or None."""
    try:
        doc = _users_col().find_one({"_id": chat_id}, {"email": 1})
        return (doc or {}).get("email")
    except PyMongoError as e:
        logger.error(f"get_user_email DB error (chat_id={chat_id}): {e}")
        return None


def get_users_with_email() -> list[dict]:
    """
    Return all user docs where email_enabled=True and email is set.
    Used by SPOM/batch monitors to send per-user email alerts.
    Each doc has at minimum: _id, email, spom_watches, watchlist.
    """
    try:
        return list(_users_col().find(
            {"email_enabled": True, "email": {"$exists": True, "$ne": ""}},
            {"_id": 1, "email": 1, "spom_watches": 1, "watchlist": 1},
        ))
    except PyMongoError as e:
        logger.error(f"get_users_with_email DB error: {e}")
        return []
