# mongo.py
from __future__ import annotations

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from .logging_utils import log


def get_collection(mongo_url: str, db_name: str, col_name: str) -> Collection:
    client = MongoClient(mongo_url)
    db = client[db_name]
    col = db[col_name]

    try:
        col.create_index([("id", ASCENDING)], unique=True, name="uniq_id")
    except Exception as e:
        log(f"[mongo] create_index warning: {e}")

    return col


def _is_good_status(x) -> bool:
    try:
        x = int(x)
    except Exception:
        return False
    return x not in (0, 401) and 200 <= x < 500  # treat 2xx/3xx/4xx(≠401) as "real response"


def _is_bad_doc(doc: dict | None) -> bool:
    """
    Since you no longer store `api`, we judge by the tiny status fields.
    A doc is BAD if it doesn't have real status codes yet (or looks like unauthorized/empty).
    """
    if not doc:
        return True

    d_status = doc.get("details_status")
    i_status = doc.get("inspection_status")

    # If either is a real status, consider it "good enough"
    if _is_good_status(d_status) or _is_good_status(i_status):
        return False

    return True


def already_have(col: Collection, post_id: int) -> bool:
    """
    True only if we already have a "good" doc.
    """
    doc = col.find_one({"id": int(post_id)}, {"_id": 1, "details_status": 1, "inspection_status": 1})
    return (doc is not None) and (not _is_bad_doc(doc))


def upsert_post(col: Collection, post: dict) -> str:
    """
    Insert or repair.
    Returns: "inserted" | "updated" | "skipped"
    """
    post_id = int(post.get("id"))

    existing = col.find_one({"id": post_id}, {"_id": 1, "details_status": 1, "inspection_status": 1})

    if existing is None:
        try:
            col.insert_one(post)
            return "inserted"
        except Exception as e:
            log(f"[mongo] insert_one warning id={post_id}: {e}")
            existing = col.find_one({"id": post_id}, {"_id": 1, "details_status": 1, "inspection_status": 1})

    if existing is not None and not _is_bad_doc(existing):
        return "skipped"

    res = col.update_one({"id": post_id}, {"$set": post}, upsert=True)
    if res.upserted_id:
        return "inserted"
    return "updated"
