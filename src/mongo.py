from __future__ import annotations

from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

from .logging_utils import log


def get_collection(mongo_url: str, db_name: str, col_name: str) -> Collection:
    client = MongoClient(mongo_url)
    db = client[db_name]
    col = db[col_name]

    # Ensure unique index on 'id' (post id)
    try:
        col.create_index([("id", ASCENDING)], unique=True, name="uniq_id")
    except Exception as e:
        log(f"[mongo] create_index warning: {e}")

    return col


def _is_bad_doc(doc: dict | None) -> bool:
    """
    A doc is 'bad' if we don't have a real API status/body yet.
    We treat these as repairable and allow updates.
    """
    if not doc:
        return True

    # Most important signal: details status
    details = (((doc.get("api") or {}).get("details") or {}).get("res") or {})
    insp = (((doc.get("api") or {}).get("inspection") or {}).get("res") or {})

    d_status = details.get("status")
    i_status = insp.get("status")

    # status missing/null => bad
    if d_status in (None, 0) and i_status in (None, 0):
        return True

    # if we have neither json nor text, it's bad
    d_has_payload = bool(details.get("json") is not None or (details.get("text") or "").strip())
    i_has_payload = bool(insp.get("json") is not None or (insp.get("text") or "").strip())

    if not d_has_payload and not i_has_payload:
        return True

    return False


def already_have(col: Collection, post_id: int) -> bool:
    """
    Return True only if we already have a 'good' doc.
    If doc exists but is bad/incomplete, return False so we refetch + repair.
    """
    doc = col.find_one({"id": int(post_id)}, {"_id": 1, "api": 1})
    return (doc is not None) and (not _is_bad_doc(doc))


def upsert_post(col: Collection, post: dict) -> str:
    """
    Insert or repair.
    Returns one of: "inserted" | "updated" | "skipped"
    """
    post_id = int(post.get("id"))

    existing = col.find_one({"id": post_id}, {"_id": 1, "api": 1})
    if existing is None:
        # Insert new
        try:
            col.insert_one(post)
            return "inserted"
        except Exception as e:
            # In case of race condition, fall through to update logic
            log(f"[mongo] insert_one warning id={post_id}: {e}")

    # If existing is good, skip to avoid rewriting
    if existing is not None and not _is_bad_doc(existing):
        return "skipped"

    # Otherwise, repair/update the doc
    # We replace key fields but keep Mongo _id.
    # You can add "$set": post to fully replace, but safer to set known top-level fields.
    res = col.update_one(
        {"id": post_id},
        {"$set": post},
        upsert=True,
    )

    # If it upserted due to race: inserted, else updated
    if res.upserted_id:
        return "inserted"
    return "updated"
