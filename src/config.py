from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _get(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


def _get_int(name: str, default: int) -> int:
    v = _get(name)
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _get_float(name: str, default: float) -> float:
    v = _get(name)
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


@dataclass
class Settings:
    target_url: str
    headless: bool

    mongo_url: str
    mongo_db: str
    mongo_collection: str

    check_interval_hours: int
    scroll_pause_sec: float

    api_lang: str

    # API headers
    authorization: Optional[str]
    token: Optional[str]
    user_id: Optional[str]
    gbuuid: Optional[str]
    device: Optional[str]
    accept_language: Optional[str]
    user_agent: Optional[str]
    cookie: Optional[str]


def get_settings() -> Settings:
    return Settings(
        target_url=_get("TARGET_URL", "https://syarah.com/filters"),
        headless=(_get("HEADLESS", "false").lower() == "true"),

        mongo_url=_get("MONGO_URL", "") or "",
        mongo_db=_get("MONGO_DB", "projectForever") or "projectForever",
        mongo_collection=_get("MONGO_COLLECTION", "syarahUsed") or "syarahUsed",

        check_interval_hours=_get_int("CHECK_INTERVAL_HOURS", 48),
        scroll_pause_sec=_get_float("SCROLL_PAUSE_SEC", 1.5),

        api_lang=_get("SYARAH_API_LANG", "ar") or "ar",

        authorization=_get("SYARAH_AUTHORIZATION"),
        token=_get("SYARAH_TOKEN"),
        user_id=_get("SYARAH_USER_ID"),
        gbuuid=_get("SYARAH_GBUUID"),
        device=_get("SYARAH_DEVICE", "web"),
        accept_language=_get("SYARAH_ACCEPT_LANGUAGE"),
        user_agent=_get("SYARAH_USER_AGENT"),
        cookie=_get("SYARAH_COOKIE"),
    )
