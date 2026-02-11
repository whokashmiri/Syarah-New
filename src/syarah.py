from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple

import requests

from .logging_utils import log

BASE = "https://syarah.com/en/filters?condition_id=1"

SEL_TITLE_AREA = "div.UnbxdTitleArea-module__h1Area"
SEL_CARDS_CONTAINER = "div.UnbxdCards-module__allCarsResult"
CARD_ID_PREFIX = "modern-card_post-"


JS_SCROLL_STEP = """
(() => {
  const beforeY = window.scrollY;
  window.scrollBy(0, Math.max(900, window.innerHeight * 0.95));
  const afterY = window.scrollY;
  return { beforeY, afterY, h: document.body.scrollHeight };
})()
""".strip()


def _js_str(s: str) -> str:
    return json.dumps(s)


# -----------------------------
# nodriver RemoteObject unwrap
# -----------------------------
def unwrap_remote(obj: Any) -> Any:
    if isinstance(obj, dict) and "type" in obj and "value" in obj:
        t = obj.get("type")
        v = obj.get("value")
        if t in ("number", "string", "boolean"):
            return v
        if t == "null":
            return None
        if t == "array":
            return [unwrap_remote(x) for x in (v or [])] if isinstance(v, list) else []
        if t == "object":
            return {k: unwrap_remote(val) for k, val in (v or {}).items()} if isinstance(v, dict) else {}
        return unwrap_remote(v)
    if isinstance(obj, list):
        return [unwrap_remote(x) for x in obj]
    if isinstance(obj, dict):
        return {k: unwrap_remote(v) for k, v in obj.items()}
    return obj


# -----------------------------
# Listing page JS evaluators
# -----------------------------
def js_get_total() -> str:
    return f"""
(() => {{
  const area = document.querySelector({_js_str(SEL_TITLE_AREA)});
  if (!area) return null;
  const spans = Array.from(area.querySelectorAll('span')).map(s => (s.textContent||'').trim());
  const n = spans.find(t => /^\\d+$/.test((t||'').replace(/\\s+/g,'')));
  return n ? parseInt(n, 10) : null;
}})()
""".strip()


def js_get_visible_cards() -> str:
    return f"""
(() => {{
  const prefix = {json.dumps(CARD_ID_PREFIX)};
  const container = document.querySelector({_js_str(SEL_CARDS_CONTAINER)});
  const root = container || document;

  const nodes = Array.from(root.querySelectorAll(`div[id^="${{prefix}}"]`));
  const out = [];

  for (const el of nodes) {{
    const idAttr = (el.getAttribute('id') || '').trim();
    const m = idAttr.match(/^modern-card_post-(\\d+)$/);
    if (!m) continue;

    const idNum = parseInt(m[1], 10);
    if (!Number.isFinite(idNum)) continue;

    const a = el.querySelector('a[href^="/cardetail/"]');
    if (!a) continue;

    const href = (a.getAttribute('href') || '').trim();
    if (!href) continue;

    out.push([idNum, href]);
  }}

  const seen = new Set();
  const uniq = [];
  for (const pair of out) {{
    const id = pair[0];
    if (seen.has(id)) continue;
    seen.add(id);
    uniq.push(pair);
  }}
  return uniq;
}})()
""".strip()


async def wait_for_listing_ready(page: Any, timeout: float = 60.0) -> None:
    end = asyncio.get_event_loop().time() + timeout
    while True:
        try:
            ok = unwrap_remote(await page.evaluate(f"Boolean(document.querySelector({_js_str(SEL_TITLE_AREA)}))"))
            if ok:
                return
        except Exception:
            pass

        if asyncio.get_event_loop().time() > end:
            raise TimeoutError("Listing page not ready (title area missing).")
        await page.sleep(0.5)


async def read_total_ads(page: Any) -> Optional[int]:
    try:
        v = unwrap_remote(await page.evaluate(js_get_total()))
        return int(v) if v is not None else None
    except Exception as e:
        log(f"[total] evaluate error: {e}")
        return None


async def read_visible_cards(page: Any) -> List[Dict[str, Any]]:
    try:
        raw = unwrap_remote(await page.evaluate(js_get_visible_cards()))
        if not isinstance(raw, list):
            return []

        out: List[Dict[str, Any]] = []
        for item in raw:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                pid, href = item[0], item[1]
                if isinstance(pid, (int, float)) and str(href or "").strip():
                    out.append({"id": int(pid), "href": str(href)})
        return out
    except Exception as e:
        log(f"[cards] evaluate error: {e}")
        return []


def abs_url(href: str) -> str:
    if not href:
        return ""
    if href.startswith("http"):
        return href
    return BASE + href


# -----------------------------
# API URLs + requests session
# -----------------------------
def build_api_urls(lang: str, post_id: int) -> Tuple[str, str]:
    base = f"https://syarah.com/api/syarah_v1/{lang}/post/view-online"
    u1 = f"{base}?id={post_id}&thumb_size=300&device_type=web&include=inspection"
    u2 = (
        f"{base}?id={post_id}&thumb_size=300&device_type=web&should_redirect=1&include="
        "details,price,story,quality,meta,analytics,campaign,g4Data,options,featuredImage,"
        "gallery_section,gallery,fuel,faqs,footerdetails,footer"
    )
    return u1, u2


def build_api_session(settings) -> requests.Session:
    """
    Build a requests session that matches DevTools as closely as we can.
    """
    s = requests.Session()

    headers = {
        "accept": "application/json",
        "device": settings.device or "web",
        "accept-enhancedstatuscodes": "1",
    }

    if getattr(settings, "accept_language", None):
        headers["accept-language"] = settings.accept_language
    if getattr(settings, "user_agent", None):
        headers["user-agent"] = settings.user_agent
    if getattr(settings, "gbuuid", None):
        headers["gbuuid"] = settings.gbuuid
    if getattr(settings, "authorization", None):
        headers["authorization"] = settings.authorization
    if getattr(settings, "token", None):
        headers["token"] = settings.token
    if getattr(settings, "user_id", None):
        headers["user-id"] = settings.user_id
    if getattr(settings, "cookie", None):
        headers["cookie"] = settings.cookie

    s.headers.update(headers)
    return s


def _req_get_json_or_text(sess: requests.Session, url: str, referer: str) -> Dict[str, Any]:
    try:
        r = sess.get(url, headers={"referer": referer}, timeout=30)
        ct = r.headers.get("content-type", "")
        text = r.text or ""

        parsed = None
        if "application/json" in ct.lower():
            try:
                parsed = r.json()
            except Exception:
                parsed = None

        return {
            "ok": bool(r.ok),
            "status": int(r.status_code),
            "url": url,
            "contentType": ct,
            "json": parsed,
            "text": None if parsed is not None else text,
            "textLen": len(text),
        }
    except Exception as e:
        return {
            "ok": False,
            "status": 0,
            "url": url,
            "contentType": "",
            "json": None,
            "text": None,
            "textLen": 0,
            "error": str(e),
        }


# -----------------------------
# Helpers for flattening
# -----------------------------
def _dig(obj: Any, path: str) -> Any:
    """Navigate nested dict/list by dot-separated path."""
    if obj is None:
        return None
    keys = path.split(".")
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key)
        elif isinstance(obj, list) and key.isdigit():
            idx = int(key)
            obj = obj[idx] if 0 <= idx < len(obj) else None
        else:
            return None
        if obj is None:
            return None
    return obj


def _first_str(*vals: Any) -> Optional[str]:
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _first_num(*vals: Any) -> Optional[float]:
    for v in vals:
        if isinstance(v, (int, float)):
            return v
        if isinstance(v, str):
            vv = "".join(ch for ch in v if ch.isdigit() or ch == ".")
            if vv:
                try:
                    return float(vv) if "." in vv else int(vv)
                except Exception:
                    pass
    return None


def _slug_en(s: Optional[str]) -> str:
    s = (s or "").strip().lower()
    out = []
    prev_us = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    key = "".join(out).strip("_")
    return key or "unknown"


def flatten_inspection_kv(categories: Any) -> Dict[str, Dict[str, Any]]:
    """
    Input: car_report list (categories)
    Output:
      { "engine": { "الصوفة الامامية": "جيد", ... }, ... }
    """
    if not isinstance(categories, list):
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    used = set()

    for cat in categories:
        if not isinstance(cat, dict):
            continue

        key = _slug_en(cat.get("category_name_en"))
        base = key
        i = 2
        while key in used:
            key = f"{base}_{i}"
            i += 1
        used.add(key)

        subs = cat.get("sub") or []
        if not isinstance(subs, list):
            subs = []

        kv: Dict[str, Any] = {}
        for s in subs:
            if not isinstance(s, dict):
                continue
            name = (s.get("name") or "").strip()
            if not name:
                continue
            kv[name] = s.get("rate")  # store rate text only
        out[key] = kv

    return out


def flatten_post(inspection_json: dict, details_json: dict) -> dict:
    """
    Flatten the Syarah API responses into a searchable structure.
    """
    ins_data = _dig(inspection_json, "data.inspection") or {}
    det_data = _dig(details_json, "data") or {}

    flat: Dict[str, Any] = {}

    # ========== BASIC IDENTITY ==========
    flat["post_id"] = _first_num(
        _dig(det_data, "details.id"),
        # _dig(details_json, "id"),
        # _dig(inspection_json, "id"),
    )

    flat["title"] = _first_str(
        _dig(det_data, "details.title"),
        _dig(det_data, "meta.title"),
    )

    # ========== VEHICLE SPECS ==========
    details_card = _dig(det_data, "details.details_card") or {}

    flat["brand"] = _first_str(_dig(details_card, "make.name"), _dig(details_card, "make.altName"))
    flat["model"] = _first_str(_dig(details_card, "model.name"), _dig(details_card, "model.altName"))
    flat["trim"] = _first_str(_dig(details_card, "extension.name"), _dig(details_card, "extension.altName"))

    flat["year"] = _first_num(_dig(details_card, "years.id"), _dig(details_card, "years.name"))
    flat["mileage_km"] = _first_num(_dig(details_card, "milage.id"), _dig(details_card, "milage.name"))

    # ========== LOCATION & ORIGIN ==========
    flat["city"] = _first_str(_dig(det_data, "g4Data.post_city"))
    flat["origin"] = _first_str(_dig(details_card, "car_origin.name"))

    # ========== MECHANICAL ==========
    flat["fuel_type"] = _first_str(_dig(details_card, "fuel_types.name"), _dig(det_data, "fuel.fuel_type"))
    flat["transmission"] = _first_str(_dig(details_card, "transmission_type.name"))
    flat["engine_size"] = _first_str(_dig(details_card, "engine_size.name"))

    flat["cylinders"] = _first_num(_dig(details_card, "cylinders.id"), _dig(details_card, "cylinders.name"))
    flat["horse_power"] = _first_num(_dig(details_card, "horse_power.id"), _dig(details_card, "horse_power.name"))
    flat["drivetrain"] = _first_str(_dig(details_card, "drivetrain_type.name"))
    flat["engine_type"] = _first_str(_dig(details_card, "engine_type.name"))

    flat["fuel_tank_liters"] = _first_num(_dig(details_card, "fuel_tank.id"), _dig(details_card, "fuel_tank.name"))
    flat["fuel_economy_kml"] = _first_num(_dig(det_data, "fuel.fuel_economy"))

    # ========== PHYSICAL ==========
    # flat["exterior_color"] = _first_str(_dig(details_card, "exterior_color.name"))
    # flat["exterior_color_code"] = _first_str(_dig(details_card, "exterior_color.code"))
    # flat["interior_color"] = _first_str(_dig(details_card, "interior_color.name"))
    # flat["interior_color_code"] = _first_str(_dig(details_card, "interior_color.code"))

    # flat["doors"] = _first_num(_dig(details_card, "doors.id"), _dig(details_card, "doors.name"))
    flat["seats"] = _first_num(_dig(details_card, "seats.id"), _dig(details_card, "seats.name"))
    # flat["number_of_keys"] = _first_num(_dig(details_card, "number_of_keys.id"), _dig(details_card, "number_of_keys.name"))

    # ========== CONDITION ==========
    # flat["condition"] = _first_str(_dig(details_card, "is_new.name"), _dig(details_card, "is_new.altName"))
    # flat["is_preowned"] = _dig(det_data, "details.is_preowned")
    # flat["is_test"] = _dig(det_data, "details.is_test")

    # ========== PRICING ==========
    price_data = _dig(det_data, "price") or {}
    flat["price_cash"] = _first_num(_dig(price_data, "vat_price.text"), _dig(det_data, "analytics.price"))
    flat["price_monthly"] = _first_num(_dig(price_data, "finance_price.text"))
    # flat["currency"] = _first_str(_dig(price_data, "currency"))

    # flat["first_payment"] = _first_num(_dig(price_data, "finance_price.first_payment"))
    # flat["last_payment"] = _first_num(_dig(price_data, "finance_price.last_payment"))
    # flat["installment_period"] = _first_num(_dig(price_data, "finance_price.installment_period"))

    # ========== INSPECTION (FLAT + KV) ==========
    # flat["inspection_date"] = _first_str(_dig(ins_data, "report_date"))
    flat["chassis_number"] = _first_str(_dig(ins_data, "chassis_number"))
    flat["plate_number"] = _first_str(_dig(ins_data, "plate_number"))

    car_report = _dig(ins_data, "car_report") or []
    # flat["inspection_categories_count"] = len(car_report) if isinstance(car_report, list) else 0

    total_points = 0
    if isinstance(car_report, list):
        for category in car_report:
            if isinstance(category, dict):
                sub = category.get("sub", [])
                total_points += len(sub) if isinstance(sub, list) else 0
    # flat["inspection_points_total"] = total_points

    # ✅ the format you asked for
    # flat["inspection_kv"] = flatten_inspection_kv(car_report)

    # ========== BODY DAMAGE ==========
    external_body = _dig(ins_data, "external_body") or {}
    body_sub = _dig(external_body, "sub") or []
    # flat["body_issues_count"] = _first_num(_dig(external_body, "category_countertext")) or 0

    flat["body_is_clear"] = False
    if isinstance(body_sub, list) and body_sub:
        first_item = body_sub[0]
        if isinstance(first_item, dict):
            flat["body_is_clear"] = first_item.get("body_is_clear") == 1

    body_report = _dig(ins_data, "body_report") or []
    body_damages: List[str] = []
    if isinstance(body_report, list):
        for item in body_report:
            if isinstance(item, dict) and item.get("image_info"):
                img_info = item["image_info"]
                if isinstance(img_info, dict):
                    note = img_info.get("note", "")
                    if isinstance(note, str) and note.strip():
                        body_damages.append(note.strip())
    # flat["body_damage_notes"] = body_damages

    # ========== IMAGES ==========
    gallery = _dig(det_data, "gallery.images") or []
    images: List[Dict[str, Any]] = []
    featured_url: Optional[str] = None
    if isinstance(gallery, list):
        for img in gallery:
            if not isinstance(img, dict):
                continue
            url = img.get("img_url")
            if isinstance(url, str) and url:
                images.append(url)
                if img.get("is_featured") == 1 and not featured_url:
                    featured_url = url

    seen = set()
    uniq_urls: List[str] = []
    for u in images:
        if u in seen:
            continue
        seen.add(u)
        uniq_urls.append(u)
    flat["images"] = uniq_urls[:30]     # list[str]
    # flat["images_count"] = len(uniq_urls)

    flat["featured_image"] = featured_url or (uniq_urls[0] if uniq_urls else None)

    # flat["has_360_spin"] = bool(_dig(det_data, "gallery.has_spin") or False)
    # flat["has_video"] = bool(_dig(det_data, "gallery.has_video") or False)

    # ========== FEATURES ==========
    options = _dig(det_data, "options.options") or []
    all_features: List[str] = []
    features_by_category: Dict[str, List[str]] = {}

    if isinstance(options, list):
        for category in options:
            if isinstance(category, dict):
                cat_name = category.get("category")
                cat_data = category.get("data", [])
                if isinstance(cat_data, list):
                    feature_names = [f.get("name") for f in cat_data if isinstance(f, dict) and f.get("name")]
                else:
                    feature_names = []
                all_features.extend(feature_names)
                if cat_name:
                    features_by_category[str(cat_name)] = feature_names

    # flat["features"] = all_features
    # flat["features_count"] = len(all_features)
    # flat["features_by_category"] = features_by_category

    # ========== LISTING INFO ==========
    flat["share_link"] = _first_str(_dig(det_data, "details.share_link"))
    # flat["product_url"] = _first_str(_dig(det_data, "details.product_url"))

    # flat["is_sold"] = _dig(det_data, "details.is_sold")
    # flat["is_deleted"] = _dig(det_data, "details.is_deleted")
    # flat["owned_by_us"] = _dig(det_data, "details.owned_by_us")

    # flat["list_date"] = _first_str(_dig(det_data, "g4Data.list_date"))
    # flat["lot_age_days"] = _first_num(_dig(det_data, "g4Data.lot_age"))

    # ========== CAMPAIGN/OFFERS ==========
    campaigns = _dig(det_data, "details.campaigns") or {}
    # flat["has_cash_campaign"] = bool(_dig(campaigns, "cash"))
    # flat["has_finance_campaign"] = bool(_dig(campaigns, "finance"))
    cash_campaign = _dig(campaigns, "cash") or {}
    # flat["campaign_text"] = _first_str(_dig(cash_campaign, "text"))

    # ========== TAGS ==========
    tags = _dig(det_data, "details.tags") or []
    if isinstance(tags, list):
        flat["tags"] = [t.get("tag_name") for t in tags if isinstance(t, dict) and t.get("tag_name")]
    else:
        flat["tags"] = []

    return flat



def fetch_post_payloads_requests(sess: requests.Session, lang: str, post_id: int) -> Dict[str, Any]:
    u1, u2 = build_api_urls(lang, post_id)
    referer = f"https://syarah.com/{lang}/cardetail/used-{post_id}"

    r1 = _req_get_json_or_text(sess, u1, referer=referer)
    r2 = _req_get_json_or_text(sess, u2, referer=referer)

    inspection_json = (r1.get("json") if isinstance(r1, dict) else None) or {}
    details_json = (r2.get("json") if isinstance(r2, dict) else None) or {}

    flat = flatten_post(inspection_json, details_json)

    from datetime import datetime, timezone

    return {
        "id": int(post_id),
        "fetchedAt": datetime.now(timezone.utc).isoformat(),

        # ✅ tiny debug/status fields (VERY small)
        "inspection_status": int(r1.get("status") or 0),
        "details_status": int(r2.get("status") or 0),

        # ✅ flat fields
        **flat,

        # ❌ NO api stored
        # "api": ...
    }
