from __future__ import annotations

import asyncio
import json
from typing import Any, Optional

import nodriver as uc

from .config import get_settings
from .logging_utils import log
from .mongo import get_collection, already_have, upsert_post

from .syarah import (
    unwrap_remote,
    wait_for_listing_ready,
    read_total_ads,
    read_visible_cards,
    abs_url,
    JS_SCROLL_STEP,
    build_api_session,
    fetch_post_payloads_requests,
)


async def _get_current_url(page: Any, fallback: str = "") -> str:
    """
    nodriver pages sometimes don’t expose .url reliably, so use location.href.
    """
    try:
        u = unwrap_remote(await page.evaluate("location.href"))
        if isinstance(u, str) and u.strip():
            return u.strip()
    except Exception:
        pass

    try:
        u = getattr(page, "url", "")
        if isinstance(u, str) and u.strip():
            return u.strip()
    except Exception:
        pass

    return fallback


async def _refresh_current_url(page: Any, current_url: str) -> None:
    """
    Refresh the CURRENT URL (not the base target url), then wait for listing ready.
    """
    log(f"[recover] refresh current url: {current_url}")
    try:
        await page.reload()
    except Exception:
        await page.get(current_url)

    await wait_for_listing_ready(page)
    await page.sleep(1.0)


async def _try_open_new_tab(browser: Any, url: str) -> Optional[Any]:
    """
    Best-effort only. Some nodriver versions don't support new tabs consistently.
    Scraping DOES NOT depend on tabs; API is fetched via requests session.
    """
    try:
        if hasattr(browser, "new_tab"):
            return await browser.new_tab(url)
    except Exception:
        pass

    try:
        return await browser.get(url, new_tab=True)  # type: ignore
    except Exception:
        return None


def _scroll_info(val: Any) -> dict:
    val = unwrap_remote(val)
    if isinstance(val, dict):
        return val
    if isinstance(val, list) and val and isinstance(val[0], dict):
        return val[0]
    return {}


def _details_status(payload: dict) -> Optional[int]:
    # prefer direct saved code
    st = payload.get("details_status")
    if isinstance(st, int):
        return st
    # fallback old shape
    try:
        return ((((payload.get("api") or {}).get("details") or {}).get("res") or {}).get("status"))
    except Exception:
        return None


async def scrape_once(browser: Any, settings) -> None:
    log(f"[syarah] Opening: {settings.target_url}")
    page = await browser.get(settings.target_url)

    await wait_for_listing_ready(page)

    total = await read_total_ads(page)
    log(f"[syarah] Total ads (from header): {total}")

    col = get_collection(settings.mongo_url, settings.mongo_db, settings.mongo_collection)

    # ✅ Build ONE requests session for the whole run (uses headers/cookies from .env)
    api_sess = build_api_session(settings)

    processed_ids: set[int] = set()
    inserted = 0
    updated = 0
    skipped = 0
    processed = 0
    unauthorized_hits = 0
    batch_no = 0

    empty_visible_rounds = 0
    stuck_rounds = 0
    last_seen_unique = 0
    last_scroll_after = None

    while True:
        batch_no += 1
        visible_cards = await read_visible_cards(page)

        if batch_no == 1:
            log(f"[debug] first batch sample: {json.dumps(visible_cards[:3], ensure_ascii=False)}")

        # -------------------------
        # If nothing visible, wait a bit; if still not done and repeated, refresh current URL
        # -------------------------
        if not visible_cards:
            empty_visible_rounds += 1
            log(f"[batch {batch_no}] visible=0 (round={empty_visible_rounds}) -> waiting")

            if total and len(processed_ids) < int(total) and empty_visible_rounds >= 8:
                cur_url = await _get_current_url(page, fallback=settings.target_url)
                await _refresh_current_url(page, cur_url)
                empty_visible_rounds = 0
                continue

            await page.sleep(1.2)

            if empty_visible_rounds >= 20:
                log("[stop] no cards detected after many retries; exiting this run")
                break
            continue

        empty_visible_rounds = 0

        # compute unprocessed AFTER we have visible cards
        unprocessed = [c for c in visible_cards if int(c["id"]) not in processed_ids]

        log(
            f"[batch {batch_no}] visible={len(visible_cards)} new_unprocessed={len(unprocessed)} "
            f"processed={processed} inserted={inserted} updated={updated} skipped={skipped}"
        )

        # -------------------------
        # If no new cards in view, try scrolling.
        # If scrolling stalls AND we're not done, refresh current URL.
        # -------------------------
        if not unprocessed:
            info = _scroll_info(await page.evaluate(JS_SCROLL_STEP))
            after_y = info.get("afterY")

            log(f"[scroll] (no new) y:{info.get('beforeY')}->{after_y} h={info.get('h')}")
            await page.sleep(settings.scroll_pause_sec)

            # ✅ stop only when all ads scraped
            if total and len(processed_ids) >= int(total):
                break

            # "stuck" = afterY not changing AND processed_unique not increasing
            progressed_scroll = (after_y is not None and after_y != last_scroll_after)
            progressed_ids = (len(processed_ids) != last_seen_unique)

            if progressed_scroll or progressed_ids:
                stuck_rounds = 0
            else:
                stuck_rounds += 1

            last_scroll_after = after_y
            last_seen_unique = len(processed_ids)

            # refresh threshold
            if total and len(processed_ids) < int(total) and stuck_rounds >= 8:
                cur_url = await _get_current_url(page, fallback=settings.target_url)
                await _refresh_current_url(page, cur_url)
                stuck_rounds = 0

            continue

        # ✅ we have new cards to process; reset stall counter
        stuck_rounds = 0

        # ✅ Process max 16 per view
        chunk = unprocessed[:16]

        for c in chunk:
            pid = int(c["id"])
            href = str(c.get("href") or "")
            url = abs_url(href)

            processed_ids.add(pid)
            processed += 1

            # already_have() now returns True only if doc is "good"
            if already_have(col, pid):
                log(
                    f"[db] skip good existing id={pid} | "
                    f"processed={processed} inserted={inserted} updated={updated}"
                )
                skipped += 1
                continue

            # Optional: open tab for realism (not required for API)
            tab = None
            if url:
                tab = await _try_open_new_tab(browser, url)
                if tab:
                    log(f"[tab] opened id={pid}")
                else:
                    log(f"[tab] open failed (continuing) id={pid}")

            # ✅ Fetch via requests (DevTools headers)
            payload = fetch_post_payloads_requests(api_sess, settings.api_lang, pid)

            if tab:
                try:
                    await tab.close()
                    log(f"[tab] closed id={pid}")
                except Exception as e:
                    log(f"[tab] close error id={pid}: {e}")

            st = _details_status(payload)
            if st == 401:
                unauthorized_hits += 1
                log(f"[auth] 401 for id={pid} (count={unauthorized_hits}). Check Bearer/token/cookie in .env")

            # ✅ Avoid polluting DB with empty results if unauthorized/failed
            if st in (None, 0, 401):
                log(f"[api] skip store id={pid} status={st}")
                continue

            result = upsert_post(col, payload)  # returns inserted/updated/skipped
            if result == "inserted":
                inserted += 1
                log(f"[db] inserted id={pid} | inserted={inserted} updated={updated} processed={processed}")
            elif result == "updated":
                updated += 1
                log(f"[db] updated id={pid} | inserted={inserted} updated={updated} processed={processed}")
            else:
                skipped += 1
                log(f"[db] skipped id={pid} | inserted={inserted} updated={updated} processed={processed}")

        # ✅ Scroll only after processing this chunk
        if len(chunk) >= 16 or (len(chunk) == len(unprocessed)):
            info = _scroll_info(await page.evaluate(JS_SCROLL_STEP))
            log(
                f"[scroll] (after processing {len(chunk)}) "
                f"y:{info.get('beforeY')}->{info.get('afterY')} h={info.get('h')}"
            )
            await page.sleep(settings.scroll_pause_sec)
        else:
            log(f"[hold] still have unprocessed visible cards ({len(unprocessed) - len(chunk)}) -> not scrolling yet")
            await page.sleep(0.4)

        if total and len(processed_ids) >= int(total):
            log(f"[syarah] reached header total (processed_unique={len(processed_ids)} >= {total})")
            break

    log(
        f"[syarah] scrape_once done | total_header={total} "
        f"processed_unique={len(processed_ids)} processed={processed} "
        f"inserted={inserted} updated={updated} skipped={skipped} 401s={unauthorized_hits}"
    )


async def main() -> None:
    settings = get_settings()

    log(f"[boot] Starting browser | headless={settings.headless}")
    browser = await uc.start(headless=settings.headless)
    log("[boot] Browser started")

    while True:
        try:
            await scrape_once(browser, settings)
        except Exception as e:
            log(f"[error] scrape_once failed: {e}")

        log(f"[sleep] Waiting {settings.check_interval_hours} hours before checking again...")
        await asyncio.sleep(settings.check_interval_hours * 3600)


if __name__ == "__main__":
    asyncio.run(main())
