from __future__ import annotations

import asyncio
import json
import os
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
    JS_SCROLL_TO_LAST_CARD,
    JS_NUDGE_UP,
    JS_CLICK_LOAD_MORE,
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


async def _bring_to_front(page: Any) -> None:
    """
    Hidden/background tabs don't render, so the site's infinite-scroll
    (IntersectionObserver) never fires there. Make sure the listing tab is active.
    """
    for meth in ("bring_to_front", "activate"):
        fn = getattr(page, meth, None)
        if fn:
            try:
                await fn()
                return
            except Exception:
                pass


async def _wait_for_new_cards(page: Any, before_ids: set, timeout: float) -> bool:
    """Poll until the DOM contains a card id that wasn't there before."""
    end = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < end:
        await page.sleep(0.5)
        cards = await read_visible_cards(page)
        if any(int(c["id"]) not in before_ids for c in cards):
            return True
    return False


async def _load_more_cards(page: Any, settings) -> bool:
    """
    Called ONLY when every card in the DOM is already processed.
    Tries progressively stronger ways to make the site load the next page.
    Returns True as soon as new cards show up in the DOM.
    """
    await _bring_to_front(page)
    before_ids = {int(c["id"]) for c in await read_visible_cards(page)}
    wait = max(4.0, settings.scroll_pause_sec * 4)

    for attempt in range(1, 4):
        if attempt == 2:
            clicked = unwrap_remote(await page.evaluate(JS_CLICK_LOAD_MORE))
            if clicked:
                log("[load-more] clicked 'load more' button")
        elif attempt == 3:
            await page.evaluate(JS_NUDGE_UP)
            await page.sleep(0.8)

        info = _scroll_info(await page.evaluate(JS_SCROLL_TO_LAST_CARD))
        log(
            f"[scroll] to last card (attempt {attempt}) cards={info.get('count')} "
            f"y={info.get('y')} h={info.get('h')} atBottom={info.get('atBottom')}"
        )

        if await _wait_for_new_cards(page, before_ids, wait):
            return True

    return False


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
    failed_load_rounds = 0
    refreshes_without_progress = 0
    ids_at_last_refresh = 0

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
        # No unscraped cards left in the DOM -> ONLY now do we scroll to load more.
        # -------------------------
        if not unprocessed:
            if total and len(processed_ids) >= int(total):
                break

            if await _load_more_cards(page, settings):
                failed_load_rounds = 0
                continue

            failed_load_rounds += 1
            log(f"[load-more] no new cards appeared (fail round={failed_load_rounds})")

            if len(processed_ids) > ids_at_last_refresh:
                # we made progress since the last refresh -> a refresh is worth a try
                refreshes_without_progress = 0
            if failed_load_rounds >= 2:
                if refreshes_without_progress >= 2:
                    log(
                        f"[stop] site stopped loading new ads "
                        f"(processed_unique={len(processed_ids)} / header total={total}); ending run"
                    )
                    break
                cur_url = await _get_current_url(page, fallback=settings.target_url)
                await _refresh_current_url(page, cur_url)
                refreshes_without_progress += 1
                ids_at_last_refresh = len(processed_ids)
                failed_load_rounds = 0
            continue

        failed_load_rounds = 0

        # ✅ Process EVERY unscraped card already in the DOM before scrolling
        chunk = unprocessed

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

        # Tabs opened above can leave the listing tab in the background,
        # where infinite-scroll won't fire. Re-focus it (no scrolling here).
        await _bring_to_front(page)

        if total and len(processed_ids) >= int(total):
            log(f"[syarah] reached header total (processed_unique={len(processed_ids)} >= {total})")
            break

    log(
        f"[syarah] scrape_once done | total_header={total} "
        f"processed_unique={len(processed_ids)} processed={processed} "
        f"inserted={inserted} updated={updated} skipped={skipped} 401s={unauthorized_hits}"
    )


def _find_browser(configured: Optional[str]) -> Optional[str]:
    """
    BROWSER_PATH from .env wins. Otherwise return None if Chrome is installed
    (nodriver finds it itself), else fall back to another Chromium browser.
    """
    if configured:
        if os.path.isfile(configured):
            return configured
        log(f"[boot] BROWSER_PATH not found: {configured} -> auto-detecting")

    pf = os.environ.get("ProgramFiles", r"C:\Program Files")
    pf86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
    local = os.environ.get("LOCALAPPDATA", "")

    chrome = [
        os.path.join(pf, r"Google\Chrome\Application\chrome.exe"),
        os.path.join(pf86, r"Google\Chrome\Application\chrome.exe"),
        os.path.join(local, r"Google\Chrome\Application\chrome.exe"),
    ]
    if any(os.path.isfile(p) for p in chrome):
        return None

    fallbacks = [
        os.path.join(pf, r"BraveSoftware\Brave-Browser\Application\brave.exe"),
        os.path.join(local, r"BraveSoftware\Brave-Browser\Application\brave.exe"),
        os.path.join(pf86, r"Microsoft\Edge\Application\msedge.exe"),
        os.path.join(pf, r"Microsoft\Edge\Application\msedge.exe"),
    ]
    for p in fallbacks:
        if os.path.isfile(p):
            return p
    return None


async def main() -> None:
    settings = get_settings()

    browser_path = _find_browser(settings.browser_path)
    log(f"[boot] Starting browser | headless={settings.headless} | path={browser_path or 'nodriver default'}")
    if browser_path:
        browser = await uc.start(headless=settings.headless, browser_executable_path=browser_path)
    else:
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
