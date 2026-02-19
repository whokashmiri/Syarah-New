# main.py
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
    js_scroll_into_view,
    JS_SCROLL_STEP,
    build_api_session,
    fetch_post_payloads_requests,
    try_click_load_more,
)


# -----------------------------
# Windows maximize helpers
# -----------------------------
def _get_browser_pid(browser: Any) -> Optional[int]:
    for attr in ("pid", "_pid", "process_pid"):
        v = getattr(browser, attr, None)
        if isinstance(v, int) and v > 0:
            return v

    for attr in ("process", "_process", "proc", "_proc"):
        p = getattr(browser, attr, None)
        if p is None:
            continue
        v = getattr(p, "pid", None)
        if isinstance(v, int) and v > 0:
            return v

    return None


def _win_restore_maximize_by_pid(pid: int) -> bool:
    try:
        import win32gui
        import win32con
        import win32process

        handles = []

        def enum_callback(hwnd, _):
            if not win32gui.IsWindowVisible(hwnd):
                return
            _, win_pid = win32process.GetWindowThreadProcessId(hwnd)
            if win_pid != pid:
                return
            title = win32gui.GetWindowText(hwnd)
            if title.strip():
                handles.append(hwnd)

        win32gui.EnumWindows(enum_callback, None)

        if not handles:
            return False

        hwnd = handles[0]
        win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
        win32gui.ShowWindow(hwnd, win32con.SW_MAXIMIZE)

        try:
            win32gui.SetForegroundWindow(hwnd)
        except Exception:
            pass

        return True
    except Exception:
        return False


# -----------------------------
# Stable "make visible" (NO viewport resize here)
# -----------------------------
async def ensure_window_visible(page: Any, browser: Any | None = None) -> None:
    # 1) Restore/maximize at OS level (Windows)
    try:
        if browser is not None and os.name == "nt":
            pid = _get_browser_pid(browser)
            if pid:
                ok = _win_restore_maximize_by_pid(pid)
                log(f"[win] restore/maximize pid={pid} ok={ok}")
                await asyncio.sleep(0.2)
    except Exception as e:
        log(f"[win] restore/maximize error: {e}")

    # 2) Bring tab front (best effort)
    try:
        if hasattr(page, "bring_to_front"):
            await page.bring_to_front()
        elif hasattr(page, "bringToFront"):
            await page.bringToFront()
    except Exception:
        pass

    # 3) Focus via JS
    try:
        await page.evaluate(
            """
            (() => {
              try { window.focus(); } catch(e) {}
              try { document.documentElement && document.documentElement.focus && document.documentElement.focus(); } catch(e) {}
              try { document.body && document.body.focus && document.body.focus(); } catch(e) {}
              return true;
            })()
            """
        )
    except Exception:
        pass

    # settle
    try:
        await page.sleep(0.15)
    except Exception:
        pass


async def set_viewport_once(page: Any) -> None:
    """
    Set viewport ONCE at start. Do NOT keep changing it or you'll break the scroll container.
    """
    vp = {"width": 1600, "height": 900}
    try:
        if hasattr(page, "set_viewport"):
            await page.set_viewport(vp)
        elif hasattr(page, "setViewport"):
            await page.setViewport(vp)
    except Exception:
        pass


async def apply_zoom_90(page: Any) -> None:
    """
    Force zoom = 90% so you get 4 cards per row.
    Re-apply after refresh because some apps reset styles.
    """
    try:
        await page.evaluate(
            """
            (() => {
              try {
                document.documentElement.style.zoom = "90%";
                document.body.style.zoom = "90%";
              } catch(e) {}
              return true;
            })()
            """
        )
    except Exception:
        pass
    await page.sleep(0.1)


async def _get_current_url(page: Any, fallback: str = "") -> str:
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


async def _get_scroll_y(page: Any) -> int:
    try:
        v = unwrap_remote(await page.evaluate("Number(window.scrollY || 0)"))
        return int(v or 0)
    except Exception:
        return 0


async def _restore_scroll_y(page: Any, y: int) -> None:
    if y <= 0:
        return
    try:
        await page.evaluate(f"window.scrollTo(0, {int(y)});")
    except Exception:
        pass
    await page.sleep(0.4)


async def _refresh_current_url(page: Any, current_url: str, restore_y: int) -> None:
    log(f"[recover] refresh current url: {current_url} (restore_y={restore_y})")
    try:
        await page.reload()
    except Exception:
        await page.get(current_url)

    await wait_for_listing_ready(page)
    await page.sleep(0.5)

    # re-apply zoom after refresh
    await apply_zoom_90(page)

    # restore previous y
    await _restore_scroll_y(page, restore_y)


async def _try_open_new_tab(browser: Any, url: str) -> Optional[Any]:
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
    return {"beforeY": 0, "afterY": 0, "h": 0, "mode": "none"}


def _details_status(payload: dict) -> Optional[int]:
    st = payload.get("details_status")
    if isinstance(st, int):
        return st
    try:
        return ((((payload.get("api") or {}).get("details") or {}).get("res") or {}).get("status"))
    except Exception:
        return None


async def scrape_once(browser: Any, settings) -> None:
    log(f"[syarah] Opening: {settings.target_url}")
    page = await browser.get(settings.target_url)

    await ensure_window_visible(page, browser)
    await set_viewport_once(page)
    await wait_for_listing_ready(page)
    await apply_zoom_90(page)

    total = await read_total_ads(page)
    log(f"[syarah] Total ads (from header): {total}")

    col = get_collection(settings.mongo_url, settings.mongo_db, settings.mongo_collection)
    api_sess = build_api_session(settings)

    processed_ids: set[int] = set()
    inserted = updated = skipped = processed = 0
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

        # No cards case
        if not visible_cards:
            empty_visible_rounds += 1
            log(f"[batch {batch_no}] visible=0 (round={empty_visible_rounds}) -> waiting")

            if total and len(processed_ids) < int(total) and empty_visible_rounds >= 8:
                cur_url = await _get_current_url(page, fallback=settings.target_url)
                restore_y = await _get_scroll_y(page)
                await ensure_window_visible(page, browser)
                await _refresh_current_url(page, cur_url, restore_y)
                empty_visible_rounds = 0
                continue

            await page.sleep(1.0)
            if empty_visible_rounds >= 20:
                log("[stop] no cards detected after many retries; exiting this run")
                break
            continue

        empty_visible_rounds = 0

        unprocessed = [c for c in visible_cards if int(c["id"]) not in processed_ids]

        log(
            f"[batch {batch_no}] visible={len(visible_cards)} new_unprocessed={len(unprocessed)} "
            f"processed={processed} inserted={inserted} updated={updated} skipped={skipped}"
        )

        # If nothing new -> scroll
        if not unprocessed:
            await ensure_window_visible(page, browser)

            info = _scroll_info(await page.evaluate(JS_SCROLL_STEP))
            log(
                f"[scroll_debug] mode={info.get('mode')} y:{info.get('beforeY')}->{info.get('afterY')} "
                f"h={info.get('h')} clientH={info.get('clientH')}"
            )

            after_y = info.get("afterY")

            await page.sleep(settings.scroll_pause_sec)

            if total and len(processed_ids) >= int(total):
                break

            progressed_scroll = (after_y is not None and after_y != last_scroll_after)
            progressed_ids = (len(processed_ids) != last_seen_unique)

            if progressed_scroll or progressed_ids:
                stuck_rounds = 0
            else:
                stuck_rounds += 1

            last_scroll_after = after_y
            last_seen_unique = len(processed_ids)

            # When stuck: try load more, then refresh preserving y
            if stuck_rounds >= 3:
                await ensure_window_visible(page, browser)
                clicked = await try_click_load_more(page)
                if clicked:
                    stuck_rounds = 0
                    continue

            if total and len(processed_ids) < int(total) and stuck_rounds >= 8:
                cur_url = await _get_current_url(page, fallback=settings.target_url)
                restore_y = await _get_scroll_y(page)
                await ensure_window_visible(page, browser)
                await _refresh_current_url(page, cur_url, restore_y)
                stuck_rounds = 0

            continue

        # Process new items
        stuck_rounds = 0
        chunk = unprocessed[:16]

        for idx, c in enumerate(chunk, start=1):
            pid = int(c["id"])
            href = str(c.get("href") or "")
            url = abs_url(href)

            processed_ids.add(pid)
            processed += 1

            if already_have(col, pid):
                skipped += 1
                continue

            tab = None
            if url:
                tab = await _try_open_new_tab(browser, url)

            payload = fetch_post_payloads_requests(api_sess, settings.api_lang, pid)

            if tab:
                try:
                    await tab.close()
                except Exception:
                    pass

            st = _details_status(payload)
            if st == 401:
                unauthorized_hits += 1
                log(f"[auth] 401 for id={pid} (count={unauthorized_hits}). Check .env headers/cookie")

            if st in (None, 0, 401):
                log(f"[api] skip store id={pid} status={st}")
                continue

            result = upsert_post(col, payload)
            if result == "inserted":
                inserted += 1
            elif result == "updated":
                updated += 1
            else:
                skipped += 1

            # ✅ after each 4 cards, nudge to next row (stable)
            if idx % 4 == 0:
                await ensure_window_visible(page, browser)
                try:
                    await page.evaluate(js_scroll_into_view(pid))
                except Exception:
                    pass
                await page.sleep(0.25)

        # After processing the 16, do a normal scroll step once
        await ensure_window_visible(page, browser)
        info = _scroll_info(await page.evaluate(JS_SCROLL_STEP))
        log(
            f"[scroll] (after processing {len(chunk)}) "
            f"mode={info.get('mode')} y:{info.get('beforeY')}->{info.get('afterY')} h={info.get('h')}"
        )
        await page.sleep(settings.scroll_pause_sec)

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
