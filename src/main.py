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
    # JS_SCROLL_STEP,
    build_api_session,
    fetch_post_payloads_requests,
    try_click_load_more,
)


from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

def set_query_param(url: str, key: str, value: str) -> str:
    u = urlparse(url)
    q = parse_qs(u.query)
    q[key] = [value]
    new_q = urlencode(q, doseq=True)
    return urlunparse((u.scheme, u.netloc, u.path, new_q, u.fragment))



from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

def force_page_url(base_filters_url: str, page_no: int) -> str:
    """
    Safely set ?page=N without breaking other params like condition_id=0,1
    """
    parts = urlsplit(base_filters_url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))

    q["page"] = str(int(page_no))

    new_query = urlencode(q, doseq=True)  # keeps commas safe as part of value
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


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
_last_win_action = 0.0

def _win_is_minimized(hwnd: int) -> bool:
    try:
        import win32gui
        return bool(win32gui.IsIconic(hwnd))
    except Exception:
        return False


def _win_restore_maximize_by_pid_if_minimized(pid: int) -> bool:
    """
    Restore+maximize only if the window is minimized (IsIconic).
    """
    try:
        import win32gui, win32con, win32process

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

        if not win32gui.IsIconic(hwnd):
            return False  # not minimized -> do nothing

        win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
        win32gui.ShowWindow(hwnd, win32con.SW_MAXIMIZE)

        try:
            win32gui.SetForegroundWindow(hwnd)
        except Exception:
            pass

        return True
    except Exception:
        return False


async def ensure_window_visible(page: Any, browser: Any | None = None) -> None:
    """
    Only restore/maximize if minimized, and debounce it.
    """
    global _last_win_action

    # 1) OS-level restore/maximize ONLY if minimized (Windows)
    try:
        now = asyncio.get_event_loop().time()
        if browser is not None and os.name == "nt" and (now - _last_win_action) > 2.0:
            pid = _get_browser_pid(browser)
            if pid:
                ok = _win_restore_maximize_by_pid_if_minimized(pid)
                if ok:
                    _last_win_action = now
                    log(f"[win] restored/maximized (was minimized) pid={pid}")
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
        await page.evaluate("(()=>{ try{window.focus()}catch(e){} return true; })()")
    except Exception:
        pass

    await page.sleep(0.05)




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


async def apply_layout_4cols(page: Any, *, zoom90_fallback: bool = False) -> None:
    """
    Force 4-column listing grid in a SPA-safe way.
    - Injects CSS once
    - Adds MutationObserver to re-apply if DOM re-renders
    - Optionally applies zoom 90% fallback (disabled by default)
    """
    try:
        await page.evaluate(
            """
(() => {
  const STYLE_ID = "__force_4cols_css";
  const OBS_ID = "__force_4cols_observer";

  // 1) CSS injection (persistent)
  let style = document.getElementById(STYLE_ID);
  if (!style) {
    style = document.createElement("style");
    style.id = STYLE_ID;
    document.head.appendChild(style);
  }

  style.textContent = `
    /* Make the result container a 4-col grid */
    ${"div.UnbxdCards-module__allCarsResult"} {
      display: grid !important;
      grid-template-columns: repeat(4, minmax(0, 1fr)) !important;
      gap: 12px !important;
      align-items: stretch !important;
    }

    /* Cards must not have fixed widths */
    ${"div.UnbxdCards-module__allCarsResult"} > div[id^="modern-card_post-"] {
      width: auto !important;
      max-width: none !important;
      min-width: 0 !important;
    }

    /* Some builds wrap card content; make wrappers fluid too */
    div[id^="modern-card_post-"] * {
      max-width: 100% !important;
      box-sizing: border-box !important;
    }

    /* Prevent horizontal overflow causing weird “shrink” behavior */
    html, body {
      overflow-x: hidden !important;
    }
  `;

  // 2) A small "apply" function to nudge layout (helps after rerenders)
  function applyNow() {
    const container = document.querySelector("div.UnbxdCards-module__allCarsResult");
    if (!container) return false;

    // Force reflow
    container.style.transform = "translateZ(0)";
    container.offsetHeight; // reflow
    container.style.transform = "";

    return true;
  }

  applyNow();

  // 3) MutationObserver: if SPA replaces container, re-apply
  if (!window[OBS_ID]) {
    const obs = new MutationObserver(() => {
      // If container exists but CSS didn't apply yet or got reset, re-apply
      applyNow();
    });

    obs.observe(document.documentElement, { childList: true, subtree: true });
    window[OBS_ID] = obs;
  }

  return true;
})()
"""
        )
    except Exception:
        pass

    # 4) OPTIONAL zoom fallback (I suggest OFF unless you really need it)
    if zoom90_fallback:
        try:
            await page.evaluate(
                """
(() => {
  try { document.documentElement.style.zoom = "90%"; } catch(e) {}
  try { document.body.style.zoom = "90%"; } catch(e) {}
  return true;
})()
"""
            )
        except Exception:
            pass

    try:
        await page.sleep(0.05)
    except Exception:
        pass

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
    await apply_layout_4cols(page)

    # restore previous y
    await _restore_scroll_y(page, restore_y)


async def open_ad_in_real_new_tab(browser: Any, listing_page: Any, url: str) -> Optional[Any]:
    """
    Open a REAL new tab using window.open() from the listing page.
    Returns the newest tab if nodriver exposes browser.tabs.
    """
    if not url:
        return None

    try:
        await listing_page.evaluate(f'window.open({json.dumps(url)}, "_blank");')
        await listing_page.sleep(0.6)
    except Exception:
        return None

    # nodriver versions differ; try best-effort
    tabs = getattr(browser, "tabs", None)
    if isinstance(tabs, list) and len(tabs) >= 2:
        return tabs[-1]

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


async def goto_page(page: Any, browser: Any, base_url: str, page_no: int) -> None:
    url = force_page_url(base_url, page_no)
    log(f"[nav] goto page={page_no} url={url}")

    try:
        await page.get(url)
    except Exception:
        # fallback: open again
        page = await browser.get(url)

    await ensure_window_visible(page, browser)
    await wait_for_listing_ready(page)
    await apply_layout_4cols(page)
    await page.sleep(0.4)


async def scrape_once(browser: Any, settings) -> None:
    base_url = settings.target_url
    page_no = 1

    col = get_collection(settings.mongo_url, settings.mongo_db, settings.mongo_collection)
    api_sess = build_api_session(settings)

    inserted = updated = skipped = processed = 0
    unauthorized_hits = 0

    batch_size = int(getattr(settings, "batch_size", 16) or 16)

    while True:
        page_url = force_page_url(base_url, page_no)
        log(f"[nav] goto page={page_no} url={page_url}")

        page = await browser.get(page_url)

        await ensure_window_visible(page, browser)
        await set_viewport_once(page)
        await wait_for_listing_ready(page)
        await apply_layout_4cols(page)

        cards = await read_visible_cards(page)
        if not cards:
            log(f"[stop] no cards found on page={page_no}")
            break

        chunk = cards[:batch_size]
        log(f"[page {page_no}] scraping {len(chunk)} ads")

        for c in chunk:
            pid = int(c["id"])
            href = str(c.get("href") or "")
            url = abs_url(href)

            processed += 1

            # 1) If already in DB, skip and LOG the id
            if already_have(col, pid):
                skipped += 1
                log(f"[db] SKIP_EXISTING id={pid} page={page_no} processed={processed}")
                continue

            # 2) OPTIONAL: open ad in a REAL new tab (for realism)
            tab = None
            if url:
                tab = await open_ad_in_real_new_tab(browser, page, url)
                if tab:
                    log(f"[tab] OPEN id={pid} page={page_no}")
                else:
                    log(f"[tab] OPEN_FAILED id={pid} page={page_no}")

            # 3) Fetch via API
            payload = fetch_post_payloads_requests(api_sess, settings.api_lang, pid)
            st = _details_status(payload)

            # close tab if opened
            if tab:
                try:
                    await tab.close()
                    log(f"[tab] CLOSE id={pid}")
                except Exception:
                    pass

            # 4) handle bad/unauthorized
            if st == 401:
                unauthorized_hits += 1
                log(f"[api] 401 id={pid} page={page_no} (401s={unauthorized_hits})")
                continue

            if st in (None, 0):
                log(f"[api] BAD_STATUS id={pid} page={page_no} status={st}")
                continue

            # 5) Save to DB and LOG the id
            result = upsert_post(col, payload)

            if result == "inserted":
                inserted += 1
                log(f"[db] INSERT id={pid} page={page_no} inserted={inserted} processed={processed}")
            elif result == "updated":
                updated += 1
                log(f"[db] UPDATE id={pid} page={page_no} updated={updated} processed={processed}")
            else:
                skipped += 1
                log(f"[db] SKIP_UPSERT id={pid} page={page_no} skipped={skipped} processed={processed}")

        # next page
        page_no += 1
        await asyncio.sleep(float(settings.scroll_pause_sec))

    log(
        f"[done] processed={processed} inserted={inserted} updated={updated} "
        f"skipped={skipped} 401s={unauthorized_hits}"
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
