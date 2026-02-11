# Syarah Filters Scraper (Python + nodriver) -> MongoDB

Scrapes listings from `https://syarah.com/filters`, loads cards as you scroll, and for each post ID calls the two JSON endpoints inside the browser context (so cookies/credentials are used):

1. `.../post/view-online?id=...&include=inspection`
2. `.../post/view-online?id=...&should_redirect=1&include=details,price,story,...`

Stores results in MongoDB and skips duplicates (unique index on `id`).

## Why browser-context fetch?
If you paste the API URL directly you may get `401 Unauthorized`. When you run `fetch()` from inside a logged-in/initialized browser page, the site cookies + any required headers are included, matching what you see in DevTools.

## Setup

1. Create `.env` from `.env.example`
2. Install deps:
   ```bash
   python -m venv .venv
   # Windows
   .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Run:
   ```bash
   python -m src.main
   ```

## Environment variables

- `MONGO_URL` (required)
- `MONGO_DB` (default: `projectForever`)
- `MONGO_COLLECTION` (default: `syarah_posts`)
- `TARGET_URL` (default: `https://syarah.com/filters`)
- `HEADLESS` (default: `false`)
- `CHECK_INTERVAL_HOURS` (default: `48`)
- `MAX_SCROLLS` (default: `10000`)
- `SCROLL_PAUSE_SEC` (default: `1.2`)
- `BATCH_SIZE` (default: `16`)

## Notes / tuning

- The scraper uses **id-based de-dupe** in MongoDB, so reruns only store new items.
- If the API fetch still returns 401, you likely need extra headers (e.g., `x-something`) that the site adds.
  In that case, capture the request headers from DevTools for the API call and share them; the code has a place
  (`EXTRA_API_HEADERS_JSON`) to inject them.

