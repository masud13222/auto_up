# Pydoll Browser Tab System — Implementation Notes

## Current State (as of 2026-03-25)

Every `_fetch_html()` call in `web_scrape.py` opens a **new Chrome process** and closes it:

```python
# CURRENT — wasteful
async def _fetch_html(url):
    async with Chrome(options=_chrome_options()) as browser:   # new process each time
        tab = await browser.start()
        await tab.go_to(url)
        return await tab.page_source
```

For a single movie pipeline this means:
```
cinefreak_title()    → Chrome #1 open → scrape → CLOSE
get_page_content()   → Chrome #2 open → scrape → CLOSE
get_url() 480p       → Chrome #3 open → resolve → CLOSE
get_url() 720p       → Chrome #4 open → resolve → CLOSE
get_url() 1080p      → Chrome #5 open → resolve → CLOSE
auto_up scraper      → Chrome #6 open → homepage → CLOSE
```

---

## Target Architecture — Pydoll Tab System

Pydoll supports multiple tabs inside **one** browser instance:

```
async with Chrome(options=...) as browser:
    tab1 = await browser.start()          # 1st tab (auto-created)
    tab2 = await browser.new_tab(url)     # 2nd tab
    tab3 = await browser.new_tab(url)     # 3rd tab
    await tab2.close()                    # close individual tab
    tabs = await browser.get_opened_tabs()  # list all open tabs
```

### Pydoll API reference (confirmed from docs)

| Method | Description |
|---|---|
| `browser.start()` | Start browser, returns 1st tab |
| `await browser.new_tab(url="")` | Open new tab (blank or with URL) |
| `await browser.get_opened_tabs()` | List all open tabs |
| `await tab.close()` | Close a single tab (not the browser) |
| `await tab.go_to(url)` | Navigate tab to URL |
| `await tab.page_source` | Get page HTML |
| `await tab.enable_auto_solve_cloudflare_captcha()` | Enable CF bypass on this tab |

---

## Planned Approach — Option 2: Per-Task Browser

One browser per pipeline task. All steps (title + page + url resolves) share the same browser via new tabs.

```python
async def run_pipeline_with_browser(task_url):
    async with Chrome(options=_chrome_options()) as browser:
        # Step 1: title fetch
        tab1 = await browser.start()
        await tab1.enable_auto_solve_cloudflare_captcha()
        await tab1.go_to(task_url)
        title_html = await tab1.page_source
        await tab1.close()

        # Step 2: full page scrape (same browser, new tab)
        tab2 = await browser.new_tab()
        await tab2.go_to(task_url)
        page_html = await tab2.page_source
        await tab2.close()

        # Step 3: resolve download URLs (parallel tabs)
        tab3 = await browser.new_tab()
        tab4 = await browser.new_tab()
        await asyncio.gather(
            tab3.go_to(dl_url_480p),
            tab4.go_to(dl_url_720p),
        )
        # ... get links from each tab
```

### Benefits
- **1 Chrome process** per pipeline task (not 5-6)
- Tabs open/close fast (no OS process overhead)
- Cloudflare cookies/session shared across tabs → fewer re-challenges

---

## Files to Modify

| File | Change needed |
|---|---|
| `upload/utils/web_scrape.py` | Refactor `_fetch_html()` to accept a `browser` or `tab` param |
| `upload/tasks/__init__.py` | Open one `Chrome()` per task, pass browser down |
| `upload/service/info.py` | Accept browser param instead of spawning internally |
| `auto_up/scraper.py` | Already rewritten with pydoll; can share browser with pipeline |

---

## Notes

- Django-Q workers are sync. Use `asyncio.run()` or `_run()` wrapper around the whole async pipeline block.
- Do NOT try to keep a global Chrome singleton across sync Django-Q workers — each worker is a separate process/thread.
- Per-task browser is the cleanest approach: open at task start, close at task end.
- `enable_auto_solve_cloudflare_captcha()` needs to be called on each tab individually (not just browser-level).

---

## Status

- [ ] Refactor `web_scrape.py` to accept shared `browser` param
- [ ] Wrap full pipeline in one `async with Chrome()` block
- [ ] Test tab sharing for download URL resolution (parallel tabs)
- [ ] Update `auto_up/scraper.py` to reuse pipeline browser if called within same context
