# Pydoll Browser Tab System — Implementation Notes

## Current Problem (as of 2026-03-25)

Every `_fetch_html()` call opens a **new Chrome process** and closes it:
```
cinefreak_title()    → Chrome open → CF solve → scrape → CLOSE (cookies gone)
get_page_content()   → Chrome open → CF solve → scrape → CLOSE (cookies gone)
get_url() 480p       → Chrome open → CF solve → resolve → CLOSE
get_url() 720p       → Chrome open → CF solve → resolve → CLOSE
get_url() 1080p      → Chrome open → CF solve → resolve → CLOSE
```
**CF challenge প্রতিটা call এ আলাদা।** Startup ~2-3s × 5 = **~15 sec waste per pipeline.**

Root cause: `_run()` প্রতিটা call এ নতুন event loop তৈরি ও বন্ধ করে → browser reuse সম্ভব না।

---

## 3টি Approach

---

### ⚡ Approach 1: Shared user-data-dir (Quick Fix, 5 min)

Chrome কে একটা fixed profile directory দাও। Browser বন্ধ হলেও **cookies disk এ থাকবে।**
CF clearance cookie পরের session এও কাজ করবে।

```python
def _chrome_options():
    opts = ChromiumOptions()
    opts.add_argument("--user-data-dir=/app/chrome_profile")  # ← এটা add করো
    # ... বাকি options
```

**✅ Pros:**
- ৫ মিনিটে implement
- CF cookie disk এ থাকে → পরের pipeline এও কাজ করে
- কোনো architecture change নেই

**❌ Cons:**
- একসাথে দুটো Chrome same profile use করতে পারে না → concurrent tasks conflict করে
- CF cookie expire হলে আবার solve করতে হবে (~2hr)
- Docker container restart এ profile মুছে যায় (volume mount করলে ঠিক হয়)

**উপযুক্ত যদি:** tasks সবসময় sequential হয়, concurrent না।

---

### ✅ Approach 2: Per-Task Async Pipeline (Recommended)

পুরো pipeline একটা `async with Chrome()` block এ wrap করো।
সব steps (title, page, url resolve) **একই browser এ নতুন tab** দিয়ে।

```python
# upload/utils/web_scrape.py এ নতুন function
async def fetch_html_in_tab(browser, url: str, settle: float = 2.0) -> str:
    """Use existing browser — open new tab, get HTML, close tab."""
    tab = await browser.new_tab()
    try:
        await tab.enable_auto_solve_cloudflare_captcha()
        await asyncio.wait_for(tab.go_to(url), timeout=30)
        await asyncio.sleep(settle)
        return await tab.page_source
    finally:
        await tab.close()

# upload/tasks/__init__.py / pipeline এ
async def run_full_pipeline_async(task_url):
    async with Chrome(options=_chrome_options()) as browser:
        first_tab = await browser.start()
        await first_tab.enable_auto_solve_cloudflare_captcha()
        await first_tab.close()

        # সব sub-calls এ browser pass করো
        title   = await scrape_title(browser, task_url)
        content = await scrape_page(browser, task_url)
        links   = await resolve_links(browser, dl_urls)   # parallel tabs possible

def process_media_task(task):
    _run(run_full_pipeline_async(task.url))   # একটাই _run() call
```

```
Result:
Chrome open (একবার)
  CF solve (একবার)
  tab → title
  tab → full page
  tab → 480p resolve  ← same session, no CF again
  tab → 720p resolve
  tab → 1080p resolve
Chrome close (একবার)
Total time: ~8-10 sec (vs ~30-40 sec আগে)
```

**✅ Pros:**
- CF একবারই solve হয়
- Startup overhead একবার
- Parallel tab download resolve সম্ভব (`asyncio.gather`)
- Cleanest architecture

**❌ Cons:**
- Moderate refactoring (সব web_scrape functions কে `browser` param নিতে হবে)
- `info.py`, `movie_pipeline.py`, `tvshow_pipeline.py` সব update করতে হবে

**উপযুক্ত:** Long-term best solution।

---

### 🔄 Approach 3: Per-Worker Browser Singleton (Advanced)

Django-Q worker process চালু হওয়ার সময় একটা browser start। Worker যতক্ষণ বাঁচে browser বাঁচে।
নতুন request এলে নতুন tab খোলো, শেষে tab close।

```python
# এর জন্য একটা persistent event loop দরকার:
import threading
import asyncio

_worker_loop: asyncio.AbstractEventLoop | None = None
_worker_browser = None

def _get_worker_loop():
    global _worker_loop
    if _worker_loop is None or _worker_loop.is_closed():
        _worker_loop = asyncio.new_event_loop()
        t = threading.Thread(target=_worker_loop.run_forever, daemon=True)
        t.start()
    return _worker_loop

def _run_in_worker(coro):
    loop = _get_worker_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=120)

async def _get_or_start_browser():
    global _worker_browser
    if _worker_browser is None:
        from pydoll.browser.chromium import Chrome
        _worker_browser = Chrome(options=_chrome_options())
        await _worker_browser.__aenter__()
        first = await _worker_browser.start()
        await first.enable_auto_solve_cloudflare_captcha()
        await first.close()
    return _worker_browser

async def _fetch_html_singleton(url, settle=2.0):
    browser = await _get_or_start_browser()
    tab = await browser.new_tab()
    try:
        await tab.enable_auto_solve_cloudflare_captcha()
        await asyncio.wait_for(tab.go_to(url), timeout=30)
        await asyncio.sleep(settle)
        return await tab.page_source
    finally:
        await tab.close()
```

**✅ Pros:**
- CF একবারই solve হয় — worker এর পুরো lifetime এ
- সব tasks benefit পায়, সব pipelines
- Fastest possible approach

**❌ Cons:**
- Complex — threading + asyncio মেশানো
- Browser crash হলে recovery logic দরকার
- Memory সবসময় use হয় (~300MB per worker)
- Worker count বাড়লে proportional memory

**উপযুক্ত:** High-volume production, যেখানে tasks প্রায় continuous।

---

## Recommendation

| Situation | Use |
|---|---|
| এখনই quick fix চাই | Approach 1 (user-data-dir) |
| Best long-term architecture | Approach 2 (per-task async) |
| High volume, always busy workers | Approach 3 (singleton) |

**আমার recommendation: Approach 2** — moderate refactor, সবচেয়ে clean।

---

## Files to Modify (Approach 2)

| File | Change |
|---|---|
| `upload/utils/web_scrape.py` | `_fetch_html(browser, url)` — browser param নেবে |
| `upload/service/info.py` | `browser` param accept করবে |
| `upload/tasks/__init__.py` | `async with Chrome()` wrap করবে, browser pass করবে |
| `upload/tasks/movie_pipeline.py` | browser pass করবে |
| `upload/tasks/tvshow_pipeline.py` | browser pass করবে |
| `auto_up/scraper.py` | optional browser param (নিজেও চালাতে পারবে) |

---

## Pydoll API Quick Reference

| Method | Description |
|---|---|
| `browser.start()` | Browser launch, returns 1st tab |
| `await browser.new_tab(url="")` | New tab in same browser |
| `await browser.get_opened_tabs()` | All open tabs list |
| `await tab.close()` | Close this tab only (browser stays) |
| `await tab.go_to(url)` | Navigate |
| `await tab.page_source` | Get HTML |
| `await tab.enable_auto_solve_cloudflare_captcha()` | CF bypass (per-tab) |

---

## Status

- [ ] Decide approach (1 / 2 / 3)
- [ ] Approach 1: Add `--user-data-dir` to `_chrome_options()` + volume mount
- [ ] Approach 2: Refactor `web_scrape.py` → accept browser param
- [ ] Approach 2: Wrap pipeline in single `async with Chrome()` block
- [ ] Approach 2: Test parallel tab URL resolve with `asyncio.gather()`
- [ ] Approach 3: Implement persistent event loop + browser singleton per worker
