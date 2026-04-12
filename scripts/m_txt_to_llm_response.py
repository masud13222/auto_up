#!/usr/bin/env python3
"""
Standalone Mistral AI smoke test (no Django DB).

- **combined**: same system prompt as production (`get_combined_system_prompt`) + user content from `m.txt` (or `--input`).
- **auto-up**: `auto_up/schema.py` filter prompt + a tiny sample payload.

Do **not** commit API keys. Use env, `.env`, or `--api-key` for local runs.

Examples:
  set MISTRAL_API_KEY=... && uv run python scripts/test.py
  uv run python scripts/test.py --api-key YOUR_KEY
  uv run python scripts/test.py --mode auto-up
  uv run python scripts/test.py --input path/to/page.md --dup
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Optional: MISTRAL_API_KEY / MISTRAL_MODEL in project .env
try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from mistralai import Mistral

from auto_up.schema import AUTO_FILTER_SYSTEM_PROMPT
from llm.json_repair import repair_json
from llm.schema import get_combined_system_prompt

# Defaults match a typical Mistral panel entry (override via env or CLI)
DEFAULT_MODEL = "mistral-medium-latest"
DEFAULT_LABEL = "Mistral"


def _call_mistral(api_key: str, model: str, system_prompt: str, user_prompt: str, temperature: float) -> str:
    client = Mistral(api_key=api_key)
    response = client.chat.complete(
        model=model,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=temperature,
    )
    return (response.choices[0].message.content or "").strip()


def _summarize_combined_parsed(data: dict) -> None:
    ct = data.get("content_type")
    inner = data.get("data") or {}
    title = inner.get("title", "?")
    print(f"  content_type: {ct}")
    print(f"  data.title: {title}")
    if ct == "movie":
        links = inner.get("download_links") or {}
        print(f"  download_links keys: {list(links.keys())}")
        for quality, entries in list(links.items())[:5]:
            count = len(entries) if isinstance(entries, list) else 0
            print(f"    {quality}: {count} file(s)")
    elif ct == "tvshow":
        seasons = inner.get("seasons") or []
        for si, s in enumerate(seasons[:3]):
            for ji, item in enumerate((s.get("download_items") or [])[:3]):
                res = item.get("resolutions") or {}
                counts = {quality: len(entries) for quality, entries in res.items() if isinstance(entries, list)}
                print(f"  season[{si}] item[{ji}] res keys: {list(res.keys())} | file counts: {counts}")
    dup = data.get("duplicate_check")
    if dup:
        print(f"  duplicate_check.action: {dup.get('action')}")
        print(f"  duplicate_check.reason (trunc): {(dup.get('reason') or '')[:120]}...")


def _sample_auto_up_user_json() -> str:
    sample = [
        {
            "raw_title": "Test Movie (2024) 480p 720p 1080p WEB-DL",
            "clean_name": "Test Movie",
            "year": 2024,
            "season_tag": None,
            "url": "https://example.com/movie",
            "db_results": {"results": [], "has_matches": False},
        }
    ]
    return json.dumps(sample, separators=(",", ":"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Mistral smoke test (combined + optional auto_up)")
    parser.add_argument(
        "--mode",
        choices=("combined", "auto-up", "both"),
        default="combined",
        help="Which prompt to run",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=ROOT / "m.txt",
        help="Markdown / HTML-ish content for combined mode (default: repo m.txt)",
    )
    parser.add_argument("--model", default=os.environ.get("MISTRAL_MODEL", DEFAULT_MODEL).strip())
    parser.add_argument(
        "--api-key",
        default=None,
        help="Mistral API key (else env MISTRAL_API_KEY)",
    )
    parser.add_argument("--temperature", type=float, default=0.1)
    parser.add_argument(
        "--dup",
        action="store_true",
        help="Inject minimal duplicate-check context into combined prompt",
    )
    parser.add_argument("--label", default=DEFAULT_LABEL, help="Print label only")
    args = parser.parse_args()

    api_key = (args.api_key or os.environ.get("MISTRAL_API_KEY", "")).strip()
    if not api_key:
        print(
            "Missing API key. Set MISTRAL_API_KEY, add it to .env, or pass --api-key.\n"
            "Do not commit keys to git.",
            file=sys.stderr,
        )
        return 1

    def run_combined() -> None:
        path = args.input
        if not path.is_file():
            print(f"Input file not found: {path}", file=sys.stderr)
            raise SystemExit(2)
        user_content = path.read_text(encoding="utf-8", errors="replace")
        db_candidates = None
        flixbd = None
        if args.dup:
            db_candidates = [{"id": 1, "title": "Single Papa", "year": 2025, "resolutions": ["720p"], "type": "tvshow"}]
            flixbd = [{"title": "Single Papa", "download_links": {"qualities": "720p"}}]

        system_prompt = get_combined_system_prompt(
            extra_below=False,
            extra_above=False,
            max_extra=0,
            db_match_candidates=db_candidates,
            flixbd_results=flixbd,
        )
        print(f"\n[{args.label}] model={args.model!r} mode=combined input={path}")
        print(f"system_prompt chars: {len(system_prompt)} | user chars: {len(user_content)}")

        raw = _call_mistral(api_key, args.model, system_prompt, user_content, args.temperature)
        print("\n--- RAW (first 2000 chars) ---\n")
        print(raw[:2000] + ("…" if len(raw) > 2000 else ""))

        try:
            parsed = repair_json(raw)
        except ValueError as e:
            print(f"\n❌ JSON parse/repair failed: {e}")
            return

        print("\n--- PARSED SUMMARY ---")
        _summarize_combined_parsed(parsed)
        out_path = ROOT / "scripts" / "mistral_combined_last.json"
        out_path.write_text(json.dumps(parsed, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nFull parsed JSON written to: {out_path}")

    def run_auto_up() -> None:
        user_prompt = _sample_auto_up_user_json()
        print(f"\n[{args.label}] model={args.model!r} mode=auto-up")
        print(f"system_prompt chars: {len(AUTO_FILTER_SYSTEM_PROMPT)} | user chars: {len(user_prompt)}")

        raw = _call_mistral(api_key, args.model, AUTO_FILTER_SYSTEM_PROMPT, user_prompt, args.temperature)
        print("\n--- RAW ---\n")
        print(raw)

        try:
            parsed = repair_json(raw)
        except ValueError as e:
            print(f"\n❌ JSON parse/repair failed: {e}")
            return

        decisions = (parsed.get("decisions") or [])[:5]
        print(f"\n--- decisions (up to 5): {len(decisions)} row(s) ---")
        for d in decisions:
            print(f"  {d.get('action')}: {d.get('url')} — {d.get('reason', '')[:80]}")

    if args.mode in ("combined", "both"):
        run_combined()
    if args.mode in ("auto-up", "both"):
        run_auto_up()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
