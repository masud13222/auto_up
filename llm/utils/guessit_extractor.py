from typing import Optional

from guessit_fork import guessit

from llm.utils.name_extractor import TitleInfo, extract_title_info as legacy_extract_title_info


def _normalize_year(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_season_tag(parsed: dict) -> Optional[str]:
    season_tag = parsed.get("season_tag")
    if isinstance(season_tag, str) and season_tag.strip():
        return season_tag.strip()
    return None


def extract_title_info(text: str) -> TitleInfo:
    raw = text or ""
    try:
        parsed = guessit(raw)
    except Exception:
        return legacy_extract_title_info(raw)

    title = parsed.get("title")
    if not isinstance(title, str) or not title.strip():
        return legacy_extract_title_info(raw)

    info = TitleInfo(
        title=title.strip(),
        year=_normalize_year(parsed.get("year")),
        season_tag=_normalize_season_tag(parsed),
        content_tag=None,
        raw=raw,
    )
    if not info.title:
        return legacy_extract_title_info(raw)
    return info


def extract_title(text: str) -> str:
    return str(extract_title_info(text))


def build_search_queries(name: str, year: str | int | None = None, season_tag: str | None = None) -> list[dict]:
    name = (name or "").strip()
    if not name:
        return []

    year_text = str(year).strip() if year is not None and str(year).strip() else ""
    season_text = (season_tag or "").strip()

    queries: list[dict] = []
    seen: set[str] = set()

    def add_query(value: str, tag: str, priority: int) -> None:
        q = (value or "").strip()
        if not q or q.lower() in seen:
            return
        seen.add(q.lower())
        queries.append({"q": q, "tag": tag, "priority": priority})

    if season_text:
        if year_text:
            add_query(f"{name} {year_text} {season_text}", "name_year_season", 3)
        else:
            add_query(f"{name} {season_text}", "name_year_season", 3)
    if year_text:
        add_query(f"{name} {year_text}", "name_year", 2)
    add_query(name, "name", 1)

    return queries
