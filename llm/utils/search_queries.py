from __future__ import annotations


def build_search_queries(
    name: str,
    year: str | int | None = None,
    season_tag: str | None = None,
    alt_name: str | None = None,
) -> list[dict]:
    """
    Build ordered search query specs for DB icontains + FlixBD phases.
    Higher ``priority`` values are tried first (see runtime_helpers.fetch_flixbd_results).
    """
    primary = (name or "").strip()
    alt = (alt_name or "").strip()
    if alt and alt.lower() == primary.lower():
        alt = ""

    if not primary:
        return []

    yt = str(year).strip() if year is not None and str(year).strip() else ""
    st = (season_tag or "").strip()

    queries: list[dict] = []
    seen: set[str] = set()

    def add_query(value: str, tag: str, priority: int) -> None:
        q = (value or "").strip()
        if len(q) < 2:
            return
        key = q.lower()
        if key in seen:
            return
        seen.add(key)
        queries.append({"q": q, "tag": tag, "priority": priority})

    if st and yt:
        add_query(f"{primary} {yt} {st}", "name_year_season", 10)
        if alt:
            add_query(f"{alt} {yt} {st}", "alt_year_season", 10)
    if yt:
        add_query(f"{primary} {yt}", "name_year", 9)
        if alt:
            add_query(f"{alt} {yt}", "alt_year", 9)
    if st:
        add_query(f"{primary} {st}", "name_season", 8)
        if alt:
            add_query(f"{alt} {st}", "alt_season", 8)

    add_query(primary, "name", 7)
    if alt:
        add_query(alt, "alt_name", 6)

    return queries
