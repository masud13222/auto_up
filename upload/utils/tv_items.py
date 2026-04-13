import re


def _normalize_episode_range(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "-" in text or "–" in text:
        parts = re.split(r"[-–]", text, maxsplit=1)
        if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip().isdigit():
            a = int(parts[0].strip())
            b = int(parts[1].strip())
            if a <= b:
                return f"{a:02d}-{b:02d}"
        return text
    if text.isdigit():
        return f"{int(text):02d}"
    return text


def _coerce_season_number(value):
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    return value


def tv_item_key(item: dict) -> tuple[str, str | None, str]:
    """
    Stable TV item key for merge/skip logic.
    Uses explicit episode_range when present; no label parsing/inference.
    """
    item_type = str(item.get("type") or "").strip()
    episode_range = _normalize_episode_range(item.get("episode_range"))
    # Combo packs for the same season should key by coverage, not by display label.
    return (item_type, episode_range, "")


def tv_item_bounds(item: dict) -> tuple[int, int] | None:
    """Episode bounds from explicit episode_range only; no label inference."""
    raw = item.get("episode_range")
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if "-" in s or "–" in s:
        parts = re.split(r"[-–]", s, maxsplit=1)
        if len(parts) == 2 and parts[0].strip().isdigit() and parts[1].strip().isdigit():
            a = int(parts[0].strip())
            b = int(parts[1].strip())
            if a <= b:
                return (a, b)
        return None
    if s.isdigit():
        v = int(s)
        return (v, v)
    return None


def tv_items_overlap(existing_item: dict, incoming_item: dict) -> bool:
    """Overlap by season item coverage. A combo pack implies full-season overlap."""
    if existing_item.get("type") == "combo_pack" or incoming_item.get("type") == "combo_pack":
        return True
    a = tv_item_bounds(existing_item)
    b = tv_item_bounds(incoming_item)
    if a is None or b is None:
        return False
    return not (a[1] < b[0] or b[1] < a[0])


def split_tv_replace_scope(existing_result: dict, incoming_data: dict) -> tuple[dict, dict, bool]:
    """
    Split existing TV result into:
    - remove_result: overlapping items to delete/replace
    - keep_result: unaffected items to preserve
    - requires_full_replace: True when selective replace is unsafe

    Selective replace is unsafe when either side involves a combo_pack, because
    the runtime cannot preserve unaffected ranges inside a whole-season pack.
    """
    incoming_by_season = {
        _coerce_season_number(s.get("season_number")): list(s.get("download_items", []))
        for s in incoming_data.get("seasons", [])
    }

    remove_result = dict(existing_result)
    keep_result = dict(existing_result)
    remove_seasons = []
    keep_seasons = []
    requires_full_replace = False

    for season in existing_result.get("seasons", []):
        snum = _coerce_season_number(season.get("season_number"))
        incoming_items = incoming_by_season.get(snum, [])
        removed_items = []
        kept_items = []

        for old_item in season.get("download_items", []):
            overlaps = incoming_items and any(
                tv_items_overlap(old_item, new_item) for new_item in incoming_items
            )
            if overlaps:
                if old_item.get("type") == "combo_pack" or any(
                    new_item.get("type") == "combo_pack" for new_item in incoming_items
                ):
                    requires_full_replace = True
                removed_items.append(old_item)
            else:
                kept_items.append(old_item)

        if removed_items:
            remove_seasons.append({"season_number": snum, "download_items": removed_items})
        if kept_items:
            keep_seasons.append({"season_number": snum, "download_items": kept_items})

    remove_result["seasons"] = remove_seasons
    keep_result["seasons"] = keep_seasons
    return remove_result, keep_result, requires_full_replace
