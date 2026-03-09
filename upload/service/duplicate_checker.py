import json
import logging
from django.db.models import Q
from upload.models import MediaTask
from upload.utils.web_scrape import WebScrapeService
from llm.utils.name_extractor import extract_title_info
from llm.services import LLMService
from llm.json_repair import repair_json
from llm.schema.duplicate_schema import DUPLICATE_CHECK_PROMPT

logger = logging.getLogger(__name__)


def check_duplicate(url: str, current_task_pk: int = None) -> dict:
    """
    Full duplicate detection pipeline:
    1. Fetch page title (no LLM needed)
    2. Extract clean name + year
    3. Search DB for matches
    4. If match found, LLM compares to decide action

    Returns:
        {
            "action": "skip" | "process" | "replace",
            "reason": "...",
            "existing_task": MediaTask or None,
            "extracted_name": "...",
            "extracted_year": "..." or None,
            "website_title": "..."
        }
    """
    # Step 1: Get raw website title
    website_title = WebScrapeService.cinefreak_title(url)
    if not website_title:
        logger.info(f"Could not fetch title for {url}, proceeding as new")
        return {
            "action": "process",
            "reason": "Could not fetch page title",
            "existing_task": None,
            "extracted_name": None,
            "extracted_year": None,
            "website_title": None,
        }

    logger.info(f"Website title: {website_title}")

    # Step 2: Extract clean name + year
    info = extract_title_info(website_title)
    name = info.title
    year = info.year
    logger.info(f"Extracted: name='{name}', year='{year}'")

    if not name:
        return {
            "action": "process",
            "reason": "Could not extract title name",
            "existing_task": None,
            "extracted_name": None,
            "extracted_year": year,
            "website_title": website_title,
        }

    # Step 3: Search DB — name only first, then name + year
    matches = _search_db(name, year, exclude_pk=current_task_pk)

    if not matches:
        logger.info(f"No existing match found for '{name}' ({year}). New content.")
        return {
            "action": "process",
            "reason": f"No existing entry found for '{name}'",
            "existing_task": None,
            "extracted_name": name,
            "extracted_year": year,
            "website_title": website_title,
        }

    # Step 4: LLM comparison with best match
    existing_task = matches[0]
    logger.info(f"Found existing match: [{existing_task.pk}] {existing_task.title}")

    result = _llm_compare(existing_task, name, year, website_title)
    result["existing_task"] = existing_task if result["action"] != "process" else None
    result["extracted_name"] = name
    result["extracted_year"] = year
    result["website_title"] = website_title

    logger.info(f"Duplicate check result: action={result['action']}, reason={result['reason']}")
    return result


FUZZY_THRESHOLD = 85


def _get_search_keywords(name: str) -> list[str]:
    """Generate progressively broader search keywords from a name."""
    words = name.strip().split()
    queries = [name]
    for i in range(len(words) - 1, 0, -1):
        partial = " ".join(words[:i])
        if partial != name and len(partial) >= 3:
            queries.append(partial)
    return queries


def _search_db(name: str, year: str = None, exclude_pk: int = None) -> list:
    """
    Search MediaTask for matching entries using fuzzy matching.
    Fetches broader candidates from DB, then scores with rapidfuzz.
    Returns only matches above FUZZY_THRESHOLD, sorted by score (best first).
    """
    from rapidfuzz import fuzz

    base_qs = MediaTask.objects.filter(status__in=['completed', 'processing']).exclude(result__isnull=True)
    if exclude_pk:
        base_qs = base_qs.exclude(pk=exclude_pk)

    keywords = _get_search_keywords(name)
    candidates = {}  # pk -> (task, score)

    # Fetch candidates from DB using keyword variants
    for keyword in keywords:
        # Try name + year first
        if year:
            try:
                qs = base_qs.filter(
                    Q(title__icontains=keyword) | Q(website_title__icontains=keyword),
                    result__year=int(year),
                ).order_by('-updated_at')[:10]
                for task in qs:
                    if task.pk not in candidates:
                        q = name.lower()
                        scores = []
                        if task.title:
                            scores.append(fuzz.partial_ratio(q, task.title.lower()))
                        if task.website_title:
                            scores.append(fuzz.partial_ratio(q, task.website_title.lower()))
                        score = max(scores) if scores else 0
                        if score >= FUZZY_THRESHOLD:
                            candidates[task.pk] = (task, score)
            except (ValueError, TypeError):
                pass

        # Name only (broader)
        qs = base_qs.filter(
            Q(title__icontains=keyword) | Q(website_title__icontains=keyword)
        ).order_by('-updated_at')[:10]
        for task in qs:
            if task.pk not in candidates:
                q = name.lower()
                scores = []
                if task.title:
                    scores.append(fuzz.partial_ratio(q, task.title.lower()))
                if task.website_title:
                    scores.append(fuzz.partial_ratio(q, task.website_title.lower()))
                score = max(scores) if scores else 0
                if score >= FUZZY_THRESHOLD:
                    candidates[task.pk] = (task, score)

    # Sort by score (best first) and return task list
    sorted_matches = sorted(candidates.values(), key=lambda x: x[1], reverse=True)
    matches = [task for task, score in sorted_matches]

    if matches:
        logger.debug(
            f"DB fuzzy match for '{name}': {len(matches)} found "
            f"(scores: {[s for _, s in sorted_matches]})"
        )

    return matches


def _get_existing_resolutions(task: MediaTask) -> list:
    """Extract resolution keys from existing task's result. Filters out null values."""
    result = task.result or {}

    # Movie: download_links keys (only non-null values)
    dl = result.get("download_links", {})
    if dl:
        return sorted(k for k, v in dl.items() if v)

    # TV show: collect all resolution keys across seasons (only non-null)
    resolutions = set()
    for season in result.get("seasons", []):
        for item in season.get("download_items", []):
            resolutions.update(k for k, v in item.get("resolutions", {}).items() if v)

    return sorted(resolutions)


def _llm_compare(existing_task: MediaTask, new_name: str, new_year: str, new_website_title: str) -> dict:
    """Use LLM to compare new vs existing content."""
    result_data = existing_task.result or {}
    existing_resolutions = _get_existing_resolutions(existing_task)

    # Determine existing content type: DB field first, then fallback to data check
    if existing_task.content_type:
        is_tvshow = existing_task.content_type == 'tvshow'
    else:
        is_tvshow = bool(result_data.get("seasons"))

    episode_count = 0
    episodes = []
    if is_tvshow:
        for season in result_data.get("seasons", []):
            for item in season.get("download_items", []):
                episode_count += 1
                label = item.get("label", "")
                res = item.get("resolutions", {})
                ep_res = sorted(k for k, v in res.items() if v)
                episodes.append(f"{label}: {','.join(ep_res)}")

    comparison_data = json.dumps({
        "new_website_title": new_website_title,
        "new_name": new_name,
        "new_year": new_year,
        "existing_title": existing_task.title,
        "existing_resolutions": existing_resolutions,
        "existing_type": "tvshow" if is_tvshow else "movie",
        "existing_episode_count": episode_count,
        "existing_episodes": episodes,
    }, ensure_ascii=False)

    logger.info(f"LLM comparing: '{new_name}' vs existing '{existing_task.title}' "
                f"(type={'tvshow' if is_tvshow else 'movie'}, res={existing_resolutions}, episodes={episode_count})")

    try:
        raw = LLMService.generate_completion(
            prompt=comparison_data,
            system_prompt=DUPLICATE_CHECK_PROMPT,
            purpose='duplicate_check',
        )
        result = repair_json(raw)

        action = result.get("action", "process")
        reason = result.get("reason", "LLM decision")
        detected_new_type = result.get("detected_new_type", "movie")
        missing_resolutions = result.get("missing_resolutions", [])
        has_new_episodes = result.get("has_new_episodes", False)

        # Validate action
        if action not in ("skip", "update", "replace", "process"):
            action = "process"
            reason = f"Invalid LLM action, defaulting to process: {result}"

        return {
            "action": action,
            "reason": reason,
            "detected_new_type": detected_new_type,
            "missing_resolutions": missing_resolutions,
            "has_new_episodes": has_new_episodes,
        }

    except Exception as e:
        logger.warning(f"LLM comparison failed, defaulting to process: {e}")
        return {
            "action": "process",
            "reason": f"LLM comparison error: {e}",
            "detected_new_type": "movie",
            "missing_resolutions": [],
            "has_new_episodes": False,
        }


