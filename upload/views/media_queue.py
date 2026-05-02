"""JSON API to enqueue scrape/upload MediaTask rows."""

from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, QueryDict

from upload.django_q_priority import EnqueueError, enqueue_process_media_task, parse_q_priority
from upload.models import MediaTask


def _has_http_or_https_scheme(url: str) -> bool:
    low = url.lower()
    return low.startswith("http://") or low.startswith("https://")


def _coerce_skip_duplicate_check(post) -> bool:
    if "skip_duplicate_check" not in post:
        return False
    sk = (post.get("skip_duplicate_check") or "1").lower()
    return sk not in ("0", "false", "off", "no")


def _url_lines_from_post(post: QueryDict) -> list[str]:
    """Non-empty trimmed lines from ``urls`` (newline block) or a single ``url``."""
    raw_block = post.get("urls", "").strip()
    single = post.get("url", "").strip()
    if raw_block:
        return [ln.strip() for ln in raw_block.splitlines() if ln.strip()]
    if single:
        return [single]
    return []


def queue_new_media_task(url: str, *, q_priority: int = 0) -> MediaTask:
    media_task = MediaTask.objects.create(url=url)
    try:
        q_task_id = enqueue_process_media_task(media_task.pk, url, q_priority=q_priority)
    except EnqueueError:
        media_task.delete()
        raise
    media_task.task_id = q_task_id
    media_task.save(update_fields=["task_id"])
    return media_task


def process_one_with_url_dedupe(url: str, *, q_priority: int = 0) -> dict:
    """Merge with existing row when URL matches settings; payload for JSON responses."""
    existing = MediaTask.objects.filter(url=url).first()

    if existing:
        if existing.status == "completed":
            return {
                "success": True,
                "duplicate": True,
                "task_id": existing.pk,
                "message": "Already processed! Redirecting to results...",
                "redirect": f"/panel/task/{existing.pk}/",
            }
        if existing.status in ("pending", "processing"):
            return {
                "success": True,
                "duplicate": True,
                "task_id": existing.pk,
                "message": "Already in queue! Redirecting...",
                "redirect": f"/panel/task/{existing.pk}/",
            }
        if existing.status == "failed":
            existing.status = "pending"
            existing.error_message = ""
            existing.save()

            try:
                q_task_id = enqueue_process_media_task(existing.pk, url, q_priority=q_priority)
            except EnqueueError as e:
                existing.status = "failed"
                existing.error_message = e.message[:2000] if len(e.message) > 2000 else e.message
                existing.save(update_fields=["status", "error_message"])
                raise

            existing.task_id = q_task_id
            existing.save(update_fields=["task_id"])

            return {
                "success": True,
                "task_id": existing.pk,
                "message": "Re-queued failed task!",
                "redirect": f"/panel/task/{existing.pk}/",
            }

    media_task = queue_new_media_task(url, q_priority=q_priority)
    return {
        "success": True,
        "task_id": media_task.pk,
        "message": "Task queued!",
        "redirect": f"/panel/task/{media_task.pk}/",
    }


def process_one_always_new(url: str, *, q_priority: int = 0) -> dict:
    """New row every time — no URL deduplication."""
    media_task = queue_new_media_task(url, q_priority=q_priority)
    return {
        "success": True,
        "task_id": media_task.pk,
        "message": "Task queued!",
        "redirect": f"/panel/task/{media_task.pk}/",
    }


@login_required
def process_media(request):
    """
    POST: enqueue one URL or batch (newline-separated ``urls``); optional duplicate skip.

    Fields: urls | url, skip_duplicate_check, q_priority
    """
    if request.method != "POST":
        return JsonResponse({"error": "POST method required."}, status=405)

    skip_dup = _coerce_skip_duplicate_check(request.POST)
    q_priority = parse_q_priority(request.POST.get("q_priority"))
    lines = _url_lines_from_post(request.POST)
    if not lines:
        return JsonResponse({"error": "URL is required."}, status=400)

    results: list[dict] = []
    failed: list[dict] = []

    for url in lines:
        if not _has_http_or_https_scheme(url):
            failed.append({"url": url, "error": "Must start with http:// or https://"})
            continue
        try:
            if skip_dup:
                payload = process_one_always_new(url, q_priority=q_priority)
            else:
                payload = process_one_with_url_dedupe(url, q_priority=q_priority)
        except EnqueueError as e:
            failed.append({"url": url, "error": e.message})
            continue
        results.append({"url": url, **payload})

    if len(lines) == 1 and len(results) == 1 and not failed:
        r = results[0]
        body = {k: v for k, v in r.items() if k != "url"}
        return JsonResponse(body)

    msg_parts = [f"Queued {len(results)}"]
    if failed:
        msg_parts.append(f"{len(failed)} invalid/skipped")
    return JsonResponse(
        {
            "success": len(results) > 0,
            "batch": True,
            "queued": len(results),
            "failed_count": len(failed),
            "results": results,
            "failed": failed,
            "message": ", ".join(msg_parts),
        }
    )
