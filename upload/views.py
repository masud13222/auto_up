from django.contrib.auth.decorators import login_required
from django.http import JsonResponse

from .django_q_priority import EnqueueError, enqueue_process_media_task, parse_q_priority
from .models import MediaTask


def _queue_new_task(url: str, *, q_priority: int = 0) -> MediaTask:
    media_task = MediaTask.objects.create(url=url)
    try:
        q_task_id = enqueue_process_media_task(media_task.pk, url, q_priority=q_priority)
    except EnqueueError:
        media_task.delete()
        raise
    media_task.task_id = q_task_id
    media_task.save(update_fields=["task_id"])
    return media_task


def _process_one_with_duplicate_check(url: str, *, q_priority: int = 0) -> dict:
    """
    When URL deduplication is on: block or merge with an existing MediaTask for the same URL.
    Returns a dict suitable for JSON (always includes keys used by the panel).
    """
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

    media_task = _queue_new_task(url, q_priority=q_priority)
    return {
        "success": True,
        "task_id": media_task.pk,
        "message": "Task queued!",
        "redirect": f"/panel/task/{media_task.pk}/",
    }


def _process_one_skip_duplicate_check(url: str, *, q_priority: int = 0) -> dict:
    """Always create a new task and queue (no URL deduplication)."""
    media_task = _queue_new_task(url, q_priority=q_priority)
    return {
        "success": True,
        "task_id": media_task.pk,
        "message": "Task queued!",
        "redirect": f"/panel/task/{media_task.pk}/",
    }


@login_required
def process_media(request):
    """
    AJAX endpoint: one or many URLs (one per line in ``urls``), optional duplicate check.

    POST:
      - ``urls`` — multiline, one URL per line (preferred)
      - ``url`` — single URL (backward compatible)
      - ``skip_duplicate_check`` — if 1/on/true/yes, never match existing MediaTask by URL
      - ``q_priority`` — optional int 0–999999; higher values are dequeued first (ORM broker)
    """
    if request.method != "POST":
        return JsonResponse({"error": "POST method required."}, status=405)

    if "skip_duplicate_check" not in request.POST:
        skip_dup = False
    else:
        _sk = (request.POST.get("skip_duplicate_check") or "1").lower()
        skip_dup = _sk not in ("0", "false", "off", "no")

    q_priority = parse_q_priority(request.POST.get("q_priority"))

    raw_block = request.POST.get("urls", "").strip()
    single = request.POST.get("url", "").strip()
    if raw_block:
        lines = [ln.strip() for ln in raw_block.splitlines() if ln.strip()]
    elif single:
        lines = [single]
    else:
        return JsonResponse({"error": "URL is required."}, status=400)

    def valid_http(u: str) -> bool:
        low = u.lower()
        return low.startswith("http://") or low.startswith("https://")

    results: list[dict] = []
    failed: list[dict] = []

    for url in lines:
        if not valid_http(url):
            failed.append({"url": url, "error": "Must start with http:// or https://"})
            continue
        try:
            if skip_dup:
                payload = _process_one_skip_duplicate_check(url, q_priority=q_priority)
            else:
                payload = _process_one_with_duplicate_check(url, q_priority=q_priority)
        except EnqueueError as e:
            failed.append(
                {
                    "url": url,
                    "error": e.message,
                }
            )
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
