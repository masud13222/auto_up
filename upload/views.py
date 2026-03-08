from django.http import JsonResponse
from django_q.tasks import async_task
from .models import MediaTask


def process_media(request):
    """
    AJAX endpoint: receives a URL, checks for duplicates, queues task.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required.'}, status=405)

    url = request.POST.get('url', '').strip()
    if not url:
        return JsonResponse({'error': 'URL is required.'}, status=400)

    # Check if this URL was already processed
    existing = MediaTask.objects.filter(url=url).first()

    if existing:
        if existing.status == 'completed':
            return JsonResponse({
                'success': True,
                'duplicate': True,
                'task_id': existing.pk,
                'message': 'Already processed! Redirecting to results...',
                'redirect': f'/panel/task/{existing.pk}/'
            })
        elif existing.status in ('pending', 'processing'):
            return JsonResponse({
                'success': True,
                'duplicate': True,
                'task_id': existing.pk,
                'message': 'Already in queue! Redirecting...',
                'redirect': f'/panel/task/{existing.pk}/'
            })
        elif existing.status == 'failed':
            # Reset and re-queue
            existing.status = 'pending'
            existing.error_message = ''
            existing.save()

            q_task_id = async_task(
                'upload.tasks.process_media_task',
                existing.pk,
                task_name=f'Process: {url[:50]}',
            )
            existing.task_id = q_task_id or ''
            existing.save(update_fields=['task_id'])

            return JsonResponse({
                'success': True,
                'task_id': existing.pk,
                'message': 'Re-queued failed task!',
                'redirect': f'/panel/task/{existing.pk}/'
            })

    # Create new task
    media_task = MediaTask.objects.create(url=url)

    q_task_id = async_task(
        'upload.tasks.process_media_task',
        media_task.pk,
        task_name=f'Process: {url[:50]}',
    )
    media_task.task_id = q_task_id or ''
    media_task.save(update_fields=['task_id'])

    return JsonResponse({
        'success': True,
        'task_id': media_task.pk,
        'message': 'Task queued!',
        'redirect': f'/panel/task/{media_task.pk}/'
    })
