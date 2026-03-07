from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django_q.tasks import async_task
from .models import MovieTask


def index(request):
    """Home page with URL input and recent tasks list."""
    recent_tasks = MovieTask.objects.all()[:20]
    return render(request, 'upload/index.html', {'tasks': recent_tasks})


def process_movie(request):
    """
    AJAX endpoint: receives a URL, checks for duplicates, queues task.
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST method required.'}, status=405)

    url = request.POST.get('url', '').strip()
    if not url:
        return JsonResponse({'error': 'URL is required.'}, status=400)

    # Check if this URL was already processed
    existing = MovieTask.objects.filter(url=url).first()

    if existing:
        if existing.status == 'completed':
            return JsonResponse({
                'success': True,
                'duplicate': True,
                'task_id': existing.pk,
                'message': 'Already processed! Redirecting to results...',
                'redirect': f'/upload/task/{existing.pk}/'
            })
        elif existing.status in ('pending', 'processing'):
            return JsonResponse({
                'success': True,
                'duplicate': True,
                'task_id': existing.pk,
                'message': 'Already in queue! Redirecting...',
                'redirect': f'/upload/task/{existing.pk}/'
            })
        elif existing.status == 'failed':
            # Reset and re-queue
            existing.status = 'pending'
            existing.error_message = ''
            existing.save()

            q_task_id = async_task(
                'upload.tasks.process_movie_task',
                existing.pk,
                task_name=f'Process: {url[:50]}',
            )
            existing.task_id = q_task_id or ''
            existing.save(update_fields=['task_id'])

            return JsonResponse({
                'success': True,
                'task_id': existing.pk,
                'message': 'Re-queued failed task!',
                'redirect': f'/upload/task/{existing.pk}/'
            })

    # Create new task
    movie_task = MovieTask.objects.create(url=url)

    q_task_id = async_task(
        'upload.tasks.process_movie_task',
        movie_task.pk,
        task_name=f'Process: {url[:50]}',
    )
    movie_task.task_id = q_task_id or ''
    movie_task.save(update_fields=['task_id'])

    return JsonResponse({
        'success': True,
        'task_id': movie_task.pk,
        'message': 'Task queued!',
        'redirect': f'/upload/task/{movie_task.pk}/'
    })


def task_detail(request, pk):
    """Task detail page — shows status and results."""
    movie_task = get_object_or_404(MovieTask, pk=pk)
    return render(request, 'upload/task_detail.html', {'task': movie_task})


def task_status_api(request, pk):
    """AJAX endpoint for polling task status."""
    movie_task = get_object_or_404(MovieTask, pk=pk)
    return JsonResponse({
        'status': movie_task.status,
        'title': movie_task.title,
        'error': movie_task.error_message,
        'result': movie_task.result,
    })
