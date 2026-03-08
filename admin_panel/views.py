from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout
from django.http import JsonResponse
from django.core.paginator import Paginator
from django.views.decorators.http import require_POST
from django.utils.safestring import mark_safe
from django_q.tasks import async_task
import json

from upload.models import MediaTask
from settings.models import GoogleConfig, UploadSettings


def _get_stats():
    qs = MediaTask.objects
    return {
        'total': qs.count(),
        'completed': qs.filter(status='completed').count(),
        'processing': qs.filter(status='processing').count(),
        'pending': qs.filter(status='pending').count(),
        'failed': qs.filter(status='failed').count(),
    }


@login_required
def dashboard(request):
    return render(request, 'panel/dashboard.html', {'stats': _get_stats()})


@login_required
def recent_tasks_fragment(request):
    tasks = MediaTask.objects.all()[:10]
    return render(request, 'panel/fragments/recent_tasks.html', {'tasks': tasks})


@login_required
def queue_status_api(request):
    processing = MediaTask.objects.filter(status='processing').count()
    pending = MediaTask.objects.filter(status='pending').count()
    return render(request, 'panel/fragments/queue_status.html', {
        'processing_count': processing,
        'pending_count': pending,
    })


@login_required
def queue(request):
    status_filter = request.GET.get('status', '')
    qs = MediaTask.objects.all()
    if status_filter:
        qs = qs.filter(status=status_filter)

    paginator = Paginator(qs, 20)
    tasks = paginator.get_page(request.GET.get('page', 1))

    statuses = [
        ('', 'All', ''),
        ('pending', 'Pending', ''),
        ('processing', 'Processing', ''),
        ('completed', 'Completed', ''),
        ('failed', 'Failed', ''),
    ]

    return render(request, 'panel/queue.html', {
        'tasks': tasks,
        'statuses': statuses,
        'current_status': status_filter,
    })


@login_required
def task_detail(request, pk):
    task = get_object_or_404(MediaTask, pk=pk)
    result_json = mark_safe(json.dumps(task.result)) if task.result else 'null'
    return render(request, 'panel/task_detail.html', {'task': task, 'result_json': result_json})



@login_required
def task_status_api(request, pk):
    task = get_object_or_404(MediaTask, pk=pk)
    return JsonResponse({
        'status': task.status,
        'title': task.title,
        'error': task.error_message,
        'result': task.result,
    })


@login_required
@require_POST
def requeue_task(request, pk):
    task = get_object_or_404(MediaTask, pk=pk)
    task.status = 'pending'
    task.error_message = ''
    task.save()
    q_id = async_task('upload.tasks.process_media_task', task.pk, task_name=f'Process: {task.url[:50]}')
    task.task_id = q_id or ''
    task.save(update_fields=['task_id'])
    return redirect('panel:task_detail', pk=pk)


@login_required
@require_POST
def delete_task(request, pk):
    task = get_object_or_404(MediaTask, pk=pk)
    task.delete()
    return redirect('panel:queue')


@login_required
def process(request):
    return render(request, 'panel/process.html')


@login_required
def settings_view(request):
    obj = UploadSettings.objects.first()
    if request.method == 'POST':
        folder_id = request.POST.get('upload_folder_id', '').strip()
        worker_count = int(request.POST.get('worker_count', 1))
        if not obj:
            obj = UploadSettings()
        obj.upload_folder_id = folder_id
        obj.worker_count = max(1, worker_count)
        obj.save()
        return redirect('panel:settings')
    return render(request, 'panel/settings.html', {'obj': obj})


@login_required
def google_accounts(request):
    accounts = GoogleConfig.objects.all()
    return render(request, 'panel/google_accounts.html', {'accounts': accounts})


@login_required
@require_POST
def add_google_account(request):
    name = request.POST.get('name', 'New Account').strip()
    config_file = request.FILES.get('config_file')
    if config_file:
        GoogleConfig.objects.create(name=name, config_file=config_file)
    return redirect('panel:google_accounts')


@login_required
@require_POST
def delete_google_account(request, pk):
    acc = get_object_or_404(GoogleConfig, pk=pk)
    acc.delete()
    return redirect('panel:google_accounts')


@login_required
def llm_settings(request):
    from llm.models import LLMSettings
    obj = LLMSettings.objects.first()
    if request.method == 'POST':
        if not obj:
            obj = LLMSettings()
        obj.api_key = request.POST.get('api_key', '').strip()
        obj.base_url = request.POST.get('base_url', '').strip()
        obj.model_name = request.POST.get('model', '').strip()
        obj.save()
        return redirect('panel:llm_settings')
    return render(request, 'panel/llm_settings.html', {'obj': obj})


def logout_view(request):
    logout(request)
    return redirect('panel:login')
