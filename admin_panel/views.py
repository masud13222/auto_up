from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout
from django.http import JsonResponse
from django.core.paginator import Paginator
from django.urls import reverse
from django.views.decorators.http import require_POST
from django.utils.safestring import mark_safe
import json
import time

from upload.django_q_priority import EnqueueError, enqueue_process_media_task, parse_q_priority
from upload.models import MediaTask
from settings.models import GoogleConfig, UploadSettings

_LLM_CHAT_HISTORY_LIMIT = 10
_LLM_CHAT_SELECTED_CONFIG_SESSION_KEY = "panel_llm_chat_selected_config_id"


def _coerce_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_temperature(value, default: float = 0.2) -> float:
    try:
        temp = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(temp, 2.0))


def _llm_chat_history_key(config_id: int) -> str:
    return f"panel_llm_chat_history_{config_id}"


def _trim_llm_chat_history(history) -> list[dict]:
    cleaned: list[dict] = []
    for item in list(history or [])[-(_LLM_CHAT_HISTORY_LIMIT * 2):]:
        role = str((item or {}).get("role") or "").strip().lower()
        content = str((item or {}).get("content") or "").strip()
        if role in {"user", "assistant"} and content:
            cleaned.append({"role": role, "content": content})
    return cleaned


def _build_llm_chat_prompt(history: list[dict], user_message: str) -> str:
    message = (user_message or "").strip()
    trimmed_history = _trim_llm_chat_history(history)
    if not trimmed_history:
        return message

    transcript = [
        "Continue the conversation below.",
        "Use the previous messages as context when they are relevant.",
        "",
    ]
    for item in trimmed_history:
        speaker = "User" if item["role"] == "user" else "Assistant"
        transcript.append(f"{speaker}: {item['content']}")
    transcript.append(f"User: {message}")
    transcript.append("Assistant:")
    return "\n".join(transcript)


def _build_llm_chat_bootstrap(request, configs) -> dict:
    selected_id = _coerce_int(request.session.get(_LLM_CHAT_SELECTED_CONFIG_SESSION_KEY))
    config_ids = {cfg.pk for cfg in configs}
    if selected_id not in config_ids:
        selected_id = configs[0].pk if configs else None

    histories = {
        str(cfg.pk): _trim_llm_chat_history(
            request.session.get(_llm_chat_history_key(cfg.pk), [])
        )
        for cfg in configs
    }

    return {
        "selected_config_id": selected_id,
        "histories": histories,
        "chat_url": reverse("panel:llm_chat_api"),
        "default_system_prompt": "You are a helpful assistant.",
        "default_temperature": "0.2",
        "configs": [
            {
                "id": cfg.pk,
                "name": cfg.name,
                "sdk": cfg.sdk,
                "model_name": cfg.model_name,
                "base_url": cfg.base_url,
                "is_primary": cfg.is_primary,
                "is_active": cfg.is_active,
            }
            for cfg in configs
        ],
    }


def _get_stats():
    qs = MediaTask.objects
    return {
        'total': qs.count(),
        'completed': qs.filter(status='completed').count(),
        'processing': qs.filter(status='processing').count(),
        'pending': qs.filter(status='pending').count(),
        'partial': qs.filter(status='partial').count(),
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
        ('partial', 'Partial', ''),
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
    q_pri = parse_q_priority(request.POST.get("q_priority"))
    try:
        q_id = enqueue_process_media_task(task.pk, task.url, q_priority=q_pri)
    except EnqueueError as e:
        msg = e.message[:2000] if len(e.message) > 2000 else e.message
        task.status = 'failed'
        task.error_message = msg
        task.save(update_fields=['status', 'error_message'])
        return redirect('panel:task_detail', pk=pk)
    task.task_id = q_id
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
        if not obj:
            obj = UploadSettings()
        obj.upload_folder_id = folder_id
        obj.extra_res_below = request.POST.get('extra_res_below') == 'on'
        obj.extra_res_above = request.POST.get('extra_res_above') == 'on'
        obj.max_extra_resolutions = int(request.POST.get('max_extra_resolutions', 0))
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
    from llm.models import LLMConfig
    if request.method == 'POST':
        action = request.POST.get('action', '')

        if action == 'add':
            LLMConfig.objects.create(
                name=request.POST.get('name', 'New Config').strip(),
                sdk=request.POST.get('sdk', 'openai').strip(),
                base_url=request.POST.get('base_url', '').strip(),
                api_key=request.POST.get('api_key', '').strip(),
                model_name=request.POST.get('model_name', '').strip(),
                is_primary=not LLMConfig.objects.filter(is_primary=True).exists(),
            )
        elif action == 'edit':
            pk = request.POST.get('pk')
            config = LLMConfig.objects.get(pk=pk)
            config.name = request.POST.get('name', config.name).strip()
            config.sdk = request.POST.get('sdk', config.sdk).strip()
            config.base_url = request.POST.get('base_url', '').strip()
            config.api_key = request.POST.get('api_key', config.api_key).strip()
            config.model_name = request.POST.get('model_name', config.model_name).strip()
            config.save()
        elif action == 'delete':
            pk = request.POST.get('pk')
            LLMConfig.objects.filter(pk=pk).delete()
        elif action == 'set_primary':
            pk = request.POST.get('pk')
            LLMConfig.objects.update(is_primary=False)
            LLMConfig.objects.filter(pk=pk).update(is_primary=True)
        elif action == 'toggle_active':
            pk = request.POST.get('pk')
            config = LLMConfig.objects.get(pk=pk)
            config.is_active = not config.is_active
            config.save(update_fields=['is_active'])

        return redirect('panel:llm_settings')

    configs = LLMConfig.objects.all().order_by('-is_primary', 'pk')
    return render(request, 'panel/llm_settings.html', {
        'configs': configs,
        'chat_bootstrap': _build_llm_chat_bootstrap(request, list(configs)),
    })


@login_required
@require_POST
def llm_chat_api(request):
    from llm.models import LLMConfig
    from llm.services import _try_one_config

    action = (request.POST.get("action") or "send").strip().lower()
    config_id = _coerce_int(request.POST.get("config_id"))
    system_prompt = str(request.POST.get("system_prompt") or "").strip() or "You are a helpful assistant."
    temperature = _coerce_temperature(request.POST.get("temperature"), default=0.2)
    user_message = str(request.POST.get("user_message") or "").strip()

    config = LLMConfig.objects.filter(pk=config_id).first()
    if config is None:
        return JsonResponse({"error": "Please choose a valid LLM config first."}, status=400)

    request.session[_LLM_CHAT_SELECTED_CONFIG_SESSION_KEY] = config.pk
    history_key = _llm_chat_history_key(config.pk)
    history = _trim_llm_chat_history(request.session.get(history_key, []))

    if action == "clear":
        request.session[history_key] = []
        request.session.modified = True
        return JsonResponse({
            "success": True,
            "cleared": True,
            "history": [],
            "config_id": config.pk,
            "config_label": f"{config.name} ({config.sdk}:{config.model_name})",
        })

    if not user_message:
        return JsonResponse({"error": "Type a message before sending."}, status=400)

    prompt = _build_llm_chat_prompt(history, user_message)
    started = time.perf_counter()
    try:
        assistant_message = _try_one_config(
            config,
            prompt,
            system_prompt,
            temperature=temperature,
            purpose="panel_chat_test",
        )
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return JsonResponse({
            "error": str(exc),
            "config_id": config.pk,
            "config_label": f"{config.name} ({config.sdk}:{config.model_name})",
            "duration_ms": duration_ms,
        }, status=500)

    duration_ms = int((time.perf_counter() - started) * 1000)
    history.extend(
        [
            {"role": "user", "content": user_message},
            {"role": "assistant", "content": assistant_message},
        ]
    )
    history = _trim_llm_chat_history(history)
    request.session[history_key] = history
    request.session.modified = True

    return JsonResponse({
        "success": True,
        "config_id": config.pk,
        "config_label": f"{config.name} ({config.sdk}:{config.model_name})",
        "assistant_message": assistant_message,
        "history": history,
        "duration_ms": duration_ms,
        "message_length": len(assistant_message or ""),
    })


def logout_view(request):
    logout(request)
    return redirect('panel:login')
