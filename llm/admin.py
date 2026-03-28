import json
import time

from django.contrib import admin
from django.db.models import Sum, Count
from django.shortcuts import redirect
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import escape
from django.utils.safestring import mark_safe
from datetime import timedelta
from .models import LLMConfig, LLMUsage
from .services import _try_one_config


_CHAT_HISTORY_LIMIT = 10
_CHAT_FORM_STATE_KEY = "llm_admin_chat_form_state"
_CHAT_RESULT_FLASH_KEY = "llm_admin_chat_result_flash"


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


def _highlight_json_html(obj) -> str:
    """Pretty JSON as HTML spans (keys/strings/numbers/bool/null) for admin display."""
    parts: list[str] = []

    def scalar(v) -> None:
        if v is None:
            parts.append('<span class="json-lit json-null">null</span>')
        elif isinstance(v, bool):
            parts.append(
                f'<span class="json-lit json-bool">{str(v).lower()}</span>'
            )
        elif isinstance(v, (int, float)):
            parts.append(
                f'<span class="json-lit json-number">{escape(str(v))}</span>'
            )
        elif isinstance(v, str):
            parts.append(
                '<span class="json-lit json-string">'
                f'{escape(json.dumps(v, ensure_ascii=False))}'
                "</span>"
            )
        else:
            parts.append(escape(str(v)))

    def walk(v, ind: str) -> None:
        if isinstance(v, dict):
            if not v:
                parts.append("{}")
                return
            parts.append("{\n")
            items = list(v.items())
            for i, (k, val) in enumerate(items):
                parts.append(ind + "  ")
                parts.append(
                    '<span class="json-key">'
                    f'{escape(json.dumps(k, ensure_ascii=False))}'
                    "</span>"
                )
                parts.append(": ")
                if isinstance(val, (dict, list)):
                    walk(val, ind + "  ")
                else:
                    scalar(val)
                if i < len(items) - 1:
                    parts.append(",")
                parts.append("\n")
            parts.append(ind + "}")
            return
        if isinstance(v, list):
            if not v:
                parts.append("[]")
                return
            parts.append("[\n")
            for i, item in enumerate(v):
                parts.append(ind + "  ")
                if isinstance(item, (dict, list)):
                    walk(item, ind + "  ")
                else:
                    scalar(item)
                if i < len(v) - 1:
                    parts.append(",")
                parts.append("\n")
            parts.append(ind + "]")
            return
        scalar(v)

    walk(obj, "")
    return "".join(parts)


@admin.register(LLMConfig)
class LLMConfigAdmin(admin.ModelAdmin):
    change_list_template = 'admin/llm/llmconfig/change_list.html'
    list_display = ('name', 'sdk', 'model_name', 'is_primary', 'is_active', 'updated_at')
    list_editable = ('is_primary', 'is_active')
    list_filter = ('sdk', 'is_primary', 'is_active')

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "chat-test/",
                self.admin_site.admin_view(self.chat_test_view),
                name="llm_llmconfig_chat_test",
            ),
        ]
        return custom_urls + urls

    def _chat_history_key(self, config_id: int) -> str:
        return f"llm_admin_chat_history_{config_id}"

    def _default_chat_form(self, configs) -> dict:
        first = configs[0] if configs else None
        return {
            "config_id": str(first.pk) if first else "",
            "system_prompt": "You are a helpful assistant.",
            "temperature": "0.2",
            "user_message": "",
        }

    def _trim_history(self, history: list[dict]) -> list[dict]:
        clean = []
        for item in history[-(_CHAT_HISTORY_LIMIT * 2):]:
            role = str(item.get("role") or "").strip().lower()
            content = str(item.get("content") or "").strip()
            if role in {"user", "assistant"} and content:
                clean.append({"role": role, "content": content})
        return clean

    def _build_chat_prompt(self, history: list[dict], user_message: str) -> str:
        cleaned_message = user_message.strip()
        trimmed_history = self._trim_history(history)
        if not trimmed_history:
            return cleaned_message

        transcript: list[str] = [
            "Continue the conversation below.",
            "Use the previous messages as context when they are relevant.",
            "",
        ]
        for item in trimmed_history:
            speaker = "User" if item["role"] == "user" else "Assistant"
            transcript.append(f"{speaker}: {item['content']}")
        transcript.append(f"User: {cleaned_message}")
        transcript.append("Assistant:")
        return "\n".join(transcript)

    def _chat_context(self, request):
        configs = list(LLMConfig.objects.all().order_by('-is_primary', 'pk'))
        stored_form = request.session.get(_CHAT_FORM_STATE_KEY)
        if not isinstance(stored_form, dict):
            stored_form = self._default_chat_form(configs)
        else:
            stored_form = {
                **self._default_chat_form(configs),
                **stored_form,
            }

        selected_config_id = _coerce_int(stored_form.get("config_id"))
        if selected_config_id is None and configs:
            selected_config_id = configs[0].pk
            stored_form["config_id"] = str(selected_config_id)

        selected_config = next((cfg for cfg in configs if cfg.pk == selected_config_id), None)
        history = []
        if selected_config is not None:
            history = self._trim_history(
                request.session.get(self._chat_history_key(selected_config.pk), [])
            )

        result = request.session.pop(_CHAT_RESULT_FLASH_KEY, None)

        return {
            "chat_test_url": reverse("admin:llm_llmconfig_chat_test"),
            "chat_configs": configs,
            "chat_form": stored_form,
            "chat_selected_config_id": selected_config_id,
            "chat_selected_config": selected_config,
            "chat_history": history,
            "chat_result": result if isinstance(result, dict) else None,
        }

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context.update(self._chat_context(request))
        return super().changelist_view(request, extra_context=extra_context)

    def chat_test_view(self, request):
        if request.method != "POST":
            return redirect("admin:llm_llmconfig_changelist")

        configs = list(LLMConfig.objects.all().order_by('-is_primary', 'pk'))
        form = {
            "config_id": str(request.POST.get("config_id", "")).strip(),
            "system_prompt": str(request.POST.get("system_prompt", "")).strip() or "You are a helpful assistant.",
            "temperature": str(request.POST.get("temperature", "0.2")).strip() or "0.2",
            "user_message": str(request.POST.get("user_message", "")).strip(),
        }
        request.session[_CHAT_FORM_STATE_KEY] = form

        selected_config_id = _coerce_int(form["config_id"])
        selected_config = next((cfg for cfg in configs if cfg.pk == selected_config_id), None)
        action = (request.POST.get("_chat_action") or "send").strip().lower()

        if selected_config is None:
            request.session[_CHAT_RESULT_FLASH_KEY] = {
                "success": False,
                "error": "Please choose a valid LLM config first.",
            }
            return redirect("admin:llm_llmconfig_changelist")

        history_key = self._chat_history_key(selected_config.pk)
        history = self._trim_history(request.session.get(history_key, []))

        if action == "clear":
            request.session.pop(history_key, None)
            request.session[_CHAT_RESULT_FLASH_KEY] = {
                "success": True,
                "cleared": True,
                "message": f"Cleared chat history for {selected_config.name}.",
            }
            request.session.modified = True
            return redirect("admin:llm_llmconfig_changelist")

        if not form["user_message"]:
            request.session[_CHAT_RESULT_FLASH_KEY] = {
                "success": False,
                "error": "Type a message before sending.",
            }
            return redirect("admin:llm_llmconfig_changelist")

        prompt = self._build_chat_prompt(history, form["user_message"])
        temperature = _coerce_temperature(form["temperature"])
        started = time.perf_counter()

        try:
            response = _try_one_config(
                selected_config,
                prompt,
                form["system_prompt"],
                temperature=temperature,
                purpose="admin_chat_test",
            )
            duration_ms = int((time.perf_counter() - started) * 1000)
            history.extend(
                [
                    {"role": "user", "content": form["user_message"]},
                    {"role": "assistant", "content": response},
                ]
            )
            request.session[history_key] = self._trim_history(history)
            request.session[_CHAT_FORM_STATE_KEY] = {
                **form,
                "temperature": f"{temperature:g}",
                "user_message": "",
            }
            request.session[_CHAT_RESULT_FLASH_KEY] = {
                "success": True,
                "config_label": f"{selected_config.name} ({selected_config.sdk}:{selected_config.model_name})",
                "duration_ms": duration_ms,
                "message_length": len(response or ""),
            }
            request.session.modified = True
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started) * 1000)
            request.session[_CHAT_RESULT_FLASH_KEY] = {
                "success": False,
                "config_label": f"{selected_config.name} ({selected_config.sdk}:{selected_config.model_name})",
                "duration_ms": duration_ms,
                "error": str(exc),
            }

        return redirect("admin:llm_llmconfig_changelist")


@admin.register(LLMUsage)
class LLMUsageAdmin(admin.ModelAdmin):
    change_list_template = 'llm/usage_change_list.html'
    change_form_template = 'admin/llm/llmusage/change_form.html'
    list_display = (
        'created_at',
        'config_name',
        'model_name',
        'purpose',
        'prompt_tokens',
        'completion_tokens',
        'total_tokens',
        'duration_ms',
        'success',
    )
    list_filter = ('sdk', 'config_name', 'purpose', 'success', 'created_at')
    search_fields = (
        'response_text',
        'duplicate_check_json',
        'duplicate_context_json',
        'purpose',
        'config_name',
        'model_name',
    )
    readonly_fields = (
        'config',
        'config_name',
        'model_name',
        'sdk',
        'prompt_tokens',
        'completion_tokens',
        'total_tokens',
        'purpose',
        'success',
        'duration_ms',
        'created_at',
        'response_display',
        'duplicate_check_display',
        'duplicate_context_display',
    )
    fieldsets = (
        (
            None,
            {
                'fields': (
                    'created_at',
                    'config_name',
                    'model_name',
                    'sdk',
                    'purpose',
                    'success',
                    'duration_ms',
                    'prompt_tokens',
                    'completion_tokens',
                    'total_tokens',
                    'config',
                ),
            },
        ),
        (
            'Full response',
            {
                'description': 'Model output for this call (syntax-highlighted when valid JSON).',
                'fields': ('response_display',),
            },
        ),
        (
            'Duplicate check (extract+dup_check)',
            {
                'description': 'Parsed duplicate_check object stored separately when present.',
                'fields': ('duplicate_check_display',),
            },
        ),
        (
            'Duplicate prompt context',
            {
                'description': 'DB candidates + target-site search results snapshot sent to the combined duplicate check prompt.',
                'fields': ('duplicate_context_display',),
            },
        ),
    )
    date_hierarchy = 'created_at'

    @admin.display(description='Full response')
    def response_display(self, obj):
        if not obj or not (obj.response_text or '').strip():
            return '—'
        raw = obj.response_text.strip()
        try:
            parsed = json.loads(raw)
            inner = _highlight_json_html(parsed)
            return mark_safe(
                '<div class="llm-usage-response-wrap">'
                '<pre class="llm-usage-response-pre llm-json-pre">'
                f"{inner}"
                "</pre></div>"
            )
        except (json.JSONDecodeError, TypeError, ValueError):
            pretty = raw
        inner = escape(pretty)
        return mark_safe(
            '<div class="llm-usage-response-wrap">'
            f'<pre class="llm-usage-response-pre">{inner}</pre>'
            '</div>'
        )

    @admin.display(description='Duplicate check (JSON)')
    def duplicate_check_display(self, obj):
        if not obj or not (obj.duplicate_check_json or '').strip():
            return '—'
        raw = obj.duplicate_check_json.strip()
        try:
            parsed = json.loads(raw)
            inner = _highlight_json_html(parsed)
            return mark_safe(
                '<div class="llm-usage-response-wrap">'
                '<pre class="llm-usage-response-pre llm-json-pre">'
                f"{inner}"
                "</pre></div>"
            )
        except (json.JSONDecodeError, TypeError, ValueError):
            inner = escape(raw)
            return mark_safe(
                '<div class="llm-usage-response-wrap">'
                f'<pre class="llm-usage-response-pre">{inner}</pre>'
                '</div>'
            )

    @admin.display(description='Duplicate prompt context (JSON)')
    def duplicate_context_display(self, obj):
        if not obj or not (obj.duplicate_context_json or '').strip():
            return '—'
        raw = obj.duplicate_context_json.strip()
        try:
            parsed = json.loads(raw)
            inner = _highlight_json_html(parsed)
            return mark_safe(
                '<div class="llm-usage-response-wrap">'
                '<pre class="llm-usage-response-pre llm-json-pre">'
                f"{inner}"
                "</pre></div>"
            )
        except (json.JSONDecodeError, TypeError, ValueError):
            inner = escape(raw)
            return mark_safe(
                '<div class="llm-usage-response-wrap">'
                f'<pre class="llm-usage-response-pre">{inner}</pre>'
                '</div>'
            )

    def has_add_permission(self, request):
        return False

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        now = timezone.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        week_start = today_start - timedelta(days=today_start.weekday())
        month_start = today_start.replace(day=1)

        def _stats(qs):
            agg = qs.aggregate(
                total=Sum('total_tokens'),
                prompt=Sum('prompt_tokens'),
                completion=Sum('completion_tokens'),
                calls=Count('id'),
            )
            return {k: v or 0 for k, v in agg.items()}

        def _fmt_int(n: int) -> str:
            return f"{int(n or 0):,}"

        def _compact_tokens(n: int) -> str:
            """Short form: 1.2M, 30.3K for dashboard hero."""
            n = int(n or 0)
            sign = "-" if n < 0 else ""
            a = abs(n)
            if a >= 1_000_000:
                v = a / 1_000_000
                s = f"{v:.2f}".rstrip("0").rstrip(".")
                return f"{sign}{s}M"
            if a >= 1_000:
                v = a / 1_000
                s = f"{v:.1f}".rstrip("0").rstrip(".")
                return f"{sign}{s}K"
            return f"{sign}{a}"

        def _stats_block(qs):
            s = _stats(qs)
            t, p, c = s["total"], s["prompt"], s["completion"]
            return {
                **s,
                "total_fmt": _fmt_int(t),
                "prompt_fmt": _fmt_int(p),
                "completion_fmt": _fmt_int(c),
                "total_compact": _compact_tokens(t),
                "prompt_compact": _compact_tokens(p),
                "completion_compact": _compact_tokens(c),
            }

        extra_context["usage_today"] = _stats_block(
            LLMUsage.objects.filter(created_at__gte=today_start)
        )
        extra_context["usage_week"] = _stats_block(
            LLMUsage.objects.filter(created_at__gte=week_start)
        )
        extra_context["usage_month"] = _stats_block(
            LLMUsage.objects.filter(created_at__gte=month_start)
        )

        return super().changelist_view(request, extra_context=extra_context)
