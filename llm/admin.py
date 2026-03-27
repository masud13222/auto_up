import json

from django.contrib import admin
from django.db.models import Sum, Count
from django.utils import timezone
from django.utils.html import escape
from django.utils.safestring import mark_safe
from datetime import timedelta
from .models import LLMConfig, LLMUsage


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
    list_display = ('name', 'sdk', 'model_name', 'is_primary', 'is_active', 'updated_at')
    list_editable = ('is_primary', 'is_active')
    list_filter = ('sdk', 'is_primary', 'is_active')


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
