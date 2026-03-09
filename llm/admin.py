from django.contrib import admin
from django.db.models import Sum, Count
from django.utils import timezone
from datetime import timedelta
from .models import LLMConfig, LLMUsage


@admin.register(LLMConfig)
class LLMConfigAdmin(admin.ModelAdmin):
    list_display = ('name', 'sdk', 'model_name', 'is_primary', 'is_active', 'updated_at')
    list_editable = ('is_primary', 'is_active')
    list_filter = ('sdk', 'is_primary', 'is_active')


@admin.register(LLMUsage)
class LLMUsageAdmin(admin.ModelAdmin):
    change_list_template = 'llm/usage_change_list.html'
    list_display = ('created_at', 'config_name', 'model_name', 'purpose', 'prompt_tokens', 'completion_tokens', 'total_tokens', 'duration_ms', 'success')
    list_filter = ('sdk', 'config_name', 'purpose', 'success', 'created_at')
    readonly_fields = ('config', 'config_name', 'model_name', 'sdk', 'prompt_tokens', 'completion_tokens', 'total_tokens', 'purpose', 'success', 'duration_ms', 'created_at')
    date_hierarchy = 'created_at'

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

        extra_context['usage_today'] = _stats(LLMUsage.objects.filter(created_at__gte=today_start))
        extra_context['usage_week'] = _stats(LLMUsage.objects.filter(created_at__gte=week_start))
        extra_context['usage_month'] = _stats(LLMUsage.objects.filter(created_at__gte=month_start))

        return super().changelist_view(request, extra_context=extra_context)
