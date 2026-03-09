from django.contrib import admin
from auto_up.models import ScrapeRun, ScrapeItem


class ScrapeItemInline(admin.TabularInline):
    model = ScrapeItem
    readonly_fields = ('raw_title', 'clean_name', 'year', 'url', 'action', 'reason', 'llm_priority', 'media_task_pk', 'created_at')
    extra = 0
    can_delete = False

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(ScrapeRun)
class ScrapeRunAdmin(admin.ModelAdmin):
    list_display = ('id', 'status', 'total_scraped', 'daily_limit_skipped', 'llm_approved', 'llm_skipped', 'queued', 'duration_seconds', 'started_at')
    list_filter = ('status',)
    readonly_fields = ('status', 'total_scraped', 'daily_limit_skipped', 'llm_approved', 'llm_skipped', 'queued', 'error_message', 'duration_seconds', 'started_at', 'finished_at')
    inlines = [ScrapeItemInline]
    ordering = ('-started_at',)

    def has_add_permission(self, request):
        return False


@admin.register(ScrapeItem)
class ScrapeItemAdmin(admin.ModelAdmin):
    list_display = ('id', 'short_title', 'action', 'reason', 'llm_priority', 'url', 'created_at')
    list_filter = ('action', 'llm_priority')
    search_fields = ('raw_title', 'clean_name', 'url')
    readonly_fields = ('run', 'raw_title', 'clean_name', 'year', 'url', 'action', 'reason', 'llm_priority', 'media_task_pk', 'created_at')
    ordering = ('-created_at',)

    def short_title(self, obj):
        return obj.raw_title[:60] + ('...' if len(obj.raw_title) > 60 else '')
    short_title.short_description = 'Title'

    def has_add_permission(self, request):
        return False
