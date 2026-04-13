from django.contrib import admin

from auto_up.models import AutoUpSkipUrl, ScrapeItem, ScrapeRun


class ScrapeItemInline(admin.TabularInline):
    model = ScrapeItem
    readonly_fields = ('raw_title', 'clean_name', 'year', 'url', 'action', 'reason', 'llm_priority', 'media_task_pk', 'created_at')
    extra = 0
    can_delete = False

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(ScrapeRun)
class ScrapeRunAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'status',
        'total_scraped',
        'skip_list_skipped',
        'url_skipped',
        'daily_limit_skipped',
        'llm_approved',
        'llm_skipped',
        'queued',
        'duration_seconds',
        'started_at',
    )
    list_filter = ('status',)
    readonly_fields = (
        'status',
        'total_scraped',
        'skip_list_skipped',
        'url_skipped',
        'daily_limit_skipped',
        'llm_approved',
        'llm_skipped',
        'queued',
        'error_message',
        'duration_seconds',
        'started_at',
        'finished_at',
    )
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


@admin.register(AutoUpSkipUrl)
class AutoUpSkipUrlAdmin(admin.ModelAdmin):
    list_display = ('id', 'short_url', 'note', 'normalized_url', 'updated_at', 'created_at')
    list_display_links = ('id', 'short_url')
    search_fields = ('url', 'normalized_url', 'note')
    readonly_fields = ('normalized_url', 'created_at', 'updated_at')
    ordering = ('-updated_at',)
    fieldsets = (
        (None, {'fields': ('url', 'note')}),
        ('System', {'fields': ('normalized_url', 'created_at', 'updated_at')}),
    )

    def short_url(self, obj):
        u = obj.url or ''
        return u[:72] + ('...' if len(u) > 72 else '')

    short_url.short_description = 'URL'
