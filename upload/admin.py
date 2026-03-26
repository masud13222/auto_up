from django.contrib import admin

from .django_q_priority import OrmQWithPriority
from .models import MediaTask


@admin.register(OrmQWithPriority)
class OrmQWithPriorityAdmin(admin.ModelAdmin):
    """
    Same table as django-q ORM queue (`django_q_ormq`) — includes `priority` (higher dequeued first).
    Django-Q's built-in admin does not show this column.
    """

    list_display = ("id", "priority", "key", "lock", "payload_preview")
    list_filter = ("key", "priority")
    ordering = ("-priority", "id")
    readonly_fields = ("id", "key", "payload", "lock", "priority")
    search_fields = ("payload",)

    @admin.display(description="Payload (preview)")
    def payload_preview(self, obj):
        p = obj.payload or ""
        return (p[:160] + "…") if len(p) > 160 else p

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request):
        return False

    def has_delete_permission(self, request):
        return request.user.is_superuser


@admin.register(MediaTask)
class MediaTaskAdmin(admin.ModelAdmin):
    list_display = ['title', 'content_type', 'status', 'url', 'created_at', 'updated_at']
    list_filter = ['status', 'content_type']
    search_fields = ['title', 'url']
    readonly_fields = ['task_id', 'result', 'created_at', 'updated_at']
    ordering = ['-created_at']
