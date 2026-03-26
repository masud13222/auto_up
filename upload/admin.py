from django.contrib import admin
from django.db import connection
from django.db.models import IntegerField
from django.db.models.expressions import RawSQL
from django_q.models import OrmQ

from .models import MediaTask


@admin.register(OrmQ)
class OrmQQueueAdmin(admin.ModelAdmin):
    """
    django-q ORM broker queue under **Django Q** in admin (OrmQ._meta.app_label).
    Adds DB column ``priority`` (upload.0007) for dequeue order — not on stock OrmQ model.
    """

    list_display = ("id", "q_priority", "key", "lock")
    list_filter = ("key",)
    search_fields = ("payload",)
    readonly_fields = ("id", "key", "payload", "lock")

    @admin.display(description="Priority", ordering="_qpri")
    def q_priority(self, obj):
        v = getattr(obj, "_qpri", None)
        return 0 if v is None else v

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        qn = connection.ops.quote_name
        t = qn(OrmQ._meta.db_table)
        p = qn("priority")
        # COALESCE: old rows / pre-migration safety
        sql = f"COALESCE({t}.{p}, 0)"
        return qs.annotate(
            _qpri=RawSQL(sql, [], output_field=IntegerField()),
        ).order_by("-_qpri", "id")

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request):
        return False

    def has_delete_permission(self, request):
        return request.user.is_superuser


@admin.register(MediaTask)
class MediaTaskAdmin(admin.ModelAdmin):
    list_display = ["title", "content_type", "status", "url", "created_at", "updated_at"]
    list_filter = ["status", "content_type"]
    search_fields = ["title", "url"]
    readonly_fields = ["task_id", "result", "created_at", "updated_at"]
    ordering = ["-created_at"]
