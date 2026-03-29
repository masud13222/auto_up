import json

from django.contrib import admin
from django.db import connection
from django.db.models import IntegerField, Q
from django.db.models.expressions import RawSQL
from django.utils.html import escape
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _
from django_q.models import OrmQ

from .models import MediaTask


class HasExtraUrlsFilter(admin.SimpleListFilter):
    title = _("Extra URLs")
    parameter_name = "has_extra_urls"

    def lookups(self, request, model_admin):
        return (
            ("yes", _("Has extra URLs")),
            ("no", _("No extra URLs")),
        )

    def queryset(self, request, queryset):
        v = self.value()
        empty_q = Q(extra_urls=[]) | Q(extra_urls__isnull=True)
        if v == "yes":
            return queryset.exclude(empty_q)
        if v == "no":
            return queryset.filter(empty_q)
        return queryset

# django_q registers OrmQ in django_q.admin; replace with priority-aware admin.
if admin.site.is_registered(OrmQ):
    admin.site.unregister(OrmQ)


@admin.register(OrmQ)
class OrmQQueueAdmin(admin.ModelAdmin):
    """
    django-q ORM broker queue under **Django Q** in admin (OrmQ._meta.app_label).
    Adds DB column ``priority`` (upload.0007) for dequeue order — not on stock OrmQ model.
    """

    change_form_template = "admin/django_q/ormq/change_form.html"
    list_display = (
        "id",
        "q_cluster_task_id",
        "q_priority",
        "target_url",
        "key",
        "lock",
    )
    list_display_links = ("id", "target_url")
    list_filter = ("key",)
    search_fields = ("payload",)
    readonly_fields = (
        "id",
        "key",
        "lock",
        "q_priority",
        "q_cluster_task_id",
        "target_url",
        "decoded_task_detail",
    )
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "id",
                    "q_cluster_task_id",
                    "q_priority",
                    "target_url",
                    "key",
                    "lock",
                ),
            },
        ),
        (
            "Decoded task",
            {
                "description": "Signed payload unpacked (read-only).",
                "fields": ("decoded_task_detail",),
            },
        ),
    )

    @admin.display(description="Priority", ordering="_qpri")
    def q_priority(self, obj):
        v = getattr(obj, "_qpri", None)
        return 0 if v is None else v

    @admin.display(description="Task ID", ordering="id")
    def q_cluster_task_id(self, obj):
        try:
            t = obj.task
            if not isinstance(t, dict):
                return "—"
            tid = t.get("id")
            return tid if tid is not None else "—"
        except Exception:
            return "—"

    @admin.display(description="URL / target")
    def target_url(self, obj):
        try:
            t = obj.task
            if not isinstance(t, dict):
                return "—"
            name = (t.get("name") or "").strip()
            func = t.get("func")
            args = t.get("args") or ()
            func_s = str(func) if func is not None else ""
            if args and "process_media_task" in func_s:
                pk = args[0]
                url = MediaTask.objects.filter(pk=pk).values_list("url", flat=True).first()
                if url:
                    short = url if len(url) <= 96 else f"{url[:93]}…"
                    return mark_safe(
                        '<span class="ormq-target-url" title="{}">{}</span>'.format(
                            escape(url),
                            escape(short),
                        )
                    )
            return name or "—"
        except Exception:
            return "—"

    @admin.display(description="Decoded package")
    def decoded_task_detail(self, obj):
        try:
            t = obj.task
            if not isinstance(t, dict):
                return "—"
            safe = {
                "id": t.get("id"),
                "name": t.get("name"),
                "func": t.get("func"),
                "args": t.get("args"),
                "kwargs": t.get("kwargs"),
                "q_priority": t.get("q_priority"),
            }
            text = json.dumps(safe, indent=2, ensure_ascii=False, default=str)
            return mark_safe(
                '<pre class="ormq-decoded-pre">{}</pre>'.format(escape(text))
            )
        except Exception as e:
            return escape(str(e))

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        qn = connection.ops.quote_name
        t = qn(OrmQ._meta.db_table)
        p = qn("priority")
        sql = f"COALESCE({t}.{p}, 0)"
        return qs.annotate(
            _qpri=RawSQL(sql, [], output_field=IntegerField()),
        ).order_by("-_qpri", "id")

    def save_model(self, request, obj, form, change):
        # Queue rows are broker-owned; admin is view-only.
        pass

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return request.user.is_staff

    def has_delete_permission(self, request, obj=None):
        return request.user.is_superuser


@admin.register(MediaTask)
class MediaTaskAdmin(admin.ModelAdmin):
    change_list_template = "admin/upload/mediatask/change_list.html"
    list_display = [
        "title",
        "content_type",
        "status",
        "url",
        "extra_urls_badge",
        "created_at",
        "updated_at",
    ]
    list_filter = ["status", "content_type", HasExtraUrlsFilter]
    search_fields = ["title", "url"]
    readonly_fields = ["task_id", "result", "site_sync_snapshot", "created_at", "updated_at", "extra_urls"]
    ordering = ["-created_at"]

    @admin.display(description=_("Extra URLs"))
    def extra_urls_badge(self, obj):
        urls = obj.extra_urls or []
        n = len(urls)
        if n == 0:
            return "—"
        return f"Yes ({n})"

    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context["mt_failed_total"] = MediaTask.objects.filter(status="failed").count()
        _empty_extra = Q(extra_urls=[]) | Q(extra_urls__isnull=True)
        extra_context["mt_extra_urls_total"] = MediaTask.objects.exclude(_empty_extra).count()
        return super().changelist_view(request, extra_context=extra_context)
