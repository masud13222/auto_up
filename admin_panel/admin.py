from django.contrib import admin

from .models import BackupSettings


@admin.register(BackupSettings)
class BackupSettingsAdmin(admin.ModelAdmin):
    list_display = (
        "is_enabled",
        "frequency",
        "daily_run_at",
        "interval_hours",
        "drive_backup_folder_id",
        "last_backup_at",
        "last_backup_ok",
        "updated_at",
    )
    readonly_fields = ("last_backup_at", "last_backup_ok", "last_backup_note", "created_at", "updated_at")
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "is_enabled",
                    "google_config",
                    "drive_backup_folder_id",
                )
            },
        ),
        (
            "Schedule",
            {
                "fields": (
                    "frequency",
                    "daily_run_at",
                    "interval_hours",
                ),
                "description": "Define when backups should run. Wire django-q Schedule or cron to your backup task to match this.",
            },
        ),
        (
            "Last run (read-only)",
            {
                "fields": ("last_backup_at", "last_backup_ok", "last_backup_note"),
            },
        ),
        (
            "Meta",
            {"fields": ("created_at", "updated_at")},
        ),
    )

    def has_add_permission(self, request):
        if BackupSettings.objects.exists():
            return False
        return super().has_add_permission(request)

    def has_delete_permission(self, request, obj=None):
        return False

    def save_model(self, request, obj, form, change):
        obj.full_clean()
        super().save_model(request, obj, form, change)
