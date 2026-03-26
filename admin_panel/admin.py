from django.contrib import admin

from .models import BackupSettings


@admin.register(BackupSettings)
class BackupSettingsAdmin(admin.ModelAdmin):
    list_display = (
        "is_enabled",
        "google_config",
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
                "fields": ("is_enabled", "google_config", "drive_backup_folder_id"),
                "description": (
                    "When enabled, folder ID is set, and a GoogleConfig exists (picked or any), "
                    "django-q creates a daily task named admin_panel.db_backup "
                    "(Admin → Django Q → Scheduled tasks). "
                    "First next_run is 24 hours after the schedule is created (no immediate backup on cold start)."
                ),
            },
        ),
        (
            "Last run",
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
