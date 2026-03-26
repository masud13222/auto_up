from django.contrib import admin
from .models import BackupSettings, GoogleConfig, UploadSettings, FlixBDSettings

# Register your models here.
@admin.register(GoogleConfig)
class GoogleConfigAdmin(admin.ModelAdmin):
    list_display = ('name', 'updated_at', 'created_at')
    search_fields = ('name',)

@admin.register(UploadSettings)
class UploadSettingsAdmin(admin.ModelAdmin):
    list_display = ('upload_folder_id', 'updated_at')
    
    def has_add_permission(self, request):
        # Only allow 1 instance
        if self.model.objects.exists():
            return False
        return super().has_add_permission(request)

    def has_delete_permission(self, request, obj=None):
        return False # Do not allow deleting the config once created


@admin.register(FlixBDSettings)
class FlixBDSettingsAdmin(admin.ModelAdmin):
    list_display = ('api_url', 'is_enabled', 'updated_at')

    def has_add_permission(self, request):
        if self.model.objects.exists():
            return False
        return super().has_add_permission(request)

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(BackupSettings)
class BackupSettingsAdmin(admin.ModelAdmin):
    list_display = ("is_enabled", "google_config", "drive_backup_folder_id", "updated_at")
    readonly_fields = ("created_at", "updated_at")
    fieldsets = (
        (
            None,
            {
                "fields": ("is_enabled", "google_config", "drive_backup_folder_id"),
                "description": (
                    "When enabled, folder ID is set, and a GoogleConfig exists (picked or any), "
                    "django-q creates a daily task named settings.db_backup "
                    "(Admin → Django Q → Scheduled tasks). "
                    "First next_run is 24 hours after the schedule is created. "
                    "Backup run history: Django Q → Successful tasks / Failures."
                ),
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
