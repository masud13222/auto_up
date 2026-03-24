from django.contrib import admin
from .models import GoogleConfig, UploadSettings, FlixBDSettings

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
