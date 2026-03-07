from django.contrib import admin
from .models import LLMSettings

# Register your models here.
@admin.register(LLMSettings)
class LLMSettingsAdmin(admin.ModelAdmin):
    list_display = ('model_name', 'base_url', 'updated_at')
    
    def has_add_permission(self, request):
        if self.model.objects.exists():
            return False
        return super().has_add_permission(request)

    def has_delete_permission(self, request, obj=None):
        return False
