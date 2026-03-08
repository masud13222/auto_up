from django.contrib import admin
from .models import LLMConfig


@admin.register(LLMConfig)
class LLMConfigAdmin(admin.ModelAdmin):
    list_display = ('name', 'sdk', 'model_name', 'is_primary', 'is_active', 'updated_at')
    list_editable = ('is_primary', 'is_active')
    list_filter = ('sdk', 'is_primary', 'is_active')
