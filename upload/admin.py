from django.contrib import admin
from .models import MediaTask


@admin.register(MediaTask)
class MediaTaskAdmin(admin.ModelAdmin):
    list_display = ['title', 'status', 'url', 'created_at', 'updated_at']
    list_filter = ['status']
    search_fields = ['title', 'url']
    readonly_fields = ['task_id', 'result', 'created_at', 'updated_at']
    ordering = ['-created_at']
