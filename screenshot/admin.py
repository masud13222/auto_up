from django.contrib import admin

from screenshot.models import ScreenshotSettings


@admin.register(ScreenshotSettings)
class ScreenshotSettingsAdmin(admin.ModelAdmin):
    list_display = ("is_enabled", "worker_base_url", "telegram_chat_id", "updated_at")
    fields = (
        "is_enabled",
        "worker_base_url",
        "telegram_bot_token",
        "telegram_chat_id",
        "crypto_phrase",
    )
