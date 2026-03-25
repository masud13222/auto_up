from django.db import models


class ScreenshotSettings(models.Model):
    """
    Singleton: Telegram + Worker for publishing frame grabs to FlixBD.
    crypto_phrase must match Cloudflare Worker secret CRYPTO_PHRASE (see screenshot/worker.js).
    """

    is_enabled = models.BooleanField(
        default=True,
        help_text="If off, pipeline skips capture (no screenshots sent to FlixBD).",
    )
    worker_base_url = models.URLField(
        max_length=500,
        blank=True,
        help_text="Worker origin, no trailing slash (e.g. https://tg-image-proxy.xxx.workers.dev)",
    )
    telegram_bot_token = models.CharField(max_length=256, blank=True)
    telegram_chat_id = models.CharField(
        max_length=32,
        blank=True,
        help_text="Group or user id (e.g. -1001234567890)",
    )
    crypto_phrase = models.CharField(
        max_length=256,
        default="FLixBD-image-host-crypto-v1",
        help_text="AES-GCM phrase; must match Worker wrangler secret CRYPTO_PHRASE",
    )
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Screenshot settings"
        verbose_name_plural = "Screenshot settings"

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)

    def __str__(self):
        return "Screenshot settings"

    @classmethod
    def get_solo(cls):
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj
