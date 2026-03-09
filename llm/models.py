from django.db import models


SDK_CHOICES = [
    ('openai', 'OpenAI Compatible'),
    ('google', 'Google Gemini'),
    ('mistral', 'Mistral AI'),
]


class LLMConfig(models.Model):
    """
    Multiple LLM provider configs with priority-based fallback.
    Primary config is tried first; on failure, others are tried in order.
    """
    name = models.CharField(max_length=100, help_text="Label for this config, e.g. 'OpenRouter Free'")
    sdk = models.CharField(max_length=20, choices=SDK_CHOICES, default='openai', help_text="Which SDK to use")
    base_url = models.CharField(max_length=500, blank=True, default='', help_text="API base URL (required for openai-compatible, optional for others)")
    api_key = models.CharField(max_length=500, help_text="API key for this provider")
    model_name = models.CharField(max_length=200, help_text="Model name, e.g. gpt-4o, gemini-2.0-flash, mistral-large-latest")
    is_primary = models.BooleanField(default=False, help_text="Primary config is tried first")
    is_active = models.BooleanField(default=True, help_text="Inactive configs are skipped")
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "LLM Config"
        verbose_name_plural = "LLM Configs"
        ordering = ['-is_primary', 'pk']

    def save(self, *args, **kwargs):
        # If this is set as primary, unset all others
        if self.is_primary:
            LLMConfig.objects.exclude(pk=self.pk).update(is_primary=False)
        super().save(*args, **kwargs)

    def __str__(self):
        primary = " ★" if self.is_primary else ""
        active = "" if self.is_active else " (disabled)"
        return f"{self.name} [{self.sdk}:{self.model_name}]{primary}{active}"


class LLMUsage(models.Model):
    """Track token usage for each LLM API call."""
    config = models.ForeignKey(LLMConfig, on_delete=models.SET_NULL, null=True, blank=True, related_name='usage_logs')
    config_name = models.CharField(max_length=100, help_text="Snapshot of config name at call time")
    model_name = models.CharField(max_length=200, help_text="Model used for this call")
    sdk = models.CharField(max_length=20, help_text="SDK used (openai/google/mistral)")

    prompt_tokens = models.IntegerField(default=0)
    completion_tokens = models.IntegerField(default=0)
    total_tokens = models.IntegerField(default=0)

    purpose = models.CharField(max_length=100, blank=True, default='', help_text="What this call was for (extract, duplicate, filename, etc.)")
    success = models.BooleanField(default=True)
    duration_ms = models.IntegerField(default=0, help_text="Call duration in milliseconds")

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        verbose_name = "LLM Usage"
        verbose_name_plural = "LLM Usage"
        ordering = ['-created_at']

    def __str__(self):
        return f"{self.config_name} | {self.total_tokens} tokens | {self.created_at:%Y-%m-%d %H:%M}"

