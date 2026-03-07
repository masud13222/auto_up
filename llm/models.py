from django.db import models

# Create your models here.
class LLMSettings(models.Model):
    api_key = models.CharField(max_length=255, help_text="Enter your OpenAI API Key")
    base_url = models.URLField(max_length=255, default="https://api.openai.com/v1", help_text="Optional: Custom base URL for proxies or local models")
    model_name = models.CharField(max_length=100, default="gpt-4o", help_text="e.g., gpt-4o, gpt-3.5-turbo")
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "LLM Settings"
        verbose_name_plural = "LLM Settings"

    def save(self, *args, **kwargs):
        self.pk = 1
        super(LLMSettings, self).save(*args, **kwargs)

    def __str__(self):
        return f"Global LLM Config ({self.model_name})"
