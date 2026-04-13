from django.core.exceptions import ValidationError
from django.db import models

from auto_up.url_match import canonical_skip_url


class ScrapeRun(models.Model):
    """
    Tracks each auto-scrape execution.
    One record per scheduled/manual run.
    Auto-cleaned after a few days (see ``LOG_RETENTION_DAYS`` in ``auto_up.tasks``).
    """
    STATUS_CHOICES = [
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='running')
    total_scraped = models.PositiveIntegerField(default=0, help_text="Total entries found on homepage")
    url_skipped = models.PositiveIntegerField(default=0, help_text="Skipped because URL already in MediaTask")
    skip_list_skipped = models.PositiveIntegerField(
        default=0,
        help_text="Skipped because URL is in the manual auto-up skip list",
    )
    daily_limit_skipped = models.PositiveIntegerField(default=0, help_text="Skipped because already processed today")
    llm_approved = models.PositiveIntegerField(default=0, help_text="Approved by LLM for processing")
    llm_skipped = models.PositiveIntegerField(default=0, help_text="Skipped by LLM decision")
    queued = models.PositiveIntegerField(default=0, help_text="Actually queued for processing")
    error_message = models.TextField(blank=True, default='')
    duration_seconds = models.FloatField(null=True, blank=True)
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        verbose_name = "Scrape Run"
        verbose_name_plural = "Scrape Runs"
        ordering = ['-started_at']

    def __str__(self):
        return f"[{self.status}] {self.started_at:%Y-%m-%d %H:%M} — scraped={self.total_scraped}, queued={self.queued}"


class ScrapeItem(models.Model):
    """
    Tracks each individual item found during a scrape run.
    Used for:
    - Daily limit enforcement (max 2 process per URL per day; bypassed 22:00–01:00 Asia/Dhaka)
    - Audit trail / debugging
    - Auto-cleaned after a few days (see ``LOG_RETENTION_DAYS`` in ``auto_up.tasks``).
    """
    ACTION_CHOICES = [
        ('process', 'Queued for Processing'),
        ('skip_url_exists', 'Skipped — URL already in DB'),
        ('skip_daily_limit', 'Skipped — Daily limit reached'),
        ('skip_llm', 'Skipped — LLM decision'),
        ('skip_race', 'Skipped — Race condition'),
        ('skip_no_title', 'Skipped — No title extracted'),
        ('skip_skip_list', 'Skipped — URL in manual skip list'),
    ]

    run = models.ForeignKey(ScrapeRun, on_delete=models.CASCADE, related_name='items')
    raw_title = models.CharField(max_length=500)
    clean_name = models.CharField(max_length=500, blank=True, default='')
    year = models.CharField(max_length=10, blank=True, default='')
    url = models.URLField(max_length=500)
    action = models.CharField(max_length=30, choices=ACTION_CHOICES)
    reason = models.CharField(max_length=500, blank=True, default='')
    llm_priority = models.CharField(max_length=10, blank=True, default='')
    media_task_pk = models.IntegerField(null=True, blank=True, help_text="PK of created MediaTask if queued")
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Scrape Item"
        verbose_name_plural = "Scrape Items"
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['url', 'action', 'created_at']),
        ]

    def __str__(self):
        return f"[{self.action}] {self.raw_title[:60]}"


class AutoUpSkipUrl(models.Model):
    """
    URLs the auto-scrape pipeline must never queue (manual blocklist).
    Compared using a canonical form so minor formatting differences still match.
    """

    url = models.URLField(max_length=500, help_text="Full page URL to skip during auto-up scrapes")
    normalized_url = models.CharField(
        max_length=600,
        unique=True,
        db_index=True,
        editable=False,
        help_text="Canonical form used for matching (set automatically)",
    )
    note = models.CharField(max_length=300, blank=True, default="", help_text="Optional note (why this URL is skipped)")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Auto-up skip URL"
        verbose_name_plural = "Auto-up skip URLs"
        ordering = ["-updated_at"]

    def __str__(self):
        return self.normalized_url[:80] or str(self.pk)

    def clean(self):
        super().clean()
        # Set early so ``validate_unique`` (full_clean) sees the real key, not an empty string.
        self.normalized_url = canonical_skip_url(self.url or "")
        if not self.normalized_url:
            raise ValidationError({"url": "Enter a valid URL."})

    def save(self, *args, **kwargs):
        self.normalized_url = canonical_skip_url(self.url or "")
        update_fields = kwargs.get("update_fields")
        if update_fields is not None:
            kwargs["update_fields"] = list(set(update_fields) | {"normalized_url"})
        super().save(*args, **kwargs)
