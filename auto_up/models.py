from django.db import models


class ScrapeRun(models.Model):
    """
    Tracks each auto-scrape execution.
    One record per scheduled/manual run.
    Auto-cleaned after 7 days.
    """
    STATUS_CHOICES = [
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='running')
    total_scraped = models.PositiveIntegerField(default=0, help_text="Total entries found on homepage")
    url_skipped = models.PositiveIntegerField(default=0, help_text="Skipped because URL already in MediaTask")
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
    - Daily limit enforcement (max 2 process per URL per day)
    - Audit trail / debugging
    - Auto-cleaned after 7 days.
    """
    ACTION_CHOICES = [
        ('process', 'Queued for Processing'),
        ('skip_url_exists', 'Skipped — URL already in DB'),
        ('skip_daily_limit', 'Skipped — Daily limit reached'),
        ('skip_llm', 'Skipped — LLM decision'),
        ('skip_race', 'Skipped — Race condition'),
        ('skip_no_title', 'Skipped — No title extracted'),
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
