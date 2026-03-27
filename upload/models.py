from django.db import models


class MediaTask(models.Model):
    """
    Tracks each media processing task (Movie or TV Show).
    Prevents duplicate processing and stores final results.

    A single MediaTask can aggregate content from MULTIPLE source URLs.
    This happens when a TV show releases new episode batches under a new URL
    (e.g. episodes 1-72 at URL-A, then episodes 73-80 at URL-B).
    The primary `url` is the first one found; subsequent URLs go into `extra_urls`.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('partial', 'Partial'),
        ('failed', 'Failed'),
    ]

    CONTENT_TYPE_CHOICES = [
        ('movie', 'Movie'),
        ('tvshow', 'TV Show'),
    ]

    url = models.URLField(max_length=500, db_index=True)
    extra_urls = models.JSONField(
        default=list,
        blank=True,
        help_text=(
            "Additional source URLs for this content. "
            "When a new URL is detected for the same TV show (e.g. new episode batch), "
            "it is appended here instead of creating a duplicate task."
        ),
    )
    title = models.CharField(max_length=500, blank=True, default='')
    website_title = models.CharField(max_length=1000, blank=True, default='', db_index=True)
    content_type = models.CharField(max_length=10, choices=CONTENT_TYPE_CHOICES, blank=True, default='')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    result = models.JSONField(null=True, blank=True)
    task_id = models.CharField(max_length=100, blank=True, default='')
    error_message = models.TextField(blank=True, default='')
    site_content_id = models.PositiveIntegerField(
        null=True,
        blank=True,
        db_index=True,
        help_text="ID of the published content on the target site (FlixBD). Null means not yet published."
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Media Task"
        verbose_name_plural = "Media Tasks"
        ordering = ['-created_at']
        # Keep old DB table name to avoid data loss
        db_table = 'upload_movietask'

    def __str__(self):
        return f"[{self.status}] {self.title or self.url[:50]}"

    def add_extra_url(self, new_url: str) -> bool:
        """
        Register a new source URL for this task (if not already tracked).
        Returns True if the URL was added, False if it was already known.
        """
        urls = self.extra_urls or []
        if new_url == self.url or new_url in urls:
            return False
        urls.append(new_url)
        self.extra_urls = urls
        return True

    def all_urls(self) -> list:
        """Return primary URL + all extra URLs."""
        return [self.url] + (self.extra_urls or [])
