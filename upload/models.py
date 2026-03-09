from django.db import models


class MediaTask(models.Model):
    """
    Tracks each media processing task (Movie or TV Show).
    Prevents duplicate processing and stores final results.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    CONTENT_TYPE_CHOICES = [
        ('movie', 'Movie'),
        ('tvshow', 'TV Show'),
    ]

    url = models.URLField(max_length=500, db_index=True)
    title = models.CharField(max_length=500, blank=True, default='')
    website_title = models.CharField(max_length=1000, blank=True, default='', db_index=True)
    content_type = models.CharField(max_length=10, choices=CONTENT_TYPE_CHOICES, blank=True, default='')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    result = models.JSONField(null=True, blank=True)
    task_id = models.CharField(max_length=100, blank=True, default='')
    error_message = models.TextField(blank=True, default='')
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
