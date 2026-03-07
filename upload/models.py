from django.db import models


class MovieTask(models.Model):
    """
    Tracks each movie processing task.
    Prevents duplicate processing and stores final results.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    url = models.URLField(max_length=500, db_index=True)
    title = models.CharField(max_length=500, blank=True, default='')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    result = models.JSONField(null=True, blank=True)
    task_id = models.CharField(max_length=100, blank=True, default='')
    error_message = models.TextField(blank=True, default='')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Movie Task"
        verbose_name_plural = "Movie Tasks"
        ordering = ['-created_at']

    def __str__(self):
        return f"[{self.status}] {self.title or self.url[:50]}"
