from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import BackupSettings


@receiver(post_save, sender=BackupSettings, dispatch_uid="settings.sync_backup_schedule")
def sync_backup_schedule(sender, **kwargs):
    from settings.scheduler import ensure_backup_schedule

    ensure_backup_schedule()
