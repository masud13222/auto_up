from django.db.models.signals import post_save
from django.dispatch import receiver

from admin_panel.models import BackupSettings


@receiver(post_save, sender=BackupSettings, dispatch_uid="admin_panel.sync_backup_schedule")
def sync_backup_schedule(sender, **kwargs):
    from admin_panel.scheduler import ensure_backup_schedule

    ensure_backup_schedule()
