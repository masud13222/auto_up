"""
ORM broker extension: dequeue ordered by q_priority (higher first), then id.

Usage:
    async_task(..., q_options={"q_priority": 10})

Requires migration upload.0007 (adds django_q_ormq.priority) and Q_CLUSTER["broker_class"].
"""

from __future__ import annotations

import logging
from contextvars import ContextVar, Token
from time import sleep

from django import db
from django.db import models, transaction
from django.db.models import QuerySet
from django.utils import timezone
from django.dispatch import receiver

from django_q.brokers.orm import ORM
from django_q.conf import Conf, logger
from django_q.signals import pre_enqueue
from django_q.signing import SignedPackage

log = logging.getLogger(__name__)

_pending_q_priority: ContextVar[int | None] = ContextVar("django_q_pending_q_priority", default=None)

_installed = False

_MAX_Q_PRIORITY = 999_999


def parse_q_priority(value: str | None, *, default: int = 0) -> int:
    """Clamp queue priority for async_task q_options (0 … 999999)."""
    if value is None:
        return default
    s = str(value).strip()
    if not s:
        return default
    try:
        p = int(s)
    except (TypeError, ValueError):
        return default
    return max(0, min(p, _MAX_Q_PRIORITY))


class OrmQWithPriority(models.Model):
    """Shadow django_q OrmQ row including priority column (managed=False)."""

    id = models.BigAutoField(primary_key=True)
    key = models.CharField(max_length=100)
    payload = models.TextField()
    lock = models.DateTimeField(null=True)
    priority = models.IntegerField(default=0)

    class Meta:
        app_label = "upload"
        db_table = "django_q_ormq"
        managed = False


class PriorityORM(ORM):
    @staticmethod
    def get_connection(list_key: str = None) -> QuerySet:
        if transaction.get_autocommit(using=Conf.ORM):
            db.close_old_connections()
        else:
            logger.debug("Broker in an atomic transaction")
        return OrmQWithPriority.objects.using(Conf.ORM)

    def enqueue(self, task: str) -> int:
        priority = 0
        try:
            data = SignedPackage.loads(task)
            priority = int(data.get("q_priority", 0))
        except Exception:
            pass
        row = self.get_connection().create(
            key=self.list_key or Conf.CLUSTER_NAME,
            payload=task,
            lock=timezone.now(),
            priority=priority,
        )
        return row.pk

    def dequeue(self):
        tasks = (
            self.get_connection()
            .filter(key=self.list_key, lock__lt=timezone.now())
            .order_by("-priority", "id")[: Conf.BULK]  # noqa: E203
        )
        if tasks:
            task_list = []
            for task in tasks:
                if (
                    self.get_connection()
                    .filter(id=task.id, lock=task.lock)
                    .update(lock=self.timeout(task))
                ):
                    task_list.append((task.pk, task.payload))
            return task_list
        sleep(Conf.POLL)


@receiver(pre_enqueue)
def _inject_q_priority(sender, task, **kwargs):
    val = _pending_q_priority.get()
    if val is not None:
        task["q_priority"] = val


def install_django_q_priority():
    """Patch async_task so q_options['q_priority'] is copied into the signed task dict."""
    global _installed
    if _installed:
        return
    _installed = True

    import django_q.tasks as dq_tasks

    _orig = dq_tasks.async_task

    def async_task(func, *args, **kwargs):
        q_options = kwargs.get("q_options")
        token: Token | None = None
        if isinstance(q_options, dict) and "q_priority" in q_options:
            try:
                token = _pending_q_priority.set(int(q_options["q_priority"]))
            except (TypeError, ValueError):
                token = _pending_q_priority.set(0)
        try:
            return _orig(func, *args, **kwargs)
        finally:
            if token is not None:
                _pending_q_priority.reset(token)

    dq_tasks.async_task = async_task
    log.debug("django_q priority: async_task patched, pre_enqueue injects q_priority")
