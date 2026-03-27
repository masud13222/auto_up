"""
ORM broker extension: dequeue ordered by q_priority (higher first), then id.

Usage:
    async_task(..., q_options={"q_priority": 10})

Requires migration upload.0007 (adds django_q_ormq.priority) and Q_CLUSTER["broker_class"].

Priority semantics (django-q):
    Ordering applies only among tasks that are *waiting* in OrmQ (not yet claimed by a worker).
    There is no preemption: a worker already running a long job will not stop for a higher
    priority task. With WORKERS=1, a new task with priority 100 still waits until the current
    job finishes; then the next dequeue picks the highest-priority waiting row.

    With WORKERS>1, multiple jobs can run at once; a free worker always pulls the best waiting
    task by (-priority, id). Enable DEBUG on logger ``upload.django_q_priority`` to log each
    dequeue (pk + priority).

Stock django-q ``pusher`` prefetches many broker tasks into a FIFO multiprocessing Queue before
workers consume them, which can reorder behind later high-priority inserts. See
``upload.django_q_pusher_backpressure`` — limits in-memory queue depth to **one** pending task
so each new dequeue sees up-to-date DB ordering (independent of ``Conf.WORKERS``).
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
        verbose_name = "Queued task (ORM + priority)"
        verbose_name_plural = "Queued tasks (ORM + priority)"


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
        if priority:
            log.info(
                "django-q OrmQ enqueued pk=%s priority=%s key=%s (higher runs first)",
                row.pk,
                priority,
                row.key,
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
                    log.debug(
                        "django-q OrmQ claimed pk=%s db_priority=%s",
                        task.pk,
                        getattr(task, "priority", 0),
                    )
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


class EnqueueError(Exception):
    """django-q broker/worker refused or failed to accept the task."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def enqueue_process_media_task(media_task_pk: int, url: str, *, q_priority: int = 0) -> str:
    """Queue ``upload.tasks.process_media_task``; raises EnqueueError on broker failure."""
    from django_q.tasks import async_task

    try:
        q_task_id = async_task(
            "upload.tasks.process_media_task",
            media_task_pk,
            task_name=f"Process: {url[:50]}",
            q_options={"q_priority": q_priority},
        )
    except Exception as e:
        log.exception("async_task enqueue failed for MediaTask pk=%s", media_task_pk)
        raise EnqueueError(str(e) or "Queue unavailable; check django-q cluster/workers.") from e
    return q_task_id or ""
