"""
django-q stock pusher calls broker.dequeue() in a tight loop and fills the multiprocessing
Queue faster than workers pop. That FIFO buffer ignores later high-priority OrmQ rows.

Wait until the in-memory ``task_queue`` has **fewer than 1** pending item before each
``dequeue()``, so the next pull always re-reads DB order (PriorityORM: -priority, id).

**Why not ``Conf.WORKERS``?** Using ``WORKERS`` as the buffer depth lets the pusher prefetch
N low-priority tasks while workers are busy; a later high-priority OrmQ row stays behind
that FIFO buffer until enough workers drain the queue. ``max_pending=1`` keeps only one
task waiting between DB and workers; multiple workers still run jobs in parallel.

Caveat: if ``Q_CLUSTER['bulk']`` is ever raised above 1, a single dequeue can add more than
one task and briefly exceed this bound; keep ``bulk`` at 1 (default here) for strict priority.
"""

from __future__ import annotations

from multiprocessing import Event
from multiprocessing.process import current_process
from multiprocessing.queues import Queue
from time import sleep

from django import core
from django.apps.registry import apps
from django.utils.translation import gettext_lazy as _

try:
    apps.check_apps_ready()
except core.exceptions.AppRegistryNotReady:
    import django

    django.setup()

from django_q.brokers import Broker, get_broker
from django_q.conf import Conf, logger
from django_q.signing import BadSignature, SignedPackage

try:
    import setproctitle
except ModuleNotFoundError:
    setproctitle = None

_POLL_S = 0.05

_installed = False


def _wait_queue_room(task_queue: Queue, max_pending: int) -> None:
    """Block until fewer than ``max_pending`` tasks are waiting in the in-memory queue."""
    if max_pending <= 0:
        return
    while True:
        try:
            if task_queue.qsize() < max_pending:
                return
        except (NotImplementedError, AttributeError, OSError, ValueError):
            # macOS / some platforms: qsize unreliable — skip backpressure
            return
        sleep(_POLL_S)


def priority_aware_pusher(task_queue: Queue, event: Event, broker: Broker = None):
    if not broker:
        broker = get_broker()
    proc_name = current_process().name
    if setproctitle:
        setproctitle.setproctitle(f"qcluster {proc_name} pusher")
    logger.info(
        _("%(name)s pushing tasks at %(id)s")
        % {"name": proc_name, "id": current_process().pid}
    )
    # Strict priority: never buffer more than one task ahead of workers (see module docstring).
    max_pending = 1
    while True:
        try:
            _wait_queue_room(task_queue, max_pending)
            task_set = broker.dequeue()
        except Exception:
            logger.exception("Failed to pull task from broker")
            sleep(10)
            continue
        if task_set:
            for task in task_set:
                ack_id = task[0]
                try:
                    task = SignedPackage.loads(task[1])
                except (TypeError, BadSignature):
                    logger.exception("Failed to push task to queue")
                    broker.fail(ack_id)
                    continue
                task["cluster"] = Conf.CLUSTER_NAME
                task["ack_id"] = ack_id
                task_queue.put(task)
            logger.debug(_("queueing from %(list_key)s") % {"list_key": broker.list_key})
        if event.is_set():
            break
    logger.info(_("%(name)s stopped pushing tasks") % {"name": current_process().name})


def install_django_q_pusher_backpressure() -> None:
    global _installed
    if _installed:
        return
    _installed = True
    import django_q.pusher as dq_pusher

    dq_pusher.pusher = priority_aware_pusher
