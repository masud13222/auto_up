"""
Backward-compat django-q entry: older schedules may still reference this path.
"""


def run_database_backup():
    from settings.tasks import run_database_backup as _run

    return _run()
