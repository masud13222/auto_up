"""
Management command to control the auto-scrape system.

Usage:
    python manage.py auto_scrape              # Run once now
    python manage.py auto_scrape --interval 15  # Set interval to 15 minutes
    python manage.py auto_scrape --pause        # Pause schedule
    python manage.py auto_scrape --resume       # Resume schedule
    python manage.py auto_scrape --status       # Show schedule status
"""

from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Control the auto-scrape system: run now, set interval, pause/resume"

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval",
            type=int,
            help="Set scrape interval in minutes (minimum 5)",
        )
        parser.add_argument(
            "--pause",
            action="store_true",
            help="Pause the auto-scrape schedule",
        )
        parser.add_argument(
            "--resume",
            action="store_true",
            help="Resume the auto-scrape schedule",
        )
        parser.add_argument(
            "--status",
            action="store_true",
            help="Show current schedule status",
        )

    def handle(self, *args, **options):
        if options["status"]:
            self._show_status()
            return

        if options["pause"]:
            from auto_up.scheduler import pause_schedule
            pause_schedule()
            self.stdout.write(self.style.SUCCESS("Auto-scrape schedule paused."))
            return

        if options["resume"]:
            from auto_up.scheduler import resume_schedule
            resume_schedule()
            self.stdout.write(self.style.SUCCESS("Auto-scrape schedule resumed."))
            return

        if options["interval"]:
            from auto_up.scheduler import update_interval
            minutes = options["interval"]
            try:
                update_interval(minutes)
                self.stdout.write(
                    self.style.SUCCESS(f"Auto-scrape interval set to {minutes} minutes.")
                )
            except ValueError as e:
                self.stdout.write(self.style.ERROR(str(e)))
            return

        # Default: run once now
        self.stdout.write("Running auto-scrape now...")
        from auto_up.tasks import auto_scrape_and_queue
        result = auto_scrape_and_queue()
        self.stdout.write(self.style.SUCCESS(f"Result: {result}"))

    def _show_status(self):
        from django_q.models import Schedule

        schedule = Schedule.objects.filter(name="auto_up.auto_scrape").first()
        if not schedule:
            self.stdout.write(self.style.WARNING("No auto-scrape schedule found."))
            return

        status = "ACTIVE" if schedule.repeats != 0 else "PAUSED"
        self.stdout.write(f"Schedule: {schedule.name}")
        self.stdout.write(f"  Status:    {status}")
        self.stdout.write(f"  Interval:  {schedule.minutes} minutes")
        self.stdout.write(f"  Next run:  {schedule.next_run}")
        self.stdout.write(f"  Repeats:   {schedule.repeats} (-1=forever, 0=paused)")
