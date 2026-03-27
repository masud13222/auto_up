from django.db import migrations, models
from django.db.utils import DatabaseError, OperationalError, ProgrammingError


def _add_duplicate_check_json_if_missing(apps, schema_editor):
    """
    Add column only when missing. Fixes deploys where the column exists in DB
    but django_migrations has no row for 0007 (manual SQL, failed migrate, etc.).
    """
    connection = schema_editor.connection
    table = "llm_llmusage"
    column = "duplicate_check_json"

    with connection.cursor() as cursor:
        if connection.vendor == "postgresql":
            cursor.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = %s
                  AND column_name = %s
                """,
                [table, column],
            )
            if cursor.fetchone() is not None:
                return
            cursor.execute(
                f'ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS "{column}" TEXT NOT NULL DEFAULT %s',
                [""],
            )
            return

        if connection.vendor == "sqlite":
            cursor.execute(f'PRAGMA table_info("{table}")')
            col_names = {row[1] for row in cursor.fetchall()}
            if column in col_names:
                return
            cursor.execute(
                f'ALTER TABLE "{table}" ADD COLUMN "{column}" TEXT NOT NULL DEFAULT ""'
            )
            return

    # MySQL / others: try add; ignore "duplicate column" style errors
    with connection.cursor() as cursor:
        try:
            if connection.vendor == "mysql":
                cursor.execute(
                    f"ALTER TABLE `{table}` ADD COLUMN `{column}` LONGTEXT NOT NULL DEFAULT ''"
                )
            else:
                cursor.execute(
                    f'ALTER TABLE "{table}" ADD COLUMN "{column}" TEXT NOT NULL DEFAULT %s',
                    [""],
                )
        except (ProgrammingError, OperationalError, DatabaseError) as exc:
            message = str(exc).lower()
            if (
                "duplicate" in message
                or "already exists" in message
                or "duplicate column" in message
            ):
                return
            raise


def _noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0006_llmusage_response_text"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddField(
                    model_name="llmusage",
                    name="duplicate_check_json",
                    field=models.TextField(
                        blank=True,
                        default="",
                        help_text="When present: duplicate_check object from combined extract+dup_check (JSON text).",
                    ),
                ),
            ],
            database_operations=[
                migrations.RunPython(
                    _add_duplicate_check_json_if_missing,
                    _noop_reverse,
                ),
            ],
        ),
    ]
