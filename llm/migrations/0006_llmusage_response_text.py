# LLMUsage.response_text — idempotent on DB: column may already exist.

from django.db import migrations, models
from django.db.utils import OperationalError, ProgrammingError


def add_response_text_if_needed(apps, schema_editor):
    table = schema_editor.quote_name("llm_llmusage")
    col = schema_editor.quote_name("response_text")
    vendor = schema_editor.connection.vendor
    if vendor in ("postgresql", "sqlite"):
        schema_editor.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} TEXT NOT NULL DEFAULT %s",
            [""],
        )
        return
    try:
        schema_editor.execute(
            f"ALTER TABLE {table} ADD COLUMN {col} TEXT NOT NULL DEFAULT %s",
            [""],
        )
    except (ProgrammingError, OperationalError):
        # Column already exists (e.g. manual DDL or retried migration).
        pass


def drop_response_text_if_exists(apps, schema_editor):
    table = schema_editor.quote_name("llm_llmusage")
    col = schema_editor.quote_name("response_text")
    vendor = schema_editor.connection.vendor
    if vendor in ("postgresql", "sqlite"):
        schema_editor.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {col}")
        return
    try:
        schema_editor.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
    except (ProgrammingError, OperationalError):
        pass


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0005_llmusage"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddField(
                    model_name="llmusage",
                    name="response_text",
                    field=models.TextField(
                        blank=True,
                        default="",
                        help_text="Full model completion text for this call (shown read-only in admin).",
                    ),
                ),
            ],
            database_operations=[
                migrations.RunPython(
                    add_response_text_if_needed,
                    drop_response_text_if_exists,
                ),
            ],
        ),
    ]
