from django.db import migrations


def add_ormq_priority_column(apps, schema_editor):
    conn = schema_editor.connection
    vendor = conn.vendor
    with conn.cursor() as cursor:
        if vendor == "sqlite":
            cursor.execute("PRAGMA table_info(django_q_ormq)")
            existing = {row[1] for row in cursor.fetchall()}
            if "priority" not in existing:
                cursor.execute(
                    "ALTER TABLE django_q_ormq ADD COLUMN priority integer NOT NULL DEFAULT 0"
                )
        elif vendor == "postgresql":
            cursor.execute(
                """
                ALTER TABLE django_q_ormq
                ADD COLUMN IF NOT EXISTS priority integer NOT NULL DEFAULT 0
                """
            )
        else:
            raise NotImplementedError(
                "upload.0007: add django_q_ormq.priority manually for database "
                f"vendor {vendor!r}"
            )


def noop_reverse(apps, schema_editor):
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("upload", "0006_flixbd_integration"),
        ("django_q", "0018_task_success_index"),
    ]

    operations = [
        migrations.RunPython(add_ormq_priority_column, noop_reverse),
    ]
