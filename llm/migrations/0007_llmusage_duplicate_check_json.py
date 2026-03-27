from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0006_llmusage_response_text"),
    ]

    operations = [
        migrations.AddField(
            model_name="llmusage",
            name="duplicate_check_json",
            field=models.TextField(
                blank=True,
                default="",
                help_text="When present: duplicate_check object from combined extract+dup_check (JSON text).",
            ),
        ),
    ]
