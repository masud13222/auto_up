# Generated manually for LLMUsage.response_text

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0005_llmusage"),
    ]

    operations = [
        migrations.AddField(
            model_name="llmusage",
            name="response_text",
            field=models.TextField(
                blank=True,
                default="",
                help_text="Full model completion text for this call (shown read-only in admin).",
            ),
        ),
    ]
