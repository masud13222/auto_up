from django.db import migrations, models


def clear_max_output_tokens(apps, schema_editor):
    """Drop only rows that still had migration 0009's default (32768), not user-picked limits."""
    LLMConfig = apps.get_model("llm", "LLMConfig")
    LLMConfig.objects.filter(max_output_tokens="32768").update(max_output_tokens="")


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0009_llmconfig_max_output_tokens"),
    ]

    operations = [
        migrations.AlterField(
            model_name="llmconfig",
            name="max_output_tokens",
            field=models.CharField(
                blank=True,
                choices=[
                    ("", "No select (omit max_tokens / max_output_tokens)"),
                    ("8192", "8,192 (8k)"),
                    ("16384", "16,384 (16k)"),
                    ("32768", "32,768 (32k)"),
                    ("65536", "65,536 (65k)"),
                    ("131072", "131,072 (128k)"),
                ],
                default="",
                help_text=(
                    "OpenAI-compatible / Mistral: max_tokens. Google: max_output_tokens. "
                    "No select: do not send a limit (provider default)."
                ),
                max_length=10,
            ),
        ),
        migrations.RunPython(clear_max_output_tokens, migrations.RunPython.noop),
    ]
