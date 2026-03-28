from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0008_llmusage_duplicate_context_json"),
    ]

    operations = [
        migrations.AddField(
            model_name="llmconfig",
            name="max_output_tokens",
            field=models.CharField(
                blank=True,
                choices=[
                    ("", "Omit (do not send max_tokens / max_output_tokens)"),
                    ("8192", "8,192 (8k)"),
                    ("16384", "16,384 (16k)"),
                    ("32768", "32,768 (32k)"),
                    ("65536", "65,536 (65k)"),
                    ("131072", "131,072 (128k)"),
                ],
                default="32768",
                help_text=(
                    "OpenAI-compatible / Mistral: maps to max_tokens. Google: max_output_tokens. "
                    "Omit: leave unset so the provider uses its default (no parameter sent)."
                ),
                max_length=10,
            ),
        ),
    ]
