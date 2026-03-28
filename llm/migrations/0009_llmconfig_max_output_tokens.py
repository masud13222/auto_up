from django.db import migrations, models


def _max_output_tokens_field():
    return models.CharField(
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
    )


def _column_exists(schema_editor, table_name: str, column_name: str) -> bool:
    conn = schema_editor.connection
    with conn.cursor() as cursor:
        try:
            desc = conn.introspection.get_table_description(cursor, table_name)
        except Exception:
            return False
    return any(getattr(col, "name", col[0]) == column_name for col in desc)


def add_max_output_tokens_if_missing(apps, schema_editor):
    """
    Add column only when absent. Fixes deploys where the column was created
    outside this migration (manual SQL, squash, or a previously failed run).
    """
    LLMConfig = apps.get_model("llm", "LLMConfig")
    table = LLMConfig._meta.db_table
    if _column_exists(schema_editor, table, "max_output_tokens"):
        return
    field = _max_output_tokens_field()
    field.set_attributes_from_name("max_output_tokens")
    schema_editor.add_field(LLMConfig, field)


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0008_llmusage_duplicate_context_json"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AddField(
                    model_name="llmconfig",
                    name="max_output_tokens",
                    field=_max_output_tokens_field(),
                ),
            ],
            database_operations=[
                migrations.RunPython(
                    add_max_output_tokens_if_missing,
                    migrations.RunPython.noop,
                ),
            ],
        ),
    ]
