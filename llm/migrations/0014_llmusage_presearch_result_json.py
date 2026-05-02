from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0013_alter_llmusage_duplicate_check_json_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="llmusage",
            name="presearch_result_json",
            field=models.TextField(
                blank=True,
                default="",
                help_text=(
                    "When purpose=presearch_extract: normalized fields after JSON repair "
                    "(content_type, name, year, season_tag, etc.) for admin and audits."
                ),
            ),
        ),
        migrations.AlterField(
            model_name="llmusage",
            name="purpose",
            field=models.CharField(
                blank=True,
                default="",
                help_text=(
                    "Call kind: e.g. presearch_extract (markdown metadata), combined extract+dup_check, "
                    "filename, auto_filter, etc."
                ),
                max_length=100,
            ),
        ),
        migrations.AlterField(
            model_name="llmusage",
            name="search_query_json",
            field=models.TextField(
                blank=True,
                default="",
                help_text=(
                    "Search query trace used before the combined LLM call (title/year/season_tag + "
                    "DB/FlixBD phases). For purpose=presearch_extract, see presearch_result_json instead."
                ),
            ),
        ),
    ]
