from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("llm", "0011_llmusage_outbound_request_json"),
    ]

    operations = [
        migrations.AddField(
            model_name="llmusage",
            name="search_query_json",
            field=models.TextField(
                blank=True,
                default="",
                help_text=(
                    "Search query trace used before LLM call. Includes extracted title/year/season_tag "
                    "and DB/FlixBD query phases so admin can see exactly what was searched."
                ),
            ),
        ),
    ]
