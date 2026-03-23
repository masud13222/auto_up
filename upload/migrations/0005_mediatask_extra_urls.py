from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('upload', '0004_add_website_title'),
    ]

    operations = [
        migrations.AddField(
            model_name='mediatask',
            name='extra_urls',
            field=models.JSONField(
                default=list,
                blank=True,
                help_text=(
                    "Additional source URLs for this content. "
                    "When a new URL is detected for the same TV show (e.g. new episode batch), "
                    "it is appended here instead of creating a duplicate task."
                ),
            ),
        ),
    ]
