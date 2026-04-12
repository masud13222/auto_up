from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("llm", "0010_alter_llmconfig_max_output_tokens_no_default"),
    ]

    operations = [
        migrations.AddField(
            model_name="llmusage",
            name="outbound_request_json",
            field=models.TextField(
                blank=True,
                default="",
                help_text=(
                    "Exact outbound body sent to the provider: JSON with system_prompt and "
                    "user_message (user role / contents), same strings passed to the SDK for this call."
                ),
            ),
        ),
    ]
