import time
import logging
from openai import OpenAI
from .models import LLMSettings

logger = logging.getLogger(__name__)

# Retry settings for LLM API calls
MAX_LLM_RETRIES = 3
LLM_RETRY_DELAYS = [3, 8, 15]


class LLMService:
    @staticmethod
    def get_client():
        """
        Initializes the OpenAI client with stored settings.
        If no settings found, returns None or raises exception.
        """
        config = LLMSettings.objects.first()
        if not config or not config.api_key:
            logger.error("LLM Settings (API Key) not found in database.")
            return None

        logger.debug(f"Initializing OpenAI client with base_url: {config.base_url}")
        client = OpenAI(
            api_key=config.api_key,
            base_url=config.base_url
        )
        return client

    @staticmethod
    def generate_completion(prompt, system_prompt="You are a helpful assistant."):
        """
        Get a text completion with automatic retry on failure.
        Retries on: rate limits (429), server errors (5xx), empty responses.
        """
        client = LLMService.get_client()
        if not client:
            raise Exception("LLM Settings not configured correctly in the admin.")

        config = LLMSettings.objects.first()
        logger.info(f"Generating LLM completion using model: {config.model_name}")

        last_error = None
        for attempt in range(MAX_LLM_RETRIES + 1):
            try:
                response = client.chat.completions.create(
                    model=config.model_name,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                )
                content = response.choices[0].message.content

                # Retry on empty response
                if not content or not content.strip():
                    logger.warning(
                        f"LLM returned empty response (attempt {attempt + 1}/{MAX_LLM_RETRIES + 1}). "
                        f"Finish reason: {response.choices[0].finish_reason}"
                    )
                    if attempt < MAX_LLM_RETRIES:
                        delay = LLM_RETRY_DELAYS[attempt]
                        logger.info(f"Retrying in {delay}s...")
                        time.sleep(delay)
                        continue
                    raise Exception("LLM returned empty response after all retries")

                logger.debug("LLM response received successfully.")
                return content

            except Exception as e:
                last_error = e
                error_str = str(e).lower()

                # Retry on rate limit or server errors
                is_retryable = any(x in error_str for x in ['429', '500', '502', '503', '504', 'rate limit', 'timeout', 'empty response'])

                if is_retryable and attempt < MAX_LLM_RETRIES:
                    delay = LLM_RETRY_DELAYS[attempt]
                    logger.warning(
                        f"LLM error (attempt {attempt + 1}/{MAX_LLM_RETRIES + 1}): {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)
                    continue

                logger.error(f"LLM completion failed after {attempt + 1} attempt(s): {e}", exc_info=True)
                raise

        raise last_error
