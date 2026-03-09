import time
import logging
from .models import LLMConfig, LLMUsage

logger = logging.getLogger(__name__)

# Retry settings
MAX_RETRIES = 3
RETRY_DELAYS = [3, 8, 15]


def _extract_usage_openai(response) -> dict:
    """Extract token usage from OpenAI-compatible response."""
    usage = getattr(response, 'usage', None)
    if not usage:
        return {}
    return {
        'prompt_tokens': getattr(usage, 'prompt_tokens', 0) or 0,
        'completion_tokens': getattr(usage, 'completion_tokens', 0) or 0,
        'total_tokens': getattr(usage, 'total_tokens', 0) or 0,
    }


def _extract_usage_google(response) -> dict:
    """Extract token usage from Google Gemini response."""
    meta = getattr(response, 'usage_metadata', None)
    if not meta:
        return {}
    return {
        'prompt_tokens': getattr(meta, 'prompt_token_count', 0) or 0,
        'completion_tokens': getattr(meta, 'candidates_token_count', 0) or 0,
        'total_tokens': getattr(meta, 'total_token_count', 0) or 0,
    }


def _extract_usage_mistral(response) -> dict:
    """Extract token usage from Mistral response."""
    usage = getattr(response, 'usage', None)
    if not usage:
        return {}
    return {
        'prompt_tokens': getattr(usage, 'prompt_tokens', 0) or 0,
        'completion_tokens': getattr(usage, 'completion_tokens', 0) or 0,
        'total_tokens': getattr(usage, 'total_tokens', 0) or 0,
    }


USAGE_EXTRACTORS = {
    'openai': _extract_usage_openai,
    'google': _extract_usage_google,
    'mistral': _extract_usage_mistral,
}


def _call_openai(config: LLMConfig, prompt: str, system_prompt: str, temperature: float = 0.1):
    """Call OpenAI-compatible API. Returns (content, response)."""
    from openai import OpenAI

    client = OpenAI(api_key=config.api_key, base_url=config.base_url or "https://api.openai.com/v1")
    response = client.chat.completions.create(
        model=config.model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        temperature=temperature,
    )
    return response.choices[0].message.content or "", response


def _call_google(config: LLMConfig, prompt: str, system_prompt: str, temperature: float = 0.1):
    """Call Google Gemini API. Returns (content, response)."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=config.api_key)
    response = client.models.generate_content(
        model=config.model_name,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=temperature,
        ),
    )
    return response.text or "", response


def _call_mistral(config: LLMConfig, prompt: str, system_prompt: str, temperature: float = 0.1):
    """Call Mistral AI API. Returns (content, response)."""
    from mistralai import Mistral

    client = Mistral(api_key=config.api_key)
    response = client.chat.complete(
        model=config.model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        temperature=temperature,
    )
    return response.choices[0].message.content or "", response


# SDK dispatcher
SDK_CALLERS = {
    'openai': _call_openai,
    'google': _call_google,
    'mistral': _call_mistral,
}


def _save_usage(config: LLMConfig, response, duration_ms: int, success: bool = True, purpose: str = ''):
    """Save token usage from LLM response to database."""
    try:
        extractor = USAGE_EXTRACTORS.get(config.sdk)
        usage = extractor(response) if extractor else {}

        if not usage:
            return

        LLMUsage.objects.create(
            config=config,
            config_name=config.name,
            model_name=config.model_name,
            sdk=config.sdk,
            prompt_tokens=usage.get('prompt_tokens', 0),
            completion_tokens=usage.get('completion_tokens', 0),
            total_tokens=usage.get('total_tokens', 0),
            purpose=purpose,
            success=success,
            duration_ms=duration_ms,
        )
        logger.debug(
            f"[{config.name}] Usage: {usage.get('prompt_tokens', 0)}+{usage.get('completion_tokens', 0)}"
            f"={usage.get('total_tokens', 0)} tokens ({duration_ms}ms)"
        )
    except Exception as e:
        logger.warning(f"Failed to save LLM usage: {e}")


def _get_ordered_configs():
    """
    Get active LLM configs ordered by priority.
    Primary first, then others by ID.
    """
    configs = list(LLMConfig.objects.filter(is_active=True).order_by('-is_primary', 'pk'))
    if not configs:
        raise Exception(
            "No active LLM configs found. "
            "Please add at least one LLM config in the admin panel."
        )
    return configs


def _try_one_config(config: LLMConfig, prompt: str, system_prompt: str, temperature: float = 0.1, purpose: str = '') -> str:
    """
    Try a single config with retries.
    Returns content string on success, raises on final failure.
    """
    caller = SDK_CALLERS.get(config.sdk)
    if not caller:
        raise ValueError(f"Unknown SDK type: {config.sdk}")

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            start = time.time()
            content, response = caller(config, prompt, system_prompt, temperature)
            duration_ms = int((time.time() - start) * 1000)

            # Check for empty response
            if not content or not content.strip():
                logger.warning(
                    f"[{config.name}] Empty response (attempt {attempt + 1}/{MAX_RETRIES + 1})"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAYS[attempt])
                    continue
                raise Exception("LLM returned empty response after all retries")

            # Save usage on success
            _save_usage(config, response, duration_ms, success=True, purpose=purpose)

            logger.debug(f"[{config.name}] Response received: {len(content)} chars")
            return content

        except Exception as e:
            last_error = e
            error_str = str(e).lower()

            # Check if retryable
            is_retryable = any(x in error_str for x in [
                '429', '500', '502', '503', '504',
                'rate limit', 'timeout', 'empty response',
                'overloaded', 'capacity', 'temporarily'
            ])

            if is_retryable and attempt < MAX_RETRIES:
                delay = RETRY_DELAYS[attempt]
                logger.warning(
                    f"[{config.name}] Error (attempt {attempt + 1}): {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)
                continue

            # Non-retryable or all retries exhausted
            break

    raise last_error


class LLMService:
    """
    Multi-provider LLM service with automatic fallback.
    Tries primary config first, then falls back to other active configs.
    """

    @staticmethod
    def generate_completion(prompt: str, system_prompt: str = "You are a helpful assistant.", temperature: float = 0.1, purpose: str = '') -> str:
        """
        Generate text completion with automatic provider fallback.
        
        Flow:
        1. Get all active configs (primary first)
        2. Try each config with retries
        3. If all fail, raise the last error
        """
        configs = _get_ordered_configs()

        logger.info(
            f"Generating LLM completion. "
            f"Configs: {[f'{c.name}({c.sdk})' for c in configs]}"
        )

        errors = []
        for i, config in enumerate(configs):
            try:
                logger.info(f"Trying [{config.name}] model={config.model_name} sdk={config.sdk}")
                content = _try_one_config(config, prompt, system_prompt, temperature, purpose=purpose)
                
                if i > 0:
                    logger.info(f"Fallback succeeded with [{config.name}] after {i} failure(s)")
                
                return content

            except Exception as e:
                logger.warning(f"[{config.name}] Failed: {e}")
                errors.append(f"{config.name}: {e}")

                # Continue to next config
                if i < len(configs) - 1:
                    logger.info(f"Falling back to next config...")
                    continue

        # All configs failed
        error_summary = "\n".join(errors)
        raise Exception(
            f"All {len(configs)} LLM config(s) failed:\n{error_summary}"
        )
