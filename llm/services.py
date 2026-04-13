import json
import logging
import os
import re
import time

from .models import LLMConfig, LLMUsage

logger = logging.getLogger(__name__)

# Retry settings
MAX_RETRIES = 3
RETRY_DELAYS = [3, 8, 15]

# Max output is configured per LLMConfig.max_output_tokens (omit = no max_* parameter sent).


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


def _completion_truncated(response, sdk: str) -> bool:
    """True if the provider stopped because of output length (partial JSON possible)."""
    try:
        sdk = (sdk or "").lower()
        if sdk == "openai":
            choices = getattr(response, "choices", None) or []
            if choices and getattr(choices[0], "finish_reason", None) == "length":
                return True
        if sdk == "mistral":
            choices = getattr(response, "choices", None) or []
            if choices and getattr(choices[0], "finish_reason", None) == "length":
                return True
        if sdk == "google":
            for c in getattr(response, "candidates", None) or []:
                fr = getattr(c, "finish_reason", None)
                if fr is not None and "MAX" in str(fr).upper():
                    return True
    except Exception:
        pass
    return False


def _config_max_output_int(config: LLMConfig) -> int | None:
    raw = (getattr(config, "max_output_tokens", "") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _max_output_cap_for_config(config: LLMConfig) -> int | None:
    """Single cap from config only: None = omit max_* on the request (no fallback)."""
    return _config_max_output_int(config)


def _chat_completions_assistant_text(response) -> str:
    """Standard chat.completions shape: choices[0].message.content (str or OpenAI text parts list)."""
    choices = getattr(response, "choices", None) or []
    if not choices:
        raise RuntimeError("LLM returned no choices.")
    msg = getattr(choices[0], "message", None)
    if msg is None:
        raise RuntimeError("LLM returned no message on the first choice.")
    raw = getattr(msg, "content", None)
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw
    if isinstance(raw, list):
        parts: list[str] = []
        for part in raw:
            if isinstance(part, dict):
                t = part.get("text")
                if isinstance(t, str):
                    parts.append(t)
            elif isinstance(part, str):
                parts.append(part)
        return "".join(parts)
    return str(raw)


def _optional_gemini_thinking_config(types_mod) -> object | None:
    """
    Only when env sets optional Gemini thinking knobs; otherwise omit (legacy behavior).
    GEMINI_THINKING_LEVEL: MINIMAL|LOW|... (ThinkingLevel). Takes precedence over budget if both set.
    GEMINI_THINKING_BUDGET: integer string (e.g. 0, -1). Invalid values are skipped with a warning.
    """
    level_raw = os.environ.get("GEMINI_THINKING_LEVEL", "").strip()
    if level_raw:
        label = level_raw.upper()
        tl = getattr(types_mod.ThinkingLevel, label, None)
        if tl is None:
            logger.warning("Unknown GEMINI_THINKING_LEVEL=%r; omitting thinking_config.", level_raw)
            return None
        return types_mod.ThinkingConfig(thinking_level=tl)
    budget_raw = os.environ.get("GEMINI_THINKING_BUDGET", "").strip()
    if not budget_raw:
        return None
    try:
        b = int(budget_raw)
    except ValueError:
        logger.warning("Invalid GEMINI_THINKING_BUDGET=%r; omitting thinking_config.", budget_raw)
        return None
    return types_mod.ThinkingConfig(thinking_budget=b)


def _call_openai(
    config: LLMConfig,
    prompt: str,
    system_prompt: str,
    temperature: float = 0.1,
    *,
    max_completion_tokens: int | None = None,
):
    """Call OpenAI-compatible API. Returns (content, response)."""
    from openai import OpenAI

    client = OpenAI(api_key=config.api_key, base_url=config.base_url or "https://api.openai.com/v1")
    kwargs = dict(
        model=config.model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
    )
    if max_completion_tokens is not None:
        kwargs["max_tokens"] = max_completion_tokens
    effort = os.environ.get("OPENAI_REASONING_EFFORT", "").strip()
    if effort:
        # API expects lowercase literals (none, minimal, low, ...); see OpenAI chat completion params.
        kwargs["reasoning_effort"] = effort.lower()
    response = client.chat.completions.create(**kwargs)
    return _chat_completions_assistant_text(response), response


def _call_google(
    config: LLMConfig,
    prompt: str,
    system_prompt: str,
    temperature: float = 0.1,
    *,
    max_completion_tokens: int | None = None,
):
    """Call Google Gemini API. Returns (content, response)."""
    from google import genai
    from google.genai import types
    from django.conf import settings

    use_proxy = getattr(settings, 'GOOGLE_LLM_USE_PROXY', False)
    proxy_url = getattr(settings, 'SCRAPE_PROXY', None) if use_proxy else None

    http_options = None
    if proxy_url:
        http_options = types.HttpOptions(client_args={"proxy": proxy_url})

    client = genai.Client(
        api_key=config.api_key,
        http_options=http_options,
    )
    gen_kwargs = dict(
        system_instruction=system_prompt,
        temperature=temperature,
    )
    if max_completion_tokens is not None:
        gen_kwargs["max_output_tokens"] = max_completion_tokens
    thinking = _optional_gemini_thinking_config(types)
    if thinking is not None:
        gen_kwargs["thinking_config"] = thinking
    response = client.models.generate_content(
        model=config.model_name,
        contents=prompt,
        config=types.GenerateContentConfig(**gen_kwargs),
    )
    return response.text or "", response


def _call_mistral(
    config: LLMConfig,
    prompt: str,
    system_prompt: str,
    temperature: float = 0.1,
    *,
    max_completion_tokens: int | None = None,
):
    """Call Mistral AI API. Returns (content, response)."""
    from mistralai import Mistral

    client = Mistral(api_key=config.api_key)
    kwargs = dict(
        model=config.model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        temperature=temperature,
    )
    if max_completion_tokens is not None:
        kwargs["max_tokens"] = max_completion_tokens
    response = client.chat.complete(**kwargs)
    return _chat_completions_assistant_text(response), response


def _invoke_llm(
    config: LLMConfig,
    prompt: str,
    system_prompt: str,
    temperature: float,
    max_completion_tokens: int | None,
) -> tuple[str, object]:
    sdk = (config.sdk or "").strip().lower()
    if sdk == "openai":
        return _call_openai(
            config,
            prompt,
            system_prompt,
            temperature,
            max_completion_tokens=max_completion_tokens,
        )
    if sdk == "google":
        return _call_google(
            config,
            prompt,
            system_prompt,
            temperature,
            max_completion_tokens=max_completion_tokens,
        )
    if sdk == "mistral":
        return _call_mistral(
            config,
            prompt,
            system_prompt,
            temperature,
            max_completion_tokens=max_completion_tokens,
        )
    raise ValueError(f"Unknown SDK type: {config.sdk}")


# SDK dispatcher (default completion cap; truncation retry uses _invoke_llm directly)
SDK_CALLERS = {
    'openai': _call_openai,
    'google': _call_google,
    'mistral': _call_mistral,
}


def _save_usage(
    config: LLMConfig,
    response,
    duration_ms: int,
    success: bool = True,
    purpose: str = "",
    response_text: str = "",
    *,
    outbound_system_prompt: str = "",
    outbound_user_message: str = "",
):
    """Save token usage, completion text, and exact outbound system+user messages."""
    try:
        extractor = USAGE_EXTRACTORS.get(config.sdk)
        usage = extractor(response) if extractor else {}

        if not usage:
            usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        body = (response_text or "").strip()
        has_tokens = any(
            int(usage.get(k, 0) or 0) > 0 for k in ("prompt_tokens", "completion_tokens", "total_tokens")
        )
        if not has_tokens and not body:
            return

        outbound_payload = {
            "system_prompt": outbound_system_prompt or "",
            "user_message": outbound_user_message or "",
        }
        outbound_json = json.dumps(outbound_payload, indent=2, ensure_ascii=False)

        row = LLMUsage.objects.create(
            config=config,
            config_name=config.name,
            model_name=config.model_name,
            sdk=config.sdk,
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            purpose=purpose,
            success=success,
            duration_ms=duration_ms,
            response_text=response_text or "",
            outbound_request_json=outbound_json,
        )
        logger.debug(
            f"[{config.name}] Usage: {usage.get('prompt_tokens', 0)}+{usage.get('completion_tokens', 0)}"
            f"={usage.get('total_tokens', 0)} tokens ({duration_ms}ms)"
        )
        return row
    except Exception as e:
        logger.warning(f"Failed to save LLM usage: {e}")
        return None


def _get_ordered_configs():
    """
    Get active LLM configs ordered by priority.
    Primary first, then others by ID.
    Drops duplicate rows that point at the same provider+model+base_url (avoids retrying
    the same failing endpoint twice, e.g. two identical Gemini primary rows).
    """
    raw = list(LLMConfig.objects.filter(is_active=True).order_by("-is_primary", "pk"))
    seen: set[tuple[str, str, str]] = set()
    configs: list[LLMConfig] = []
    for c in raw:
        key = (
            (c.sdk or "").strip().lower(),
            (c.model_name or "").strip().lower(),
            (c.base_url or "").strip().lower(),
        )
        if key in seen:
            logger.warning(
                "Skipping duplicate LLM config pk=%s name=%r (same sdk/model/base as earlier in chain)",
                c.pk,
                c.name,
            )
            continue
        seen.add(key)
        configs.append(c)
    if not configs:
        raise Exception(
            "No active LLM configs found. "
            "Please add at least one LLM config in the admin panel."
        )
    return configs


_MD_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?", re.MULTILINE)
_MD_FENCE_TAIL_RE = re.compile(r"\n?```\s*$", re.MULTILINE)


def _strip_markdown_fences(text: str) -> str:
    """Remove ```json ... ``` wrappers that LLMs sometimes add despite being told not to."""
    stripped = _MD_FENCE_RE.sub("", text.strip())
    stripped = _MD_FENCE_TAIL_RE.sub("", stripped.strip())
    return stripped.strip()


def _try_one_config(config: LLMConfig, prompt: str, system_prompt: str, temperature: float = 0.1, purpose: str = '') -> str:
    """
    Try a single config with retries.
    Returns content string on success, raises on final failure.
    """
    last_error = None
    last_duration_ms = 0
    for attempt in range(MAX_RETRIES + 1):
        try:
            llm_start = time.time()
            cap = _max_output_cap_for_config(config)
            content, response = _invoke_llm(
                config, prompt, system_prompt, temperature, cap
            )
            if _completion_truncated(response, config.sdk):
                raise RuntimeError(
                    f"[{config.name}] Output hit the max length cap (finish_reason=length). "
                    "Pick a higher max output value in this LLM config or shorten the prompt."
                )
            duration_ms = int((time.time() - llm_start) * 1000)

            content = _strip_markdown_fences(content)

            if not content:
                logger.warning(
                    f"[{config.name}] Empty response (attempt {attempt + 1}/{MAX_RETRIES + 1})"
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAYS[attempt])
                    continue
                raise Exception("LLM returned empty response after all retries")

            _save_usage(
                config,
                response,
                duration_ms,
                success=True,
                purpose=purpose,
                response_text=content,
                outbound_system_prompt=system_prompt,
                outbound_user_message=prompt,
            )

            logger.info(
                f"[{config.name}] LLM raw output: {len(content)} characters "
                f"(purpose={purpose or 'n/a'}, {duration_ms}ms)"
            )
            return content

        except Exception as e:
            last_error = e
            last_duration_ms = int((time.time() - llm_start) * 1000)
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

    if last_error is not None:
        _save_usage(
            config,
            None,
            last_duration_ms,
            success=False,
            purpose=purpose,
            response_text=str(last_error),
            outbound_system_prompt=system_prompt,
            outbound_user_message=prompt,
        )
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
