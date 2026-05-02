import json
import logging
import os
import re
import time
from typing import Any

from .models import LLMConfig, LLMUsage

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAYS = (3, 8, 15)

_RETRYABLE_SUBSTRINGS = frozenset(
    (
        "429",
        "500",
        "502",
        "503",
        "504",
        "rate limit",
        "timeout",
        "empty response",
        "overloaded",
        "capacity",
        "temporarily",
    )
)


def _extract_usage_openai_like(response: Any) -> dict:
    usage = getattr(response, "usage", None)
    if not usage:
        return {}
    return {
        "prompt_tokens": int(getattr(usage, "prompt_tokens", 0) or 0),
        "completion_tokens": int(getattr(usage, "completion_tokens", 0) or 0),
        "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
    }


def _extract_usage_google(response: Any) -> dict:
    meta = getattr(response, "usage_metadata", None)
    if not meta:
        return {}
    return {
        "prompt_tokens": int(getattr(meta, "prompt_token_count", 0) or 0),
        "completion_tokens": int(getattr(meta, "candidates_token_count", 0) or 0),
        "total_tokens": int(getattr(meta, "total_token_count", 0) or 0),
    }


USAGE_EXTRACTORS = {
    "openai": _extract_usage_openai_like,
    "google": _extract_usage_google,
    "mistral": _extract_usage_openai_like,
}


def _completion_truncated(response: Any, sdk: str) -> bool:
    try:
        s = (sdk or "").lower()
        if s in ("openai", "mistral"):
            choices = getattr(response, "choices", None) or []
            if choices and getattr(choices[0], "finish_reason", None) == "length":
                return True
        if s == "google":
            for c in getattr(response, "candidates", None) or []:
                fr = getattr(c, "finish_reason", None)
                if fr is not None and "MAX" in str(fr).upper():
                    return True
    except Exception:
        return False
    return False


def _config_max_output_int(config: LLMConfig) -> int | None:
    raw = (getattr(config, "max_output_tokens", "") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _chat_completions_assistant_text(response: Any) -> str:
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


def _optional_gemini_thinking_config(types_mod: Any) -> Any | None:
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
) -> tuple[str, Any]:
    from openai import OpenAI

    client = OpenAI(api_key=config.api_key, base_url=config.base_url or "https://api.openai.com/v1")
    kwargs: dict[str, Any] = {
        "model": config.model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
    }
    if max_completion_tokens is not None:
        kwargs["max_tokens"] = max_completion_tokens
    effort = os.environ.get("OPENAI_REASONING_EFFORT", "").strip()
    if effort:
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
) -> tuple[str, Any]:
    from google import genai
    from google.genai import types
    from django.conf import settings

    use_proxy = getattr(settings, "GOOGLE_LLM_USE_PROXY", False)
    proxy_url = getattr(settings, "SCRAPE_PROXY", None) if use_proxy else None

    http_options = types.HttpOptions(client_args={"proxy": proxy_url}) if proxy_url else None

    client = genai.Client(
        api_key=config.api_key,
        http_options=http_options,
    )
    gen_kwargs: dict[str, Any] = {
        "system_instruction": system_prompt,
        "temperature": temperature,
    }
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
) -> tuple[str, Any]:
    from mistralai import Mistral

    client = Mistral(api_key=config.api_key)
    kwargs: dict[str, Any] = {
        "model": config.model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": temperature,
    }
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
) -> tuple[str, Any]:
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
    raise ValueError(f"Unknown SDK type: {config.sdk!r}")


def _capture_usage_snapshot(
    config: LLMConfig,
    response: Any | None,
    duration_ms: int,
    *,
    purpose: str = "",
) -> dict[str, Any]:
    if response is None:
        usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    else:
        extractor = USAGE_EXTRACTORS.get(config.sdk)
        usage = extractor(response) if extractor else {}
        if not usage:
            usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    return {
        "purpose": purpose or "",
        "config_name": getattr(config, "name", ""),
        "model_name": getattr(config, "model_name", ""),
        "sdk": getattr(config, "sdk", ""),
        "duration_ms": int(duration_ms or 0),
        "prompt_tokens": int(usage.get("prompt_tokens", 0) or 0),
        "completion_tokens": int(usage.get("completion_tokens", 0) or 0),
        "total_tokens": int(usage.get("total_tokens", 0) or 0),
    }


def _save_usage(
    config: LLMConfig,
    response: Any,
    duration_ms: int,
    success: bool = True,
    purpose: str = "",
    response_text: str = "",
    *,
    outbound_system_prompt: str = "",
    outbound_user_message: str = "",
) -> LLMUsage | None:
    try:
        usage: dict[str, int]
        if response is None:
            usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
        else:
            extractor = USAGE_EXTRACTORS.get(config.sdk)
            usage = extractor(response) if extractor else {}
            if not usage:
                usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

        body = (response_text or "").strip()
        has_tokens = any(int(usage.get(k, 0) or 0) > 0 for k in ("prompt_tokens", "completion_tokens", "total_tokens"))
        if not has_tokens and not body:
            return None

        outbound_json = json.dumps(
            {
                "system_prompt": outbound_system_prompt or "",
                "user_message": outbound_user_message or "",
            },
            indent=2,
            ensure_ascii=False,
        )

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
            "[%s] Usage: %s+%s=%s tokens (%sms)",
            config.name,
            usage.get("prompt_tokens", 0),
            usage.get("completion_tokens", 0),
            usage.get("total_tokens", 0),
            duration_ms,
        )
        return row
    except Exception as e:
        logger.warning("Failed to save LLM usage: %s", e)
        return None


def _get_ordered_configs() -> list[LLMConfig]:
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
        raise RuntimeError(
            "No active LLM configs found. Please add at least one LLM config in the admin panel."
        )
    return configs


_MD_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?", re.MULTILINE)
_MD_FENCE_TAIL_RE = re.compile(r"\n?```\s*$", re.MULTILINE)


def _strip_markdown_fences(text: str) -> str:
    stripped = _MD_FENCE_RE.sub("", text.strip())
    stripped = _MD_FENCE_TAIL_RE.sub("", stripped.strip())
    return stripped.strip()


def _is_retryable_error_message(msg: str) -> bool:
    lower = msg.lower()
    return any(s in lower for s in _RETRYABLE_SUBSTRINGS)


def _try_one_config(
    config: LLMConfig,
    prompt: str,
    system_prompt: str,
    temperature: float = 0.1,
    purpose: str = "",
    *,
    persist_usage: bool = True,
    capture_usage_events: list[dict[str, Any]] | None = None,
) -> str:
    last_error: Exception | None = None
    last_duration_ms = 0

    for attempt in range(MAX_RETRIES + 1):
        try:
            llm_start = time.time()
            cap = _config_max_output_int(config)
            content, response = _invoke_llm(
                config, prompt, system_prompt, temperature, cap
            )
            if _completion_truncated(response, config.sdk):
                raise RuntimeError(
                    f"[{config.name}] Output hit the max length cap (finish_reason=length). "
                    "Pick a higher max output value in this LLM config or shorten the prompt."
                )
            duration_ms = max(0, int((time.time() - llm_start) * 1000))

            content = _strip_markdown_fences(content)

            if not content:
                logger.warning(
                    "[%s] Empty response (attempt %s/%s)",
                    config.name,
                    attempt + 1,
                    MAX_RETRIES + 1,
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAYS[attempt])
                    continue
                raise RuntimeError("LLM returned empty response after all retries")

            if capture_usage_events is not None:
                capture_usage_events.append(
                    _capture_usage_snapshot(config, response, duration_ms, purpose=purpose)
                )

            if persist_usage:
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
                "[%s] LLM raw output: %s characters (purpose=%s, %sms)",
                config.name,
                len(content),
                purpose or "n/a",
                duration_ms,
            )
            return content

        except Exception as e:
            last_error = e
            last_duration_ms = max(0, int((time.time() - llm_start) * 1000))
            err_text = str(e).lower()

            if _is_retryable_error_message(err_text) and attempt < MAX_RETRIES:
                delay = RETRY_DELAYS[attempt]
                logger.warning(
                    "[%s] Error (attempt %s): %s. Retrying in %ss...",
                    config.name,
                    attempt + 1,
                    e,
                    delay,
                )
                time.sleep(delay)
                continue
            break

    if last_error is not None and persist_usage:
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
    if last_error is None:
        raise RuntimeError("LLM call failed with no exception recorded.")
    raise last_error


class LLMService:
    @staticmethod
    def generate_completion(
        prompt: str,
        system_prompt: str = "You are a helpful assistant.",
        temperature: float = 0.1,
        purpose: str = "",
        *,
        persist_usage: bool = True,
        capture_usage_events: list[dict[str, Any]] | None = None,
    ) -> str:
        configs = _get_ordered_configs()

        logger.info(
            "Generating LLM completion. Configs: %s",
            [f"{c.name}({c.sdk})" for c in configs],
        )

        errors: list[str] = []
        for i, config in enumerate(configs):
            try:
                logger.info("Trying [%s] model=%s sdk=%s", config.name, config.model_name, config.sdk)
                content = _try_one_config(
                    config,
                    prompt,
                    system_prompt,
                    temperature,
                    purpose=purpose,
                    persist_usage=persist_usage,
                    capture_usage_events=capture_usage_events,
                )

                if i > 0:
                    logger.info("Fallback succeeded with [%s] after %s failure(s)", config.name, i)

                return content

            except Exception as e:
                logger.warning("[%s] Failed: %s", config.name, e)
                errors.append(f"{config.name}: {e}")

                if i < len(configs) - 1:
                    logger.info("Falling back to next config...")

        error_summary = "\n".join(errors)
        raise RuntimeError(f"All {len(configs)} LLM config(s) failed:\n{error_summary}")
