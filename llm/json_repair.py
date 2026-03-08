import json
import re
import logging

logger = logging.getLogger(__name__)


def repair_json(text: str):
    """
    Attempts to repair truncated or malformed JSON from LLM output.
    
    Strategy:
    1. Extract the JSON portion from the text
    2. Fix trailing commas
    3. Close any unclosed brackets/braces
    4. Try to parse
    
    Returns parsed dict/list on success, raises ValueError on failure.
    """
    if not text or not text.strip():
        raise ValueError("Empty response — nothing to repair")

    # Step 1: Extract JSON portion (find first { or [)
    json_text = _extract_json_block(text)

    # Step 2: Try parsing as-is first (most responses are fine)
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        logger.debug("Direct parse failed, attempting repair...")

    # Step 3: Repair and retry
    repaired = _repair_truncated(json_text)

    try:
        result = json.loads(repaired)
        logger.info("JSON repair successful!")
        return result
    except json.JSONDecodeError as e:
        logger.error(f"JSON repair failed: {e}")
        logger.debug(f"Repaired text (last 200 chars): ...{repaired[-200:]}")
        raise ValueError(f"Could not repair JSON: {e}")


def _extract_json_block(text: str) -> str:
    """Extract the JSON block from LLM text (strip markdown fences, extra text, etc.)"""
    # Remove markdown code fences if present
    text = re.sub(r'^```(?:json)?\s*\n?', '', text.strip(), flags=re.MULTILINE)
    text = re.sub(r'\n?```\s*$', '', text.strip(), flags=re.MULTILINE)

    # Find the first { or [
    obj_start = text.find('{')
    arr_start = text.find('[')

    if obj_start == -1 and arr_start == -1:
        raise ValueError("No JSON object or array found in response")

    # Use whichever comes first
    if obj_start == -1:
        start = arr_start
    elif arr_start == -1:
        start = obj_start
    else:
        start = min(obj_start, arr_start)

    return text[start:]


def _repair_truncated(text: str) -> str:
    """
    Repairs truncated JSON by:
    1. Removing trailing incomplete key-value pairs
    2. Fixing trailing commas
    3. Closing unclosed brackets/braces
    """
    # Track bracket depth
    stack = []
    in_string = False
    escape_next = False
    last_valid_pos = 0

    for i, ch in enumerate(text):
        if escape_next:
            escape_next = False
            continue

        if ch == '\\' and in_string:
            escape_next = True
            continue

        if ch == '"' and not escape_next:
            in_string = not in_string
            continue

        if in_string:
            continue

        if ch in '{[':
            stack.append(ch)
            last_valid_pos = i
        elif ch == '}':
            if stack and stack[-1] == '{':
                stack.pop()
                last_valid_pos = i
        elif ch == ']':
            if stack and stack[-1] == '[':
                stack.pop()
                last_valid_pos = i

    # If balanced, return as-is (just fix trailing commas)
    if not stack:
        return _fix_trailing_commas(text)

    # Truncated — find the last "safe" point to cut
    # We want to cut at the last complete value
    truncated = text[:last_valid_pos + 1]

    # Remove any trailing incomplete content after the last complete element
    # Look for patterns like:  , "incomplete_key":  or  , "incomplete_key": "partial_val
    truncated = re.sub(
        r',\s*"[^"]*"\s*:\s*("([^"\\]|\\.)*)?$',
        '',
        truncated,
        flags=re.DOTALL
    )

    # Remove trailing comma if present
    truncated = _fix_trailing_commas(truncated)

    # Close any unclosed brackets/braces
    # Re-scan to find what needs closing
    stack2 = []
    in_string2 = False
    escape2 = False

    for ch in truncated:
        if escape2:
            escape2 = False
            continue
        if ch == '\\' and in_string2:
            escape2 = True
            continue
        if ch == '"' and not escape2:
            in_string2 = not in_string2
            continue
        if in_string2:
            continue
        if ch in '{[':
            stack2.append(ch)
        elif ch == '}' and stack2 and stack2[-1] == '{':
            stack2.pop()
        elif ch == ']' and stack2 and stack2[-1] == '[':
            stack2.pop()

    # Close in reverse order
    closers = ''
    for bracket in reversed(stack2):
        if bracket == '{':
            closers += '}'
        elif bracket == '[':
            closers += ']'

    result = truncated + closers
    return result


def _fix_trailing_commas(text: str) -> str:
    """Remove trailing commas before } or ] (invalid in JSON but common in LLM output)"""
    # Remove: ,  } or ,  ]
    text = re.sub(r',\s*([\]}])', r'\1', text)
    return text
