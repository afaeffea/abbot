from __future__ import annotations

import copy
import json
import os
import tempfile
from pathlib import Path

from settings_defaults import DEFAULT_BUSINESS_FACTS, DEFAULT_GREETING, DEFAULT_PERSONA_PROMPT

# Relative to this module's own location, not cwd — same reasoning as
# CALL_LOGS_DIR in call_recorder.py, so it resolves the same way both in the
# repo layout and the flattened Docker image.
STORE_PATH = Path(__file__).resolve().parent / "settings_override.json"

REASONING_EFFORT_CHOICES = ("minimal", "low", "medium", "high")

DEFAULT_VAD = {
    "energy_threshold": 0.02,
    "speech_start_ms": 150,
    "silence_hangover_ms": 700,
    "barge_in_energy_threshold": 0.035,
    "barge_in_speech_start_ms": 250,
}

MAX_REPLY_TOKENS_RANGE = (50, 2000)  # below 50, tool-calling JSON args risk not fitting; above 2000, no benefit for a short-spoken-phrase bot

DEFAULT_SETTINGS = {
    "greeting": DEFAULT_GREETING,
    "persona_prompt": DEFAULT_PERSONA_PROMPT,
    "business_facts": DEFAULT_BUSINESS_FACTS,
    "reasoning_effort": "low",
    "temperature": None,  # None = don't pass to the API, use the provider's own default
    "voice_id": None,  # None = current auto-pick-first-available behavior
    # 400 was observed in production to routinely cut off mid-sentence on
    # anything requiring real enumeration (e.g. listing several tariffs with
    # weekday/weekend pricing each) — not the separate "reasoning tokens eat
    # the budget" issue, just not enough room for a genuinely detailed reply.
    "max_reply_tokens": 700,
    "vad": dict(DEFAULT_VAD),
}

_VAD_RANGES = {
    "energy_threshold": (0.0, 1.0),
    "speech_start_ms": (0, 10000),
    "silence_hangover_ms": (0, 10000),
    "barge_in_energy_threshold": (0.0, 1.0),
    "barge_in_speech_start_ms": (0, 10000),
}


def validate_nonempty_prompt_field(value, field_name: str) -> str:
    text = (value or "").strip()
    if not text:
        raise ValueError(f"{field_name} не может быть пустым")
    return text


def validate_reasoning_effort(value) -> str:
    text = (value or "").strip()
    if text not in REASONING_EFFORT_CHOICES:
        raise ValueError(
            f"reasoning_effort должен быть одним из {', '.join(REASONING_EFFORT_CHOICES)}"
        )
    return text


def validate_temperature(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        temperature = float(value)
    except (TypeError, ValueError):
        raise ValueError("temperature должен быть числом")
    if not (0.0 <= temperature <= 2.0):
        raise ValueError("temperature должен быть от 0.0 до 2.0")
    return temperature


def validate_voice_id(value) -> str | None:
    text = (value or "").strip()
    return text or None


def validate_max_reply_tokens(value) -> int:
    try:
        tokens = int(value)
    except (TypeError, ValueError):
        raise ValueError("max_reply_tokens должен быть целым числом")
    low, high = MAX_REPLY_TOKENS_RANGE
    if not (low <= tokens <= high):
        raise ValueError(f"max_reply_tokens должен быть от {low} до {high}")
    return tokens


def validate_vad(raw) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("vad должен быть объектом с числовыми порогами")
    result = {}
    for key, (low, high) in _VAD_RANGES.items():
        if key not in raw:
            raise ValueError(f"vad.{key} обязателен")
        try:
            num = float(raw[key])
        except (TypeError, ValueError):
            raise ValueError(f"vad.{key} должен быть числом")
        if not (low <= num <= high):
            raise ValueError(f"vad.{key} должен быть от {low} до {high}")
        result[key] = num
    return result


def normalize_settings(payload: dict) -> dict:
    """Full validate+normalize for PUT /api/settings. Raises ValueError(str)
    with a human-readable message on the first bad field."""
    if not isinstance(payload, dict):
        raise ValueError("Некорректный формат настроек")
    return {
        "greeting": validate_nonempty_prompt_field(payload.get("greeting"), "greeting"),
        "persona_prompt": validate_nonempty_prompt_field(payload.get("persona_prompt"), "persona_prompt"),
        "business_facts": validate_nonempty_prompt_field(payload.get("business_facts"), "business_facts"),
        "reasoning_effort": validate_reasoning_effort(payload.get("reasoning_effort")),
        "temperature": validate_temperature(payload.get("temperature")),
        "voice_id": validate_voice_id(payload.get("voice_id")),
        "max_reply_tokens": validate_max_reply_tokens(payload.get("max_reply_tokens")),
        "vad": validate_vad(payload.get("vad")),
    }


def get_current_settings() -> dict:
    """Effective settings for a *new* session. Always reads the file fresh
    (no in-memory caching) so a save takes effect for the very next session
    started after it. Merges per-field over DEFAULT_SETTINGS so a missing/
    corrupt/partial file degrades to per-field defaults instead of crashing
    session start."""
    settings = copy.deepcopy(DEFAULT_SETTINGS)
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return settings
    if not isinstance(raw, dict):
        return settings

    for key in ("greeting", "persona_prompt", "business_facts", "reasoning_effort", "temperature", "voice_id", "max_reply_tokens"):
        if key in raw:
            settings[key] = raw[key]
    if isinstance(raw.get("vad"), dict):
        settings["vad"].update({k: v for k, v in raw["vad"].items() if k in DEFAULT_VAD})
    return settings


def save_settings(settings: dict) -> None:
    """Overwrites the stored settings override. Caller must pass an already
    normalize_settings()-d dict. Writes via a uniquely-named temp file in the
    same directory + os.replace(), so concurrent get_current_settings() reads
    always see either the fully-old or fully-new content, and concurrent
    saves never interleave writes into one file."""
    fd, tmp_path = tempfile.mkstemp(
        dir=str(STORE_PATH.parent), prefix=".settings_override_", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, STORE_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def reset_settings() -> None:
    """Deletes the override file, reverting new sessions to DEFAULT_SETTINGS.
    No-op if already absent."""
    try:
        STORE_PATH.unlink()
    except FileNotFoundError:
        pass
