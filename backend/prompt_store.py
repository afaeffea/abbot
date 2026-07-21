from __future__ import annotations

import os
import tempfile
from pathlib import Path

from prompt import DEFAULT_SYSTEM_PROMPT

# Relative to this module's own location, not cwd — same reasoning as
# CALL_LOGS_DIR in call_recorder.py, so it resolves the same way both in the
# repo layout and the flattened Docker image.
STORE_PATH = Path(__file__).resolve().parent / "prompt_override.txt"


def get_current_prompt() -> str:
    """Effective system prompt for a *new* session. Always reads the file
    fresh (no in-memory caching) so a save takes effect for the very next
    session started after it. Deliberately synchronous: called from
    BaseSession.__init__, which isn't a coroutine — reading one small text
    file once per call start is a negligible blocking cost."""
    try:
        text = STORE_PATH.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return DEFAULT_SYSTEM_PROMPT
    return text if text else DEFAULT_SYSTEM_PROMPT


def save_prompt(text: str) -> None:
    """Overwrites the stored prompt override. Writes via a uniquely-named
    temp file in the same directory + os.replace(), so concurrent
    get_current_prompt() reads always see either the fully-old or fully-new
    content, and concurrent saves never interleave writes into one file."""
    text = text.strip()
    if not text:
        raise ValueError("prompt must not be empty")

    fd, tmp_path = tempfile.mkstemp(
        dir=str(STORE_PATH.parent), prefix=".prompt_override_", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp_path, STORE_PATH)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def reset_prompt() -> None:
    """Deletes the override file, reverting new sessions to
    DEFAULT_SYSTEM_PROMPT. No-op if already absent."""
    try:
        STORE_PATH.unlink()
    except FileNotFoundError:
        pass
