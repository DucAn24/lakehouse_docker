"""Settings for the examples: process env first, then the repo's .env, then the given default."""

from __future__ import annotations

import os
from pathlib import Path

_DOTENV = Path(__file__).resolve().parents[1] / ".env"


def _read_dotenv() -> dict[str, str]:
    values: dict[str, str] = {}
    if _DOTENV.is_file():
        for line in _DOTENV.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("'\"")
    return values


_FILE = _read_dotenv()


def env(name: str, default: str) -> str:
    return os.environ.get(name) or _FILE.get(name) or default
