from __future__ import annotations

import json
import os
from pathlib import Path


def _cache_base() -> Path:
    xdg = os.environ.get("XDG_CACHE_HOME")
    return Path(xdg) if xdg else Path.home() / ".cache"


CACHE_DIR = _cache_base() / "slurm_stats"
CACHE_FILE = (
    CACHE_DIR / f"live_cache_{os.environ.get('SLURM_STATS_USER', 'default')}.json"
)


def read_cache() -> dict:
    try:
        if CACHE_FILE.exists():
            return json.loads(CACHE_FILE.read_text())
    except Exception:
        pass
    return {}


def write_cache(d: dict) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(CACHE_DIR, 0o700)
        except Exception:
            pass
        CACHE_FILE.write_text(json.dumps(d))
    except Exception:
        pass
