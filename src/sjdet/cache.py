from __future__ import annotations

import json
import os
import re
from pathlib import Path


def _cache_base() -> Path:
    xdg = os.environ.get("XDG_CACHE_HOME")
    return Path(xdg) if xdg else Path.home() / ".cache"


CACHE_DIR = _cache_base() / "slurm_stats"


def cache_file(user: str) -> Path:
    """Per-inspected-user cache file, so `--user` runs keep separate history."""
    safe_user = re.sub(r"[^A-Za-z0-9_.-]", "_", user or "") or "default"
    return CACHE_DIR / f"live_cache_{safe_user}.json"


def read_cache(user: str) -> dict:
    try:
        path = cache_file(user)
        if path.exists():
            data = json.loads(path.read_text())
            return data if isinstance(data, dict) else {}
    except Exception:
        pass
    return {}


def write_cache(user: str, d: dict) -> None:
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(CACHE_DIR, 0o700)
        except Exception:
            pass
        # Write then rename so concurrent runs never read a half-written file.
        path = cache_file(user)
        tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
        tmp.write_text(json.dumps(d))
        os.replace(tmp, path)
    except Exception:
        pass


def clear_cache(user: str) -> None:
    try:
        path = cache_file(user)
        if path.exists():
            path.unlink()
    except Exception:
        pass
