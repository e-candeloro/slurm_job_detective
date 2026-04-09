from __future__ import annotations

import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from importlib import metadata
from typing import Dict, List, Optional, Tuple

GITHUB_OWNER = "e-candeloro"
GITHUB_REPO = "slurm_job_detective"
GITHUB_API = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}"
GITHUB_LATEST_RELEASE_API = f"{GITHUB_API}/releases/latest"
GIT_INSTALL_URL = f"git+https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}.git"

UPDATE_CHECK_TTL_SECONDS = 24 * 60 * 60
UPDATE_NOTICE_COOLDOWN_SECONDS = 7 * 24 * 60 * 60


def _http_get_json(url: str, timeout: float = 4.0) -> Optional[object]:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "sjdet-update-check",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (
        urllib.error.HTTPError,
        urllib.error.URLError,
        TimeoutError,
        json.JSONDecodeError,
    ):
        return None


def _parse_semver_like(v: str) -> Optional[Tuple[int, int, int, int, int]]:
    """Parse x.y.z and x.y.z(a|b|rc)N (with optional leading v)."""
    s = (v or "").strip()
    match = re.match(r"^v?(\d+)\.(\d+)\.(\d+)(?:(a|b|rc)(\d+))?$", s)
    if not match:
        return None

    major = int(match.group(1))
    minor = int(match.group(2))
    patch = int(match.group(3))
    stage = match.group(4) or ""
    stage_num = int(match.group(5) or 0)

    stage_rank = {
        "a": 0,
        "b": 1,
        "rc": 2,
        "": 3,
    }[stage]
    return major, minor, patch, stage_rank, stage_num


def _installed_version() -> str:
    try:
        return metadata.version("slurm-job-detective")
    except Exception:
        return "0.0.0"


def _normalize_version(v: str) -> str:
    return re.sub(r"^v", "", (v or "").strip(), flags=re.IGNORECASE)


def _latest_release_reference() -> Dict[str, str]:
    """Return latest GitHub release information only.

    This intentionally avoids tags/commit fallbacks so update checks and
    upgrades always target the latest published release version.
    """
    rel = _http_get_json(GITHUB_LATEST_RELEASE_API)
    if isinstance(rel, dict):
        tag_name = str(rel.get("tag_name", "")).strip()
        if tag_name:
            return {
                "kind": "release",
                "ref": tag_name,
                "version": _normalize_version(tag_name),
                "url": str(rel.get("html_url", "")).strip(),
            }

    return {"kind": "unknown", "ref": "", "version": "", "url": ""}


def check_for_update() -> Dict[str, object]:
    remote = _latest_release_reference()
    kind = str(remote.get("kind", "unknown"))
    remote_ref = str(remote.get("ref", ""))
    target_version = str(remote.get("version", ""))

    current_version = _installed_version()
    current_version_tuple = _parse_semver_like(current_version)
    remote_version_tuple = _parse_semver_like(target_version)

    available = False
    if kind == "release" and current_version_tuple and remote_version_tuple:
        available = remote_version_tuple > current_version_tuple

    return {
        "last_check_ts": time.time(),
        "remote_kind": kind,
        "remote_ref": remote_ref,
        "remote_url": str(remote.get("url", "")),
        "compare_mode": "version",
        "current_version": current_version,
        "local_version": current_version,
        "target_version": target_version,
        "available": available,
    }


def maybe_update_notice(
    cache: Dict[str, object], now: float
) -> Tuple[Optional[str], Dict[str, object]]:
    update_meta = cache.get("update", {})
    if not isinstance(update_meta, dict):
        update_meta = {}

    last_check = float(update_meta.get("last_check_ts", 0) or 0)
    if now - last_check >= UPDATE_CHECK_TTL_SECONDS or "available" not in update_meta:
        update_meta = check_for_update()

    notice = None
    if update_meta.get("available"):
        last_notice = float(update_meta.get("last_notice_ts", 0) or 0)
        if now - last_notice >= UPDATE_NOTICE_COOLDOWN_SECONDS:
            update_meta["last_notice_ts"] = now
            current_version = str(
                update_meta.get("current_version", update_meta.get("local_version", ""))
            )
            target_version = str(
                update_meta.get("target_version", update_meta.get("remote_ref", ""))
            )
            notice = (
                f"Update available: {current_version} -> {target_version}. "
                "Run 'sjdet --update' to upgrade."
            )

    return notice, update_meta


def run_update_chain(
    target_version: str, current_version: Optional[str] = None
) -> Dict[str, object]:
    target_version = _normalize_version(target_version)
    current_version = _normalize_version(current_version or _installed_version())
    if not target_version:
        return {
            "success": False,
            "command": "",
            "output": "missing target version",
            "attempts": [],
            "from_version": current_version,
            "to_version": target_version,
        }

    target_tag = f"v{target_version}"
    versioned_git_url = f"{GIT_INSTALL_URL}@{target_tag}"

    commands: List[List[str]] = [
        ["uv", "tool", "install", "--upgrade", versioned_git_url],
        ["pipx", "install", "--force", versioned_git_url],
        [sys.executable, "-m", "pip", "install", "--upgrade", versioned_git_url],
    ]

    attempts: List[Dict[str, object]] = []
    for cmd in commands:
        cmd_label = " ".join(cmd)
        try:
            proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
                timeout=180,
            )
            output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
            attempts.append(
                {
                    "command": cmd_label,
                    "returncode": proc.returncode,
                    "output": output.strip()[:3000],
                }
            )
            if proc.returncode == 0:
                return {
                    "success": True,
                    "command": cmd_label,
                    "output": output.strip()[:3000],
                    "attempts": attempts,
                    "from_version": current_version,
                    "to_version": target_version,
                }
        except FileNotFoundError:
            attempts.append(
                {
                    "command": cmd_label,
                    "returncode": None,
                    "output": "command not found",
                }
            )
        except subprocess.TimeoutExpired:
            attempts.append(
                {
                    "command": cmd_label,
                    "returncode": None,
                    "output": "timed out",
                }
            )

    last = attempts[-1] if attempts else {"command": "", "output": "no attempts"}
    return {
        "success": False,
        "command": last.get("command", ""),
        "output": str(last.get("output", "")),
        "attempts": attempts,
        "from_version": current_version,
        "to_version": target_version,
    }
