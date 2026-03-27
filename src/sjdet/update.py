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


def _installed_commit() -> str:
    try:
        dist = metadata.distribution("slurm-job-detective")
        direct_url = dist.read_text("direct_url.json")
        if not direct_url:
            return ""
        data = json.loads(direct_url)
        vcs = data.get("vcs_info", {})
        commit_id = vcs.get("commit_id", "")
        return commit_id.strip()
    except Exception:
        return ""


def _installed_version() -> str:
    try:
        return metadata.version("slurm-job-detective")
    except Exception:
        return "0.0.0"


def _latest_remote_reference() -> Dict[str, str]:
    # 1) Prefer GitHub release tag when available.
    rel = _http_get_json(f"{GITHUB_API}/releases/latest")
    if isinstance(rel, dict):
        tag_name = str(rel.get("tag_name", "")).strip()
        if tag_name:
            return {
                "kind": "release",
                "ref": tag_name,
                "url": str(rel.get("html_url", "")).strip(),
            }

    # 2) Fall back to first tag if releases are missing.
    tags = _http_get_json(f"{GITHUB_API}/tags")
    if isinstance(tags, list) and tags:
        first = tags[0]
        if isinstance(first, dict):
            tag_name = str(first.get("name", "")).strip()
            commit = first.get("commit", {})
            commit_sha = ""
            if isinstance(commit, dict):
                commit_sha = str(commit.get("sha", "")).strip()
            if tag_name:
                return {
                    "kind": "tag",
                    "ref": tag_name,
                    "url": f"https://github.com/{GITHUB_OWNER}/{GITHUB_REPO}/releases/tag/{tag_name}",
                    "commit": commit_sha,
                }

    # 3) Last fallback: default branch head commit.
    repo_meta = _http_get_json(GITHUB_API)
    default_branch = "master"
    if isinstance(repo_meta, dict):
        default_branch = (
            str(repo_meta.get("default_branch", "master")).strip() or "master"
        )

    commit = _http_get_json(f"{GITHUB_API}/commits/{default_branch}")
    if isinstance(commit, dict):
        sha = str(commit.get("sha", "")).strip()
        html_url = str(commit.get("html_url", "")).strip()
        if sha:
            return {
                "kind": "commit",
                "ref": sha,
                "url": html_url,
                "branch": default_branch,
            }

    return {"kind": "unknown", "ref": "", "url": ""}


def check_for_update() -> Dict[str, object]:
    remote = _latest_remote_reference()
    kind = str(remote.get("kind", "unknown"))
    remote_ref = str(remote.get("ref", ""))

    local_version = _installed_version()
    local_version_tuple = _parse_semver_like(local_version)
    remote_version_tuple = _parse_semver_like(remote_ref)
    local_commit = _installed_commit()

    available = False
    compare_mode = "unknown"

    if kind in {"release", "tag"} and local_version_tuple and remote_version_tuple:
        compare_mode = "version"
        available = remote_version_tuple > local_version_tuple
    elif kind == "commit" and local_commit:
        compare_mode = "commit"
        available = remote_ref != local_commit

    return {
        "last_check_ts": time.time(),
        "remote_kind": kind,
        "remote_ref": remote_ref,
        "remote_url": str(remote.get("url", "")),
        "compare_mode": compare_mode,
        "local_version": local_version,
        "local_commit": local_commit,
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
            remote_kind = str(update_meta.get("remote_kind", "update"))
            remote_ref = str(update_meta.get("remote_ref", ""))
            target = f"{remote_kind} {remote_ref}".strip()
            notice = f"Update available ({target}). Run 'sjdet --update' to upgrade."

    return notice, update_meta


def run_update_chain() -> Dict[str, object]:
    commands: List[List[str]] = [
        ["uv", "tool", "upgrade", "slurm-job-detective"],
        ["pipx", "upgrade", "slurm-job-detective"],
        [sys.executable, "-m", "pip", "install", "--upgrade", GIT_INSTALL_URL],
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
    }
