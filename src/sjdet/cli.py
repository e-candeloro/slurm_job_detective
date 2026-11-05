from __future__ import annotations

import argparse
import os
import time
from dataclasses import asdict
from importlib import metadata
from typing import Dict, List, Optional

from rich.console import Console

from sjdet.cache import clear_cache, read_cache, write_cache
from sjdet.display import build_table
from sjdet.slurm import (
    CounterMetric,
    JobUsage,
    LiveRow,
    SlurmCommandError,
    SlurmCommandNotFoundError,
    first_hostname,
    list_live_squeue,
    run,
    scontrol_node_gpu_info,
    sstat_batch,
    to_seconds,
)
from sjdet.update import check_for_update, maybe_update_notice, run_update_chain

console = Console()
MAX_DELTA_WINDOW_SECONDS = 24 * 60 * 60
EPSILON = 1e-9
SNAPSHOT_VERSION = 2
COUNTER_NAMES = ("pages", "io_read", "io_write")


def _installed_version() -> str:
    try:
        return metadata.version("slurm-job-detective")
    except Exception:
        return "0.0.0"


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: object) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct_change(current: float, previous: float) -> Optional[float]:
    if previous <= EPSILON:
        return 0.0 if current <= EPSILON else None
    return ((current - previous) / abs(previous)) * 100.0


def print_slurm_missing_warning(command: str) -> None:
    console.print(
        f"[yellow]Warning: SLURM command '{command}' was not found in PATH.[/yellow]"
    )
    console.print("[yellow]Are you connected to a SLURM cluster?[/yellow]")


def print_slurm_error(exc: SlurmCommandError) -> None:
    console.print(f"[yellow]Warning: {exc.command} failed: {exc.detail}[/yellow]")


# ----------------------------- metric sampling ----------------------------- #


def _update_counter(metric: CounterMetric, previous: object, dt: float) -> None:
    """Compute the rate of a cumulative counter since the previous fresh sample."""
    if not isinstance(previous, dict):
        return
    delta = metric.total - _safe_float(previous.get("total"))
    if delta < -EPSILON:
        # Counter went backwards (e.g. a step ended): restart the baseline.
        metric.reset = True
        return
    metric.rate_per_sec = max(0.0, delta / dt)
    prev_rate = _optional_float(previous.get("rate_per_sec"))
    if prev_rate is not None:
        metric.rate_change_pct = _pct_change(metric.rate_per_sec, prev_rate)


def apply_usage(row: LiveRow, usage: JobUsage, previous: object, now: float) -> None:
    """Fill a row from a fresh sstat sample and compare with the previous one."""
    row.sampled = True
    denom = to_seconds(row.elapsed) * max(row.cpus, 1)
    row.cpu_eff_pct = (100.0 * usage.cpu_seconds / denom) if denom > 0 else 0.0
    row.maxrss_gb = usage.rss_gb
    row.gpu_mem_gb = usage.gpu_mem_gb
    # NVML utilization is summed over the job's GPUs; report the per-GPU mean.
    row.gpu_util_pct = usage.gpu_util_sum_pct / max(row.gpu_count, 1)
    row.pages.total = usage.pages
    row.io_read.total = usage.io_read_gb
    row.io_write.total = usage.io_write_gb

    if not isinstance(previous, dict):
        return
    dt = now - _safe_float(previous.get("ts"))
    if dt <= 0 or dt > MAX_DELTA_WINDOW_SECONDS:
        return

    row.has_history = True
    row.cpu_eff_change_pct = _pct_change(
        row.cpu_eff_pct, _safe_float(previous.get("cpu_eff_pct"))
    )
    row.maxrss_change_pct = _pct_change(
        row.maxrss_gb, _safe_float(previous.get("maxrss_gb"))
    )
    row.gpu_vram_change_pct = _pct_change(
        row.gpu_vram_pct, _safe_float(previous.get("gpu_vram_pct"))
    )
    for name in COUNTER_NAMES:
        _update_counter(getattr(row, name), previous.get(name), dt)


def snapshot_row(row: LiveRow, sample_ts: float) -> Dict[str, object]:
    """Serialize everything derived from a fresh sample for later runs."""
    snap: Dict[str, object] = {
        "ts": sample_ts,
        "has_history": row.has_history,
        "cpu_eff_pct": row.cpu_eff_pct,
        "cpu_eff_change_pct": row.cpu_eff_change_pct,
        "maxrss_gb": row.maxrss_gb,
        "maxrss_change_pct": row.maxrss_change_pct,
        "gpu_mem_gb": row.gpu_mem_gb,
        "gpu_util_pct": row.gpu_util_pct,
        "gpu_vram_pct": row.gpu_vram_pct,
        "gpu_vram_change_pct": row.gpu_vram_change_pct,
    }
    for name in COUNTER_NAMES:
        counter = asdict(getattr(row, name))
        # Only a measured rate may serve as the base of the next rate change.
        if not row.has_history or counter["reset"]:
            counter["rate_per_sec"] = None
        snap[name] = counter
    return snap


def restore_row(row: LiveRow, snap: Dict[str, object]) -> None:
    """Show the last fresh sample unchanged while sstat results are cached."""
    row.sampled = True
    row.has_history = bool(snap.get("has_history"))
    row.cpu_eff_pct = _safe_float(snap.get("cpu_eff_pct"))
    row.cpu_eff_change_pct = _optional_float(snap.get("cpu_eff_change_pct"))
    row.maxrss_gb = _safe_float(snap.get("maxrss_gb"))
    row.maxrss_change_pct = _optional_float(snap.get("maxrss_change_pct"))
    row.gpu_mem_gb = _safe_float(snap.get("gpu_mem_gb"))
    row.gpu_util_pct = _safe_float(snap.get("gpu_util_pct"))
    row.gpu_vram_change_pct = _optional_float(snap.get("gpu_vram_change_pct"))
    for name in COUNTER_NAMES:
        counter = snap.get(name)
        if not isinstance(counter, dict):
            continue
        setattr(
            row,
            name,
            CounterMetric(
                total=_safe_float(counter.get("total")),
                rate_per_sec=_safe_float(counter.get("rate_per_sec")),
                rate_change_pct=_optional_float(counter.get("rate_change_pct")),
                reset=bool(counter.get("reset")),
            ),
        )


def _load_snapshots(cache: dict) -> Dict[str, object]:
    section = cache.get("metric_snapshot", {})
    if not isinstance(section, dict) or section.get("version") != SNAPSHOT_VERSION:
        return {}
    jobs = section.get("jobs", {})
    return jobs if isinstance(jobs, dict) else {}


# ----------------------------- main ----------------------------- #


def _run_update(cache: dict, user: str) -> None:
    update_check = check_for_update()
    current_version = str(update_check.get("current_version", _installed_version()))
    target_version = str(update_check.get("target_version", ""))

    if not target_version:
        console.print("[red]Could not determine latest release version.[/red]")
        return

    if not bool(update_check.get("available")):
        console.print(f"[green]sjdet is already up to date ({current_version}).[/green]")
        return

    result = run_update_chain(target_version, current_version=current_version)
    update_meta = cache.get("update", {})
    if not isinstance(update_meta, dict):
        update_meta = {}
    updated_from = str(result.get("from_version", current_version))
    updated_to = str(result.get("to_version", target_version))
    update_meta.update(
        {
            "last_update_attempt_ts": time.time(),
            "last_update_success": bool(result.get("success")),
            "last_update_command": str(result.get("command", "")),
            "last_update_output": str(result.get("output", ""))[:3000],
            "last_update_from_version": updated_from,
            "last_update_to_version": updated_to,
        }
    )
    cache["update"] = update_meta
    write_cache(user, cache)

    if result.get("success"):
        console.print(
            f"[green]sjdet updated successfully: {updated_from} -> {updated_to}.[/green]"
        )
        return

    console.print(
        f"[red]Failed to update sjdet automatically ({updated_from} -> {updated_to}).[/red]"
    )
    attempts = result.get("attempts", [])
    if isinstance(attempts, list):
        for item in attempts:
            if isinstance(item, dict):
                cmd = str(item.get("command", ""))
                rc = item.get("returncode")
                console.print(f"[yellow]- {cmd} (rc={rc})[/yellow]")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"sjdet {_installed_version()}",
    )
    parser.add_argument("--user", default=os.getenv("USER", ""))
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=10,
        help="Query sstat for at most N running jobs (longest-running first)",
    )
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--headroom", type=float, default=0.20)
    parser.add_argument(
        "--force-update-nodes",
        action="store_true",
        help="Force update the node info cache",
    )
    parser.add_argument(
        "--clear-cache", action="store_true", help="Clear the local cache and exit"
    )
    parser.add_argument("--update", action="store_true", help="Update sjdet and exit")
    args = parser.parse_args()

    current_user = os.getenv("USER") or run("whoami")
    user = args.user or current_user

    if args.clear_cache:
        clear_cache(user)
        console.print("[green]Cache cleared successfully.[/green]")
        return

    cache = read_cache(user)
    if args.update:
        _run_update(cache, user)
        return

    notice, update_meta = maybe_update_notice(cache, time.time())
    cache["update"] = update_meta
    write_cache(user, cache)
    if notice:
        console.print(f"[yellow]{notice}[/yellow]")

    min_interval = max(60, args.interval)

    try:
        rows = list_live_squeue(user)
    except SlurmCommandNotFoundError as exc:
        print_slurm_missing_warning(exc.command)
        return
    except SlurmCommandError as exc:
        print_slurm_error(exc)
        return

    if not rows:
        console.print("[yellow]No RUNNING or PENDING jobs found.[/yellow]")
        return

    running: List[LiveRow] = sorted(
        (r for r in rows if r.state == "RUNNING"),
        key=lambda r: -to_seconds(r.elapsed),
    )[: args.max_jobs]

    now = time.time()
    joblist_key = "allsteps:" + ",".join(sorted(r.stat_jobid for r in running))
    old_snapshots = _load_snapshots(cache)

    # GPU node info (model + VRAM per GPU) is static hardware — cache forever,
    # only call scontrol for nodes we haven't seen yet. Multi-node jobs use
    # their first node, assuming homogeneous GPU nodes.
    cached_node_info = cache.get("node_info", {})
    if not isinstance(cached_node_info, dict):
        cached_node_info = {}
    gpu_hosts = {first_hostname(r.node) for r in rows if r.gpu_count > 0 and r.node}
    missing_nodes = [
        n for n in gpu_hosts if n and (n not in cached_node_info or args.force_update_nodes)
    ]
    if missing_nodes:
        try:
            cached_node_info.update(scontrol_node_gpu_info(missing_nodes))
        except SlurmCommandNotFoundError as exc:
            print_slurm_missing_warning(exc.command)
            return
        except SlurmCommandError as exc:
            print_slurm_error(exc)
        cache["node_info"] = cached_node_info

    for r in rows:
        info = cached_node_info.get(first_hostname(r.node))
        if isinstance(info, (list, tuple)) and len(info) == 2:
            model, vram_gb = info
            r.gpu_total_gb = _safe_float(vram_gb)
            if not r.gpu_type:
                r.gpu_type = str(model)

    use_cache = (
        bool(running)
        and cache.get("joblist") == joblist_key
        and (now - _safe_float(cache.get("ts"))) < min_interval
    )

    if use_cache:
        for r in running:
            snap = old_snapshots.get(r.jobid)
            if isinstance(snap, dict):
                restore_row(r, snap)
    elif running:
        try:
            usage = sstat_batch([r.stat_jobid for r in running])
        except SlurmCommandNotFoundError as exc:
            print_slurm_missing_warning(exc.command)
            return
        except SlurmCommandError as exc:
            print_slurm_error(exc)
            usage = {}

        if not usage and user != current_user:
            console.print(
                "[yellow]Note: sstat only reports your own jobs unless you are a "
                "SLURM operator/admin.[/yellow]"
            )

        snapshots: Dict[str, object] = {}
        for r in running:
            # sstat may label array tasks by raw id or by '<array>_<task>'.
            job_usage = usage.get(r.stat_jobid) or usage.get(r.jobid)
            if job_usage is None:
                continue
            apply_usage(r, job_usage, old_snapshots.get(r.jobid), now)
            snapshots[r.jobid] = snapshot_row(r, now)

        cache.pop("data", None)  # raw sstat output cached by older versions
        cache.update(
            {
                "ts": now,
                "joblist": joblist_key,
                "metric_snapshot": {"version": SNAPSHOT_VERSION, "jobs": snapshots},
            }
        )

    write_cache(user, cache)
    console.print(build_table(rows, args.headroom))
