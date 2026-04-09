from __future__ import annotations

import argparse
import os
import time
from importlib import metadata
from typing import Dict, Optional

from rich.console import Console

from sjdet.cache import clear_cache, read_cache, write_cache
from sjdet.display import build_table
from sjdet.slurm import (
    LiveRow,
    SlurmCommandNotFoundError,
    list_live_squeue,
    metric_to_gb,
    parse_gres_gpu_count,
    parse_gres_gpu_type,
    parse_pages,
    parse_tres_value,
    run,
    scontrol_node_gpu_info,
    sstat_batch,
    to_seconds,
)
from sjdet.update import check_for_update, maybe_update_notice, run_update_chain

console = Console()
MAX_DELTA_WINDOW_SECONDS = 24 * 60 * 60
EPSILON = 1e-9


def _installed_version() -> str:
    try:
        return metadata.version("slurm-job-detective")
    except Exception:
        return "0.0.0"


def _clamp_pct(pct: float) -> float:
    return max(0.0, min(100.0, pct))


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


def _gpu_vram_pct(row: LiveRow) -> float:
    if row.gpu_total_gb > 0:
        return _clamp_pct((100.0 * row.gpu_mem_gb) / row.gpu_total_gb)
    return _clamp_pct(row.gpu_util_pct)


def print_slurm_missing_warning(command: str) -> None:
    console.print(
        f"[yellow]Warning: SLURM command '{command}' was not found in PATH.[/yellow]"
    )
    console.print("[yellow]Are you connected to a SLURM cluster?[/yellow]")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"sjdet {_installed_version()}",
    )
    parser.add_argument("--user", default=os.getenv("USER", ""))
    parser.add_argument("--max-jobs", type=int, default=10)
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

    user = args.user or run("whoami")
    os.environ["SLURM_STATS_USER"] = user

    if args.clear_cache:
        clear_cache()
        console.print("[green]Cache cleared successfully.[/green]")
        return

    cache = read_cache()
    if args.update:
        update_check = check_for_update()
        current_version = str(update_check.get("current_version", _installed_version()))
        target_version = str(update_check.get("target_version", ""))

        if not target_version:
            console.print("[red]Could not determine latest release version.[/red]")
            return

        if not bool(update_check.get("available")):
            console.print(
                f"[green]sjdet is already up to date ({current_version}).[/green]"
            )
            return

        result = run_update_chain(target_version, current_version=current_version)
        update_meta = cache.get("update", {})
        if not isinstance(update_meta, dict):
            update_meta = {}
        now_update = time.time()
        updated_from = str(result.get("from_version", current_version))
        updated_to = str(result.get("to_version", target_version))
        update_meta.update(
            {
                "last_update_attempt_ts": now_update,
                "last_update_success": bool(result.get("success")),
                "last_update_command": str(result.get("command", "")),
                "last_update_output": str(result.get("output", ""))[:3000],
                "last_update_from_version": updated_from,
                "last_update_to_version": updated_to,
            }
        )
        cache["update"] = update_meta
        write_cache(cache)

        if result.get("success"):
            console.print(
                f"[green]sjdet updated successfully: {updated_from} -> {updated_to}.[/green]"
            )
        else:
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
        return

    notice, update_meta = maybe_update_notice(cache, time.time())
    cache["update"] = update_meta
    write_cache(cache)
    if notice:
        console.print(f"[yellow]{notice}[/yellow]")

    min_interval = max(60, args.interval)

    try:
        live = list_live_squeue(user)
    except SlurmCommandNotFoundError as exc:
        print_slurm_missing_warning(exc.command)
        return

    if not live:
        console.print("[yellow]No RUNNING or PENDING jobs found.[/yellow]")
        return

    rows = [
        LiveRow(
            jobid=jid,
            name=name,
            state=state,
            elapsed=el,
            cpus=cp,
            req_mem_gb=req,
            node=node,
            gpu_count=parse_gres_gpu_count(gres),
            gpu_type=parse_gres_gpu_type(gres),
        )
        for jid, state, el, cp, req, name, node, gres in live
    ]
    running_ids = [r.jobid for r in rows if r.state == "RUNNING"]
    running_ids = sorted(
        running_ids,
        key=lambda j: -to_seconds([r.elapsed for r in rows if r.jobid == j][0]),
    )[: args.max_jobs]

    now = time.time()
    joblist_key = ",".join(sorted(running_ids))
    old_data = cache.get("data", {})
    snapshot_section = cache.get("metric_snapshot", {})
    if not isinstance(snapshot_section, dict):
        snapshot_section = {}
    old_snapshots = snapshot_section.get("jobs", {})
    if not isinstance(old_snapshots, dict):
        old_snapshots = {}

    # GPU node info (model + total VRAM) is static hardware — cache forever,
    # only call scontrol for nodes we haven't seen yet.
    cached_node_info = cache.get("node_info", {})
    gpu_nodes = [n for r in rows if r.gpu_count > 0 and r.node for n in [r.node]]
    missing_nodes = list(
        {n for n in gpu_nodes if n not in cached_node_info or args.force_update_nodes}
    )
    if missing_nodes:
        try:
            cached_node_info.update(scontrol_node_gpu_info(missing_nodes))
        except SlurmCommandNotFoundError as exc:
            print_slurm_missing_warning(exc.command)
            return

    for r in rows:
        if r.node in cached_node_info:
            model, vram_gb = cached_node_info[r.node]
            r.gpu_total_gb = vram_gb
            if not r.gpu_type:
                r.gpu_type = model

    use_cache = (
        running_ids
        and cache.get("joblist") == joblist_key
        and (now - cache.get("ts", 0)) < min_interval
    )
    sstat_data = old_data if use_cache else {}

    if running_ids and not use_cache:
        try:
            sstat_data = sstat_batch(running_ids)
        except SlurmCommandNotFoundError as exc:
            print_slurm_missing_warning(exc.command)
            return
        cache.update(
            {
                "ts": now,
                "joblist": joblist_key,
                "data": sstat_data,
                "node_info": cached_node_info,
            }
        )
        write_cache(cache)
    elif missing_nodes:
        # sstat still cached but we learned about new nodes — persist node info
        cache.update(
            {
                "ts": cache.get("ts", 0),
                "joblist": cache.get("joblist", ""),
                "data": old_data,
                "node_info": cached_node_info,
            }
        )
        write_cache(cache)

    for r in rows:
        if r.state != "RUNNING" or not (d := sstat_data.get(r.jobid)):
            continue

        r.maxrss_gb = metric_to_gb(d.get("maxrss"), "K")
        r.maxdisk_gb = metric_to_gb(d.get("maxdisk"), "B")
        r.maxdiskread_gb = metric_to_gb(d.get("maxdiskread"), "B")
        r.maxpages = parse_pages(d.get("maxpages"))

        tres = d.get("tres_in_max", "")
        r.gpu_mem_gb = metric_to_gb(parse_tres_value(tres, "gres/gpumem"), "B")
        try:
            r.gpu_util_pct = float(parse_tres_value(tres, "gres/gpuutil") or 0)
        except ValueError:
            pass

        try:
            avecpu_s = to_seconds(d.get("avecpu", "0"))
            ntasks = int(d.get("ntasks", "0"))
            el_s = to_seconds(r.elapsed)
            denom = el_s * max(r.cpus, 1)
            r.cpu_eff_pct = (100.0 * avecpu_s * ntasks / denom) if denom > 0 else 0.0
        except Exception:
            pass

        previous = old_snapshots.get(r.jobid, {})
        if not isinstance(previous, dict):
            continue

        prev_ts = _safe_float(previous.get("ts"), 0.0)
        dt = now - prev_ts
        if dt <= 0 or dt > MAX_DELTA_WINDOW_SECONDS:
            continue

        prev_cpu = _safe_float(previous.get("cpu_eff_pct"), 0.0)
        prev_rss = _safe_float(previous.get("maxrss_gb"), 0.0)
        prev_gpu_pct = _safe_float(previous.get("gpu_vram_pct"), 0.0)
        prev_pages = _safe_float(previous.get("maxpages"), 0.0)
        prev_disk = _safe_float(previous.get("maxdisk_gb"), 0.0)
        prev_disk_read = _safe_float(previous.get("maxdiskread_gb"), 0.0)
        prev_pages_rate = _optional_float(previous.get("maxpages_rate_per_sec"))
        prev_disk_rate = _optional_float(previous.get("maxdisk_rate_gb_per_sec"))
        prev_disk_read_rate = _optional_float(
            previous.get("maxdiskread_rate_gb_per_sec")
        )

        r.cpu_eff_change_pct = _pct_change(r.cpu_eff_pct, prev_cpu)
        r.maxrss_change_pct = _pct_change(r.maxrss_gb, prev_rss)
        r.gpu_vram_change_pct = _pct_change(_gpu_vram_pct(r), prev_gpu_pct)

        r.maxpages_delta = float(r.maxpages) - prev_pages
        r.maxdisk_delta_gb = r.maxdisk_gb - prev_disk
        r.maxdiskread_delta_gb = r.maxdiskread_gb - prev_disk_read

        if r.maxpages_delta < -EPSILON:
            r.maxpages_reset = True
            r.maxpages_trend = 0
            r.maxpages_rate_per_sec = 0.0
            r.maxpages_rate_change_pct = None
        else:
            r.maxpages_trend = 1 if r.maxpages_delta > EPSILON else 0
            r.maxpages_rate_per_sec = max(0.0, r.maxpages_delta / dt)
            if prev_pages_rate is not None:
                r.maxpages_rate_change_pct = _pct_change(
                    r.maxpages_rate_per_sec, prev_pages_rate
                )

        if r.maxdisk_delta_gb < -EPSILON:
            r.maxdisk_reset = True
            r.maxdisk_trend = 0
            r.maxdisk_rate_gb_per_sec = 0.0
            r.maxdisk_rate_change_pct = None
        else:
            r.maxdisk_trend = 1 if r.maxdisk_delta_gb > EPSILON else 0
            r.maxdisk_rate_gb_per_sec = max(0.0, r.maxdisk_delta_gb / dt)
            if prev_disk_rate is not None:
                r.maxdisk_rate_change_pct = _pct_change(
                    r.maxdisk_rate_gb_per_sec, prev_disk_rate
                )

        if r.maxdiskread_delta_gb < -EPSILON:
            r.maxdiskread_reset = True
            r.maxdiskread_trend = 0
            r.maxdiskread_rate_gb_per_sec = 0.0
            r.maxdiskread_rate_change_pct = None
        else:
            r.maxdiskread_trend = 1 if r.maxdiskread_delta_gb > EPSILON else 0
            r.maxdiskread_rate_gb_per_sec = max(0.0, r.maxdiskread_delta_gb / dt)
            if prev_disk_read_rate is not None:
                r.maxdiskread_rate_change_pct = _pct_change(
                    r.maxdiskread_rate_gb_per_sec, prev_disk_read_rate
                )

    current_snapshots: Dict[str, Dict[str, float]] = {}
    for r in rows:
        if r.state != "RUNNING":
            continue
        current_snapshots[r.jobid] = {
            "ts": now,
            "cpu_eff_pct": r.cpu_eff_pct,
            "maxrss_gb": r.maxrss_gb,
            "gpu_vram_pct": _gpu_vram_pct(r),
            "maxpages": float(r.maxpages),
            "maxdisk_gb": r.maxdisk_gb,
            "maxdiskread_gb": r.maxdiskread_gb,
            "maxpages_rate_per_sec": r.maxpages_rate_per_sec,
            "maxdisk_rate_gb_per_sec": r.maxdisk_rate_gb_per_sec,
            "maxdiskread_rate_gb_per_sec": r.maxdiskread_rate_gb_per_sec,
        }

    cache["metric_snapshot"] = {"jobs": current_snapshots}
    write_cache(cache)

    console.print(build_table(rows, args.headroom))
