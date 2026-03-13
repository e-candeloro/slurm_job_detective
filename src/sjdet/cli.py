from __future__ import annotations

import argparse
import os
import time

from rich.console import Console

from sjdet.cache import clear_cache, read_cache, write_cache
from sjdet.display import build_table
from sjdet.slurm import (
    LiveRow,
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

console = Console()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default=os.getenv("USER", ""))
    parser.add_argument("--max-jobs", type=int, default=10)
    parser.add_argument("--interval", type=int, default=60)
    parser.add_argument("--headroom", type=float, default=0.20)
    parser.add_argument("--force-update-nodes", action="store_true", help="Force update the node info cache")
    parser.add_argument("--clear-cache", action="store_true", help="Clear the local cache and exit")
    args = parser.parse_args()

    user = args.user or run("whoami")
    os.environ["SLURM_STATS_USER"] = user

    if args.clear_cache:
        clear_cache()
        console.print("[green]Cache cleared successfully.[/green]")
        return

    min_interval = max(60, args.interval)

    live = list_live_squeue(user)
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

    cache = read_cache()
    now = time.time()
    joblist_key = ",".join(sorted(running_ids))
    old_data = cache.get("data", {})

    # GPU node info (model + total VRAM) is static hardware — cache forever,
    # only call scontrol for nodes we haven't seen yet.
    cached_node_info = cache.get("node_info", {})
    gpu_nodes = [n for r in rows if r.gpu_count > 0 and r.node for n in [r.node]]
    missing_nodes = list({n for n in gpu_nodes if n not in cached_node_info or args.force_update_nodes})
    if missing_nodes:
        cached_node_info.update(scontrol_node_gpu_info(missing_nodes))

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
        sstat_data = sstat_batch(running_ids)
        write_cache({"ts": now, "joblist": joblist_key, "data": sstat_data, "node_info": cached_node_info})
    elif missing_nodes:
        # sstat still cached but we learned about new nodes — persist node info
        write_cache({"ts": cache.get("ts", 0), "joblist": cache.get("joblist", ""), "data": old_data, "node_info": cached_node_info})

    for r in rows:
        if r.state != "RUNNING" or not (d := sstat_data.get(r.jobid)):
            continue

        r.maxrss_gb = metric_to_gb(d.get("maxrss"), "K")
        r.maxdisk_gb = metric_to_gb(d.get("maxdisk"), "B")
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

        # Trend calculation (only valid if we actively polled new data)
        if not use_cache and r.jobid in old_data:
            old_p = parse_pages(old_data[r.jobid].get("maxpages"))
            old_d = metric_to_gb(old_data[r.jobid].get("maxdisk"), "B")
            old_g = metric_to_gb(
                parse_tres_value(old_data[r.jobid].get("tres_in_max", ""), "gres/gpumem"), "B"
            )
            r.maxpages_trend = (
                1 if r.maxpages > old_p else (-1 if r.maxpages < old_p else 0)
            )
            r.maxdisk_trend = (
                1 if r.maxdisk_gb > old_d else (-1 if r.maxdisk_gb < old_d else 0)
            )
            r.gpu_mem_trend = (
                1 if r.gpu_mem_gb > old_g else (-1 if r.gpu_mem_gb < old_g else 0)
            )

    console.print(build_table(rows, args.headroom))
