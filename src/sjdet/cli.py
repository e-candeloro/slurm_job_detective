from __future__ import annotations

import argparse
import os
import time

from rich.console import Console

from sjdet.cache import read_cache, write_cache
from sjdet.display import build_table
from sjdet.slurm import (
    LiveRow,
    list_live_squeue,
    metric_to_gb,
    parse_pages,
    run,
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
    args = parser.parse_args()

    user = args.user or run("whoami")
    os.environ["SLURM_STATS_USER"] = user
    min_interval = max(60, args.interval)

    live = list_live_squeue(user)
    if not live:
        console.print("[yellow]No RUNNING or PENDING jobs found.[/yellow]")
        return

    rows = [
        LiveRow(jid, name, state, el, cp, req) for jid, state, el, cp, req, name in live
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

    use_cache = (
        running_ids
        and cache.get("joblist") == joblist_key
        and (now - cache.get("ts", 0)) < min_interval
    )
    sstat_data = old_data if use_cache else {}

    if running_ids and not use_cache:
        sstat_data = sstat_batch(running_ids)
        write_cache({"ts": now, "joblist": joblist_key, "data": sstat_data})

    for r in rows:
        if r.state != "RUNNING" or not (d := sstat_data.get(r.jobid)):
            continue

        r.maxrss_gb = metric_to_gb(d.get("maxrss"), "K")
        r.maxdisk_gb = metric_to_gb(d.get("maxdisk"), "B")
        r.maxpages = parse_pages(d.get("maxpages"))

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
            r.maxpages_trend = (
                1 if r.maxpages > old_p else (-1 if r.maxpages < old_p else 0)
            )
            r.maxdisk_trend = (
                1 if r.maxdisk_gb > old_d else (-1 if r.maxdisk_gb < old_d else 0)
            )

    console.print(build_table(rows, args.headroom))
