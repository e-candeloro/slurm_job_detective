#!/usr/bin/env python3
"""Live SLURM job stats (RUNNING/PENDING), instant render, safe & batched.

Safety:
- One squeue (minimal fields) + zero/one batched sstat (comma-separated IDs).
- Throttle via /tmp cache; --interval is clamped to >= 60s.
- No per-job RPC loops. No sacct in live mode.

UX:
- RUNNING (green), PENDING (yellow)
- CPUs Req + CPU eff bar merged into one stacked column.
- Mem Use/Req → "X/Y GB (Z%)" text + utilization bar merged.
- Suggest --mem = ceil(MaxRSS * (1 + headroom)) moved next to memory stats.
- MaxPages & MaxDiskWrite with active delta tracking (↑ / - / ↓).

Usage:
  uv run slurm_stats.py --user <you> --max-jobs 10 --interval 60 --headroom 0.20
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shlex
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich import box
from rich.console import Console, Group
from rich.progress import ProgressBar
from rich.table import Table
from rich.text import Text

console = Console()


# ----------------------------- shell utils ----------------------------- #


def run(cmd: str) -> str:
    """Run a shell command and return stdout (stripped)."""
    out = subprocess.run(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    return out.stdout.strip()


# ----------------------------- parsing utils ----------------------------- #

_TIME_PATTERNS = [
    re.compile(r"^(\d+)-(\d{2}):(\d{2}):(\d{2})$"),  # D-HH:MM:SS
    re.compile(r"^(\d{1,2}):(\d{2}):(\d{2})$"),  # HH:MM:SS
    re.compile(r"^(\d{1,2}):(\d{2})$"),  # MM:SS
]


def to_seconds(ts: str) -> int:
    """Parse [D-]HH:MM:SS or HH:MM:SS or MM:SS to seconds."""
    ts = (ts or "").strip()
    for i, pattern in enumerate(_TIME_PATTERNS):
        m = pattern.match(ts)
        if m:
            if i == 0:
                d, h, m2, s = map(int, m.groups())
                return d * 86400 + h * 3600 + m2 * 60 + s
            elif i == 1:
                h, m2, s = map(int, m.groups())
                return h * 3600 + m2 * 60 + s
            else:
                m2, s = map(int, m.groups())
                return m2 * 60 + s
    return 0


def metric_to_gb(s: str, default_unit: str = "B") -> float:
    """Convert Slurm memory/disk metrics to GB reliably."""
    s = (s or "").strip().upper()
    if not s or s == "UNKNOWN":
        return 0.0
    m = re.match(r"^([\d\.]+)([KMGTPB]?)$", s)
    if not m:
        num = re.match(r"^([\d\.]+)", s)
        if not num:
            return 0.0
        val = float(num.group(1))
        unit = default_unit
    else:
        val = float(m.group(1))
        unit = m.group(2) or default_unit

    if unit == "G":
        return val
    if unit == "M":
        return val / 1024.0
    if unit == "K":
        return val / 1048576.0
    if unit == "T":
        return val * 1024.0
    if unit == "B":
        return val / 1073741824.0
    return val / 1073741824.0


def parse_pages(s: str) -> int:
    """Extract raw integer page faults."""
    s = (s or "").strip().upper()
    if not s or s == "UNKNOWN":
        return 0
    m = re.match(r"^([\d\.]+)([KMGTP]?)$", s)
    if not m:
        return 0
    val = float(m.group(1))
    unit = m.group(2)
    mults = {"K": 1e3, "M": 1e6, "G": 1e9}
    return int(val * mults.get(unit, 1.0))


# ----------------------------- data model ----------------------------- #


@dataclass
class LiveRow:
    jobid: str
    name: str
    state: str
    elapsed: str
    cpus: int
    req_mem_gb: float
    maxrss_gb: float = 0.0
    cpu_eff_pct: float = 0.0
    maxpages: int = 0
    maxpages_trend: int = 0
    maxdisk_gb: float = 0.0
    maxdisk_trend: int = 0


# ----------------------------- SLURM helpers ----------------------------- #


def list_live_squeue(user: str) -> List[Tuple[str, str, str, int, float, str]]:
    out = run(f'squeue -u {shlex.quote(user)} -h -o "%i|%T|%M|%C|%m|%j"')
    rows = []
    for line in out.splitlines():
        if not line.strip():
            continue
        jid, state, elapsed, cpus, reqm, name = line.split("|", 5)
        rows.append(
            (
                jid.strip(),
                state.strip(),
                elapsed.strip(),
                int(cpus),
                metric_to_gb(reqm, "M"),
                name.strip(),
            )
        )
    return rows


def sstat_batch(jobids: List[str]) -> Dict[str, Dict[str, str]]:
    if not jobids:
        return {}
    jlist = ",".join(jobids)
    rows = run(
        f"sstat -j {shlex.quote(jlist)} --noheader --parsable2 "
        f"--format=JobID,AveCPU,NTasks,MaxRSS,MaxPages,MaxDiskWrite"
    )
    result = {}
    for ln in rows.splitlines():
        parts = ln.split("|")
        if len(parts) < 6:
            continue
        jobstep, avecpu, ntasks, mrss, mpages, mdisk = parts[:6]
        jid = jobstep.split(".")[0]
        result[jid] = {
            "avecpu": avecpu.strip(),
            "ntasks": ntasks.strip(),
            "maxrss": mrss.strip(),
            "maxpages": mpages.strip(),
            "maxdisk": mdisk.strip(),
        }
    return result


# ----------------------------- cache / throttle ----------------------------- #


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


# ----------------------------- UI helpers ----------------------------- #


def state_cell(state: str) -> Text:
    return (
        Text("RUNNING", style="green")
        if state == "RUNNING"
        else Text("PENDING", style="yellow3")
    )


def color_for_util(pct: float) -> str:
    """Colors memory usage text: cyan=under-utilized, green=nominal, yellow=high, red=critical."""
    if pct < 25:
        return "cyan"
    if pct < 80:
        return "green"
    if pct < 95:
        return "yellow3"
    return "red3"


def cpu_combined_group(pct: Optional[float], cpus: int) -> Group:
    """Renders the requested CPUs and efficiency bar vertically stacked."""
    if pct is None:
        txt = Text(f"CPUs Req: {cpus}")
        bar = ProgressBar(total=1.0, completed=0.0, width=18, style="grey50")
        return Group(txt, bar)

    comp = max(0.0, min(100.0, pct))
    txt = Text(f"CPUs Req: {cpus} ({comp:.1f}%)")

    # CPU logic is intentionally inverted from memory: High is Good (Green), Low is Bad (Red)
    if comp >= 85:
        txt.stylize("green")
        style = "green"
    elif comp >= 60:
        txt.stylize("yellow3")
        style = "yellow3"
    else:
        txt.stylize("red3")
        style = "red3"

    bar = ProgressBar(total=100.0, completed=comp, width=18, style=style)
    return Group(txt, bar)


def mem_combined_group(rss_gb: float, req_gb: float) -> Group:
    """Renders the text ratio and progress bar vertically stacked in one cell."""
    if req_gb <= 0 or rss_gb <= 0:
        txt = Text("-")
        bar = ProgressBar(total=1.0, completed=0.0, width=18, style="grey50")
        return Group(txt, bar)

    pct = 100.0 * rss_gb / req_gb
    txt = Text(f"{rss_gb:.2f} / {req_gb:.0f} GB ({pct:.1f}%)")
    txt.stylize(color_for_util(pct))

    frac = max(0.0, min(1.0, rss_gb / req_gb))
    style = "green" if frac < 0.8 else ("yellow3" if frac < 0.9 else "red3")
    bar = ProgressBar(total=1.0, completed=frac, width=18, style=style)

    return Group(txt, bar)


def trend_cell(val_str: str, trend: int) -> Text:
    if trend > 0:
        return Text(f"{val_str} ↑", style="red")
    elif trend < 0:
        return Text(f"{val_str} ↓", style="green")
    return Text(f"{val_str} -", style="dim")


def build_table(rows: List[LiveRow], headroom: float) -> Table:
    t = Table(
        title=f"LIVE (RUNNING/PENDING) — Suggest based on MaxRSS (+{int(round(headroom * 100))}%)",
        box=box.MINIMAL_DOUBLE_HEAD,
    )
    t.add_column("JobID")
    t.add_column("Name", overflow="fold", max_width=60)
    t.add_column("State", justify="right")
    t.add_column("Elapsed", justify="right")
    t.add_column("CPU eff", justify="center")
    t.add_column("Mem Use/Req", justify="center")
    t.add_column("Suggest\n--mem (GB)", justify="center")
    t.add_column("MaxPages", justify="right")
    t.add_column("MaxDiskWr", justify="right")

    for r in sorted(
        rows,
        key=lambda x: (
            0 if x.state == "RUNNING" else 1,
            -to_seconds(x.elapsed),
            x.jobid,
        ),
    ):
        if r.state == "RUNNING":
            cpu_combined = cpu_combined_group(r.cpu_eff_pct, r.cpus)
            mem_combined = mem_combined_group(r.maxrss_gb, r.req_mem_gb)
            sugg = (
                Text(
                    f"{math.ceil(r.maxrss_gb * (1.0 + headroom)):.0f}",
                    style="bold cyan",
                )
                if r.maxrss_gb > 0
                else Text("-")
            )
            pages = trend_cell(str(r.maxpages), r.maxpages_trend)
            disk = trend_cell(f"{r.maxdisk_gb:.2f}G", r.maxdisk_trend)
        else:
            cpu_combined = cpu_combined_group(None, r.cpus)
            mem_combined = mem_combined_group(0, r.req_mem_gb)
            sugg = Text("-")
            pages = Text("-")
            disk = Text("-")

        t.add_row(
            r.jobid,
            r.name,
            state_cell(r.state),
            r.elapsed,
            cpu_combined,
            mem_combined,
            sugg,
            pages,
            disk,
        )
    return t


# ----------------------------- core logic ----------------------------- #


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


if __name__ == "__main__":
    main()
