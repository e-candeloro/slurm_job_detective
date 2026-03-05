from __future__ import annotations

import math
from typing import List, Optional

from rich import box
from rich.console import Group
from rich.progress import ProgressBar
from rich.table import Table
from rich.text import Text

from sjdet.slurm import LiveRow, to_seconds


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
