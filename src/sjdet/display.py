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


def gpu_group(gpu_count: int, gpu_type: str, gpu_mem_gb: float, gpu_util_pct: float, gpu_total_gb: float = 0.0, running: bool = False, gpu_mem_trend: int = 0) -> Group:
    """Renders GPU label+trend, VRAM used/total (%), and utilization bar."""
    if gpu_count == 0:
        return Group(Text("-", style="dim"))

    # Label line: "GPU×1 (P100) ↑" — trend arrow in grey
    label = f"GPU×{gpu_count}"
    if gpu_type:
        label += f" ({gpu_type})"
    header = Text(label, style="dim")
    if running:
        if gpu_mem_trend > 0:
            header.append(" ↑", style="grey50")
        elif gpu_mem_trend < 0:
            header.append(" ↓", style="grey50")
        else:
            header.append(" -", style="grey50")

    if not running:
        return Group(header)

    # ---- VRAM used / total, inverted color scale (high = green, like CPU eff) ----
    if gpu_total_gb > 0:
        pct = 100.0 * gpu_mem_gb / gpu_total_gb
        vram_style = "green" if pct >= 80 else ("yellow3" if pct >= 40 else "red3")
        vram_txt = Text(f"{gpu_mem_gb:.2f} / {gpu_total_gb:.0f} GB ({pct:.1f}%)")
        vram_txt.stylize(vram_style)
        frac = max(0.0, min(1.0, gpu_mem_gb / gpu_total_gb))
        bar = ProgressBar(total=1.0, completed=frac, width=18, style=vram_style)
    else:
        # total VRAM unknown — fall back to util% bar with same inverted scale
        util = max(0.0, min(100.0, gpu_util_pct))
        util_style = "green" if util >= 80 else ("yellow3" if util >= 40 else "red3")
        vram_txt = Text(f"util {util:.0f}%")
        vram_txt.stylize(util_style)
        bar = ProgressBar(total=100.0, completed=util, width=18, style=util_style)

    return Group(header, vram_txt, bar)


def build_table(rows: List[LiveRow], headroom: float) -> Table:
    has_gpu = any(r.gpu_count > 0 for r in rows)
    t = Table(
        title=f"LIVE (RUNNING/PENDING) — Suggest based on MaxRSS (+{int(round(headroom * 100))}%)",
        box=box.MINIMAL_DOUBLE_HEAD,
    )
    t.add_column("JobID")
    t.add_column("Name", overflow="fold", max_width=60)
    t.add_column("State", justify="right")
    t.add_column("Elapsed", justify="right")
    if has_gpu:
        t.add_column("Node", justify="left")
        t.add_column("VRAM Use/Req", justify="center")
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
            gpu = gpu_group(r.gpu_count, r.gpu_type, r.gpu_mem_gb, r.gpu_util_pct, r.gpu_total_gb, running=True, gpu_mem_trend=r.gpu_mem_trend)
        else:
            cpu_combined = cpu_combined_group(None, r.cpus)
            mem_combined = mem_combined_group(0, r.req_mem_gb)
            sugg = Text("-")
            pages = Text("-")
            disk = Text("-")
            gpu = gpu_group(r.gpu_count, r.gpu_type, 0.0, 0.0, r.gpu_total_gb, running=False)

        row_cells = [
            r.jobid,
            r.name,
            state_cell(r.state),
            r.elapsed,
        ]
        if has_gpu:
            row_cells += [r.node or "-", gpu]
        row_cells += [cpu_combined, mem_combined, sugg, pages, disk]
        t.add_row(*row_cells)
    return t
