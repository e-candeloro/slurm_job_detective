from __future__ import annotations

import math
from typing import List, Optional

from rich import box
from rich.align import Align
from rich.console import Group
from rich.padding import Padding
from rich.progress import ProgressBar
from rich.table import Table
from rich.text import Text

from sjdet.slurm import LiveRow, to_seconds


BAR_WIDTH = 14
BAR_TRACK_STYLE = "grey35"
GPU_INLINE_BAR_WIDTH = 6
ROW_STYLES = ["none", "on grey11"]
MIN_ROW_LINES = 2
ROW_PADDING_TOP = 1
ROW_PADDING_BOTTOM = 1


def state_cell(state: str) -> Text:
    return (
        Text("RUNNING", style="bold green")
        if state == "RUNNING"
        else Text("PENDING", style="bold yellow3")
    )


def color_for_util(pct: float) -> str:
    """Global utilization bracket used by CPU, memory, and GPU metrics.

    Policy: utilization >= 90% is red (critical), >= 75% is yellow (warning), else green.
    """
    if pct >= 90:
        return "red3"
    if pct >= 75:
        return "yellow3"
    return "green"


def inline_util_line(
    label: str, pct: float, fill_style: str, width: int = GPU_INLINE_BAR_WIDTH
) -> Text:
    clamped = max(0.0, min(100.0, pct))
    filled = int(round((clamped / 100.0) * width))
    filled = max(0, min(width, filled))

    txt = Text(label, style=fill_style)
    txt.append(" ")
    txt.append("━" * filled, style=fill_style)
    txt.append("━" * (width - filled), style=BAR_TRACK_STYLE)
    return txt


def centered_cell(content: object) -> Padding:
    """Center any cell content and enforce a minimum row height."""
    if isinstance(content, Group):
        lines = list(content.renderables)
    else:
        lines = [content]

    if len(lines) < MIN_ROW_LINES:
        lines.extend(Text(" ") for _ in range(MIN_ROW_LINES - len(lines)))

    centered = Align(Group(*lines), align="center", vertical="middle")
    return Padding(centered, (ROW_PADDING_TOP, 0, ROW_PADDING_BOTTOM, 0))


def cpu_combined_group(pct: Optional[float], cpus: int, show_bar: bool = True) -> Group:
    """Render CPU utilization with compact label and optional bar."""
    if pct is None:
        txt = Text(f"{cpus} CPUs req", style="dim")
        if not show_bar:
            return Group(txt)
        bar = ProgressBar(
            total=100.0,
            completed=0.0,
            width=BAR_WIDTH,
            style=BAR_TRACK_STYLE,
            complete_style=BAR_TRACK_STYLE,
        )
        return Group(txt, bar)

    comp = max(0.0, min(100.0, pct))
    style = color_for_util(comp)
    txt = Text(f"{comp:.0f}%/{cpus}c", style=style)
    if not show_bar:
        return Group(txt)
    bar = ProgressBar(
        total=100.0,
        completed=comp,
        width=BAR_WIDTH,
        style=BAR_TRACK_STYLE,
        complete_style=style,
    )
    return Group(txt, bar)


def mem_combined_group(rss_gb: float, req_gb: float, show_bar: bool = True) -> Group:
    """Render memory utilization with compact ratio and optional bar."""
    if req_gb <= 0 or rss_gb <= 0:
        txt = Text("-", style="dim")
        if not show_bar:
            return Group(txt)
        bar = ProgressBar(
            total=1.0,
            completed=0.0,
            width=BAR_WIDTH,
            style=BAR_TRACK_STYLE,
            complete_style=BAR_TRACK_STYLE,
        )
        return Group(txt, bar)

    pct = 100.0 * rss_gb / req_gb
    style = color_for_util(pct)
    txt = Text(f"{pct:.0f}% {rss_gb:.0f}/{req_gb:.0f}G", style=style)

    if not show_bar:
        return Group(txt)
    frac = max(0.0, min(1.0, rss_gb / req_gb))
    bar = ProgressBar(
        total=1.0,
        completed=frac,
        width=BAR_WIDTH,
        style=BAR_TRACK_STYLE,
        complete_style=style,
    )

    return Group(txt, bar)


def trend_cell(val_str: str, trend: int) -> Text:
    if trend > 0:
        return Text(f"{val_str} ↑", style="bold red3")
    elif trend < 0:
        return Text(f"{val_str} ↓", style="bold green")
    return Text(f"{val_str} -", style="dim")


def gpu_group(
    gpu_count: int,
    gpu_type: str,
    gpu_mem_gb: float,
    gpu_util_pct: float,
    gpu_total_gb: float = 0.0,
    running: bool = False,
    gpu_mem_trend: int = 0,
) -> Group:
    """Render compact GPU summary with at most two lines."""
    if gpu_count == 0:
        return Group(Text("-", style="dim"))

    label = f"GPUx{gpu_count}"
    if gpu_type:
        label += f" ({gpu_type})"
    header = Text(label, style="dim")

    if running:
        if gpu_mem_trend > 0:
            header.append(" ↑", style="bold yellow3")
        elif gpu_mem_trend < 0:
            header.append(" ↓", style="bold cyan")
        else:
            header.append(" -", style="dim")

    if not running:
        return Group(header)

    if gpu_total_gb > 0:
        pct = 100.0 * gpu_mem_gb / gpu_total_gb
        vram_style = color_for_util(pct)
        vram_txt = inline_util_line(
            f"{gpu_mem_gb:.0f}/{gpu_total_gb:.0f}G {pct:.0f}%",
            pct,
            vram_style,
        )
    else:
        util = max(0.0, min(100.0, gpu_util_pct))
        util_style = color_for_util(util)
        vram_txt = inline_util_line(f"util {util:.0f}%", util, util_style)

    return Group(header, vram_txt)


def build_table(rows: List[LiveRow], headroom: float) -> Table:
    has_gpu = any(r.gpu_count > 0 for r in rows)
    t = Table(
        title=f"LIVE (RUNNING/PENDING) — Suggest based on MaxRSS (+{int(round(headroom * 100))}%)",
        box=box.SIMPLE_HEAVY,
        row_styles=ROW_STYLES,
        leading=0,
    )
    t.add_column("JobID", justify="center", vertical="middle")
    t.add_column(
        "Name", justify="center", overflow="fold", max_width=36, vertical="middle"
    )
    t.add_column("State", justify="center", vertical="middle")
    t.add_column("Elapsed", justify="center", vertical="middle")
    if has_gpu:
        t.add_column("Node", justify="center", no_wrap=True, vertical="middle")
        t.add_column("GPU Util", justify="center", no_wrap=True, vertical="middle")
    t.add_column("CPU Util", justify="center", vertical="middle")
    t.add_column("Mem Util", justify="center", no_wrap=True, vertical="middle")
    t.add_column("Suggest GB", justify="center", vertical="middle")
    t.add_column("MaxPages", justify="center", vertical="middle")
    t.add_column("MaxDiskWr", justify="center", vertical="middle")

    for r in sorted(
        rows,
        key=lambda x: (
            0 if x.state == "RUNNING" else 1,
            -to_seconds(x.elapsed),
            x.jobid,
        ),
    ):
        if r.state == "RUNNING":
            cpu_combined = cpu_combined_group(r.cpu_eff_pct, r.cpus, show_bar=True)
            mem_combined = mem_combined_group(r.maxrss_gb, r.req_mem_gb, show_bar=True)
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
            gpu = gpu_group(
                r.gpu_count,
                r.gpu_type,
                r.gpu_mem_gb,
                r.gpu_util_pct,
                r.gpu_total_gb,
                running=True,
                gpu_mem_trend=r.gpu_mem_trend,
            )
        else:
            cpu_combined = cpu_combined_group(None, r.cpus, show_bar=False)
            mem_combined = mem_combined_group(0, r.req_mem_gb, show_bar=False)
            sugg = Text("-", style="dim")
            pages = Text("-", style="dim")
            disk = Text("-", style="dim")
            gpu = gpu_group(
                r.gpu_count, r.gpu_type, 0.0, 0.0, r.gpu_total_gb, running=False
            )

        row_cells = [
            Text(r.jobid),
            Text(r.name),
            state_cell(r.state),
            Text(r.elapsed),
        ]
        if has_gpu:
            row_cells += [Text(r.node or "-"), gpu]
        row_cells += [cpu_combined, mem_combined, sugg, pages, disk]
        t.add_row(*[centered_cell(cell) for cell in row_cells])
    return t
