from __future__ import annotations

import math
from typing import List, Optional

from rich import box
from rich.align import Align
from rich.console import Group
from rich.padding import Padding
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
EPSILON = 1e-9


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


def format_pct_label(pct: float) -> str:
    p = max(0.0, min(100.0, pct))
    if p <= 0.0:
        return "0%"
    if p < 0.1:
        return "<0.1%"
    if p < 10.0:
        return f"{p:.1f}%"
    return f"{p:.0f}%"


def format_used_gb_label(gb: float) -> str:
    g = max(0.0, gb)
    if g <= 0.0:
        return "0G"
    if g < 1.0:
        mb = g * 1024.0
        if mb < 1.0:
            return "<1M"
        return f"{mb:.0f}M"
    if g < 10.0:
        return f"{g:.1f}G"
    return f"{g:.0f}G"


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


def util_bar_line(pct: float, fill_style: str, width: int = BAR_WIDTH) -> Text:
    clamped = max(0.0, min(100.0, pct))
    filled = int(round((clamped / 100.0) * width))
    filled = max(0, min(width, filled))

    txt = Text()
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


def change_badge(change_pct: Optional[float]) -> Text:
    if change_pct is None:
        return Text("")
    if abs(change_pct) < 0.05:
        return Text("-", style="dim")
    if change_pct > 0:
        return Text(f"↑ {abs(change_pct):.1f}%", style="bold red3")
    if change_pct < 0:
        return Text(f"↓ {abs(change_pct):.1f}%", style="green")
    return Text("-", style="dim")


def format_pages_rate(rate_per_sec: float) -> str:
    r = max(0.0, rate_per_sec)
    if r >= 1_000_000:
        return f"{r / 1_000_000:.2f} MPg/s"
    if r >= 1_000:
        return f"{r / 1_000:.2f} KPg/s"
    return f"{r:.2f} Pg/s"


def format_disk_rate(rate_gb_per_sec: float) -> str:
    r = max(0.0, rate_gb_per_sec)
    if r >= 1.0:
        return f"{r:.2f} GB/s"
    r_mb = r * 1024.0
    if r_mb >= 1.0:
        return f"{r_mb:.2f} MB/s"
    return f"{r_mb * 1024.0:.2f} KB/s"


def cumulative_metric_group(
    rate_label: str,
    change_pct: Optional[float],
    delta: float,
    reset: bool,
    has_history: bool,
) -> Group:
    if not has_history:
        return Group(Text("-", style="dim"))

    line1 = Text(rate_label, style="bold grey82")
    if reset:
        return Group(
            Text("↺ reset", style="bold yellow3"), Text("baseline", style="yellow3")
        )

    badge = change_badge(change_pct)
    if badge.plain:
        return Group(line1, badge)
    return Group(line1)


def cpu_combined_group(
    pct: Optional[float],
    cpus: int,
    show_bar: bool = True,
    change_pct: Optional[float] = None,
) -> Group:
    """Render CPU utilization with compact label and optional bar."""
    if pct is None:
        txt = Text(f"{cpus} CPUs req", style="dim")
        if not show_bar:
            return Group(txt)
        bar = util_bar_line(0.0, BAR_TRACK_STYLE)
        return Group(txt, bar)

    comp = max(0.0, min(100.0, pct))
    style = color_for_util(comp)
    txt = Text(f"{format_pct_label(comp)}/{cpus}c", style=style)
    badge = change_badge(change_pct)
    if not show_bar:
        return Group(txt, badge) if badge.plain else Group(txt)
    bar = util_bar_line(comp, style)
    return Group(txt, bar, badge) if badge.plain else Group(txt, bar)


def mem_combined_group(
    rss_gb: float,
    req_gb: float,
    show_bar: bool = True,
    change_pct: Optional[float] = None,
) -> Group:
    """Render memory utilization with compact ratio and optional bar."""
    if req_gb <= 0 or rss_gb <= 0:
        txt = Text("-", style="dim")
        if not show_bar:
            return Group(txt)
        bar = util_bar_line(0.0, BAR_TRACK_STYLE)
        return Group(txt, bar)

    pct = 100.0 * rss_gb / req_gb
    style = color_for_util(pct)
    txt = Text(
        f"{format_pct_label(pct)} {format_used_gb_label(rss_gb)}/{req_gb:.0f}G",
        style=style,
    )
    badge = change_badge(change_pct)

    if not show_bar:
        return Group(txt, badge) if badge.plain else Group(txt)
    frac_pct = max(0.0, min(100.0, 100.0 * rss_gb / req_gb))
    bar = util_bar_line(frac_pct, style)

    return Group(txt, bar, badge) if badge.plain else Group(txt, bar)


def gpu_group(
    gpu_count: int,
    gpu_type: str,
    gpu_mem_gb: float,
    gpu_util_pct: float,
    gpu_total_gb: float = 0.0,
    running: bool = False,
    gpu_mem_trend: int = 0,
    gpu_change_pct: Optional[float] = None,
) -> Group:
    """Render compact GPU summary with at most two lines."""
    if gpu_count == 0:
        return Group(Text("-", style="dim"))

    label = f"GPUx{gpu_count}"
    if gpu_type:
        label += f" ({gpu_type})"
    header = Text(label, style="dim")

    if not running:
        return Group(header)

    if gpu_total_gb > 0:
        pct = 100.0 * gpu_mem_gb / gpu_total_gb
        vram_style = color_for_util(pct)
        vram_txt = inline_util_line(
            f"{format_used_gb_label(gpu_mem_gb)}/{gpu_total_gb:.0f}G {format_pct_label(pct)}",
            pct,
            vram_style,
        )
    else:
        util = max(0.0, min(100.0, gpu_util_pct))
        util_style = color_for_util(util)
        vram_txt = inline_util_line(f"util {format_pct_label(util)}", util, util_style)

    badge = change_badge(gpu_change_pct)

    return Group(header, vram_txt, badge) if badge.plain else Group(header, vram_txt)


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
        t.add_column("Cpu Eff %", justify="center", vertical="middle")
        t.add_column(
            "RAM Max Util %", justify="center", no_wrap=True, vertical="middle"
        )
        t.add_column("GPU VRAM %", justify="center", no_wrap=True, vertical="middle")

    else:
        t.add_column("Cpu Eff %", justify="center", vertical="middle")
        t.add_column(
            "RAM Max Util %", justify="center", no_wrap=True, vertical="middle"
        )
    t.add_column("MaxPages", justify="center", vertical="middle")
    t.add_column("MaxDiskWr", justify="center", vertical="middle")
    t.add_column("MaxDiskRead", justify="center", vertical="middle")

    for r in sorted(
        rows,
        key=lambda x: (
            0 if x.state == "RUNNING" else 1,
            -to_seconds(x.elapsed),
            x.jobid,
        ),
    ):
        if r.state == "RUNNING":
            has_history = (
                r.cpu_eff_change_pct is not None
                or r.maxrss_change_pct is not None
                or r.gpu_vram_change_pct is not None
                or abs(r.maxpages_delta) > EPSILON
                or abs(r.maxdisk_delta_gb) > EPSILON
                or abs(r.maxdiskread_delta_gb) > EPSILON
                or r.maxpages_reset
                or r.maxdisk_reset
                or r.maxdiskread_reset
            )

            cpu_combined = cpu_combined_group(
                r.cpu_eff_pct,
                r.cpus,
                show_bar=True,
                change_pct=r.cpu_eff_change_pct,
            )
            mem_combined = mem_combined_group(
                r.maxrss_gb,
                r.req_mem_gb,
                show_bar=True,
                change_pct=r.maxrss_change_pct,
            )
            sugg = (
                Text(
                    f"{math.ceil(r.maxrss_gb * (1.0 + headroom)):.0f}",
                    style="bold cyan",
                )
                if r.maxrss_gb > 0
                else Text("-")
            )

            pages_pct = r.maxpages_rate_change_pct
            disk_pct = r.maxdisk_rate_change_pct
            disk_read_pct = r.maxdiskread_rate_change_pct

            pages = cumulative_metric_group(
                format_pages_rate(r.maxpages_rate_per_sec),
                pages_pct,
                r.maxpages_delta,
                r.maxpages_reset,
                has_history,
            )
            disk = cumulative_metric_group(
                format_disk_rate(r.maxdisk_rate_gb_per_sec),
                disk_pct,
                r.maxdisk_delta_gb,
                r.maxdisk_reset,
                has_history,
            )
            disk_read = cumulative_metric_group(
                format_disk_rate(r.maxdiskread_rate_gb_per_sec),
                disk_read_pct,
                r.maxdiskread_delta_gb,
                r.maxdiskread_reset,
                has_history,
            )
            gpu = gpu_group(
                r.gpu_count,
                r.gpu_type,
                r.gpu_mem_gb,
                r.gpu_util_pct,
                r.gpu_total_gb,
                running=True,
                gpu_mem_trend=r.gpu_mem_trend,
                gpu_change_pct=r.gpu_vram_change_pct,
            )
        else:
            cpu_combined = cpu_combined_group(None, r.cpus, show_bar=False)
            mem_combined = mem_combined_group(0, r.req_mem_gb, show_bar=False)
            sugg = Text("-", style="dim")
            pages = Text("-", style="dim")
            disk = Text("-", style="dim")
            disk_read = Text("-", style="dim")
            gpu = gpu_group(
                r.gpu_count, r.gpu_type, 0.0, 0.0, r.gpu_total_gb, running=False
            )

        _ = sugg
        row_cells = [
            Text(r.jobid),
            Text(r.name),
            state_cell(r.state),
            Text(r.elapsed),
        ]
        if has_gpu:
            row_cells += [Text(r.node or "-"), cpu_combined, mem_combined, gpu]
        else:
            row_cells += [cpu_combined, mem_combined]
        row_cells += [pages, disk, disk_read]
        t.add_row(*[centered_cell(cell) for cell in row_cells])
    return t
