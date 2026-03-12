from __future__ import annotations

import re
import shlex
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Tuple


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


def parse_tres_value(tres: str, key: str) -> str:
    """Extract the raw value string for a key from a TRES string.

    e.g. parse_tres_value('gres/gpumem=4096,gres/gpuutil=72', 'gres/gpumem') -> '4096'
    """
    m = re.search(rf'(?:^|,){re.escape(key)}=([^,\s]+)', tres or "")
    return m.group(1) if m else ""


def parse_gres_gpu_count(gres: str) -> int:
    """Extract GPU count from squeue %b GRES string.

    Handles 'gres/gpu:1' and 'gres/gpu:a100:2' formats.
    """
    m = re.search(r'gres/gpu(?::[a-zA-Z0-9_]+)?:(\d+)', gres or "")
    return int(m.group(1)) if m else 0


def parse_gres_gpu_type(gres: str) -> str:
    """Extract GPU type from GRES string, e.g. 'a100' from 'gres/gpu:a100:2'."""
    m = re.search(r'gres/gpu:([a-zA-Z][a-zA-Z0-9_]*)?:(\d+)', gres or "")
    return m.group(1) if m else ""


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
    node: str = ""
    gpu_count: int = 0       # GPUs allocated (from squeue %b)
    gpu_type: str = ""       # GPU model if available (e.g. 'a100')
    gpu_mem_gb: float = 0.0  # VRAM used in GB (from sstat TRESUsageInMax gres/gpumem)
    gpu_util_pct: float = 0.0  # GPU utilization % (from sstat TRESUsageInMax gres/gpuutil)
    gpu_total_gb: float = 0.0  # total VRAM per GPU from node features (e.g. gpu_A40_45G → 45)
    gpu_mem_trend: int = 0   # +1 ↑, -1 ↓, 0 — compared to previous poll


# ----------------------------- SLURM helpers ----------------------------- #


def list_live_squeue(user: str) -> List[Tuple[str, str, str, int, float, str, str, str]]:
    out = run(f'squeue -u {shlex.quote(user)} -h -o "%i|%T|%M|%C|%m|%j|%N|%b"')
    rows = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("|", 7)
        if len(parts) < 8:
            continue
        jid, state, elapsed, cpus, reqm, name, node, gres = parts
        rows.append(
            (
                jid.strip(),
                state.strip(),
                elapsed.strip(),
                int(cpus),
                metric_to_gb(reqm, "M"),
                name.strip(),
                node.strip(),
                gres.strip(),
            )
        )
    return rows


def scontrol_node_gpu_info(nodes: List[str]) -> Dict[str, Tuple[str, float]]:
    """Return {node: (gpu_model, vram_gb_per_gpu)} for each node.

    Reads AvailableFeatures=gpu_<MODEL>_<VRAM>G from a single batched
    `scontrol show node` call — no per-node loops.
    """
    if not nodes:
        return {}
    nodelist = ",".join(sorted(set(nodes)))
    out = run(f"scontrol show node {shlex.quote(nodelist)}")

    result: Dict[str, Tuple[str, float]] = {}
    current_node = ""
    for line in out.splitlines():
        nm = re.search(r'NodeName=(\S+)', line)
        if nm:
            current_node = nm.group(1)
            continue
        fm = re.search(r'AvailableFeatures=gpu_([A-Za-z0-9_]+)_(\d+)G', line)
        if fm and current_node:
            model = fm.group(1).replace("_", " ")
            vram_gb = float(fm.group(2))
            result[current_node] = (model, vram_gb)
    return result


def sstat_batch(jobids: List[str]) -> Dict[str, Dict[str, str]]:
    if not jobids:
        return {}
    jlist = ",".join(jobids)
    rows = run(
        f"sstat -j {shlex.quote(jlist)} --noheader --parsable2 "
        f"--format=JobID,AveCPU,NTasks,MaxRSS,MaxPages,MaxDiskWrite,TRESUsageInMax"
    )
    result = {}
    for ln in rows.splitlines():
        parts = ln.split("|")
        if len(parts) < 6:
            continue
        jobstep, avecpu, ntasks, mrss, mpages, mdisk = parts[:6]
        tres = parts[6].strip() if len(parts) > 6 else ""
        jid = jobstep.split(".")[0]
        result[jid] = {
            "avecpu": avecpu.strip(),
            "ntasks": ntasks.strip(),
            "maxrss": mrss.strip(),
            "maxpages": mpages.strip(),
            "maxdisk": mdisk.strip(),
            "tres_in_max": tres,
        }
    return result
