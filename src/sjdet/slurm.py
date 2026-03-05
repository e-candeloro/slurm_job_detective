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
