from __future__ import annotations

import re
import shlex
import subprocess
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


SLURM_BINARIES = {
    "squeue",
    "sstat",
    "scontrol",
    "sacct",
    "sinfo",
    "srun",
    "sbatch",
    "salloc",
    "scancel",
}


class SlurmCommandNotFoundError(RuntimeError):
    def __init__(self, command: str) -> None:
        self.command = command
        super().__init__(f"SLURM command not found: {command}")


# ----------------------------- shell utils ----------------------------- #


def run(cmd: str) -> str:
    """Run a shell command and return stdout (stripped)."""
    args = shlex.split(cmd)
    if not args:
        return ""

    try:
        out = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            check=False,
        )
    except FileNotFoundError as exc:
        command = args[0].rsplit("/", 1)[-1]
        if command in SLURM_BINARIES:
            raise SlurmCommandNotFoundError(command) from exc
        raise
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
    m = re.search(rf"(?:^|,){re.escape(key)}=([^,\s]+)", tres or "")
    return m.group(1) if m else ""


def parse_gres_gpu_count(gres: str) -> int:
    """Extract GPU count from squeue %b GRES string.

    Handles 'gres/gpu:1' and 'gres/gpu:a100:2' formats.
    """
    m = re.search(r"gres/gpu(?::[a-zA-Z0-9_]+)?:(\d+)", gres or "")
    return int(m.group(1)) if m else 0


def parse_gres_gpu_type(gres: str) -> str:
    """Extract GPU type from GRES string, e.g. 'a100' from 'gres/gpu:a100:2'."""
    m = re.search(r"gres/gpu:([a-zA-Z][a-zA-Z0-9_]*)?:(\d+)", gres or "")
    return m.group(1) if m else ""


def _safe_int(s: str) -> int:
    try:
        return int((s or "").strip())
    except (TypeError, ValueError):
        return 0


def _tres_score(tres: str) -> Tuple[float, float, int]:
    """Score TRESUsageInMax strings to keep the most informative step row."""
    gpu_mem = metric_to_gb(parse_tres_value(tres, "gres/gpumem"), "B")
    try:
        gpu_util = float(parse_tres_value(tres, "gres/gpuutil") or 0)
    except ValueError:
        gpu_util = 0.0
    return gpu_mem, gpu_util, len((tres or "").strip())


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
    maxpages_delta: float = 0.0
    maxpages_rate_per_sec: float = 0.0
    maxpages_rate_change_pct: Optional[float] = None
    maxpages_reset: bool = False
    maxdisk_gb: float = 0.0
    maxdisk_trend: int = 0
    maxdisk_delta_gb: float = 0.0
    maxdisk_rate_gb_per_sec: float = 0.0
    maxdisk_rate_change_pct: Optional[float] = None
    maxdisk_reset: bool = False
    maxdiskread_gb: float = 0.0
    maxdiskread_trend: int = 0
    maxdiskread_delta_gb: float = 0.0
    maxdiskread_rate_gb_per_sec: float = 0.0
    maxdiskread_rate_change_pct: Optional[float] = None
    maxdiskread_reset: bool = False
    cpu_eff_change_pct: Optional[float] = None
    maxrss_change_pct: Optional[float] = None
    gpu_vram_change_pct: Optional[float] = None
    node: str = ""
    gpu_count: int = 0  # GPUs allocated (from squeue %b)
    gpu_type: str = ""  # GPU model if available (e.g. 'a100')
    gpu_mem_gb: float = 0.0  # VRAM used in GB (from sstat TRESUsageInMax gres/gpumem)
    gpu_util_pct: float = (
        0.0  # GPU utilization % (from sstat TRESUsageInMax gres/gpuutil)
    )
    gpu_total_gb: float = (
        0.0  # total VRAM per GPU from node features (e.g. gpu_A40_45G → 45)
    )
    gpu_mem_trend: int = 0  # +1 ↑, -1 ↓, 0 — compared to previous poll


# ----------------------------- SLURM helpers ----------------------------- #


def list_live_squeue(
    user: str,
) -> List[Tuple[str, str, str, int, float, str, str, str]]:
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
    node_blocks = re.split(r"(?=NodeName=)", out)

    for block in node_blocks:
        nm = re.search(r"NodeName=(\S+)", block)
        if not nm:
            continue
        current_node = nm.group(1)

        # Try standard format like AvailableFeatures=gpu_A40_45G
        fm = re.search(r"AvailableFeatures=[^\s]*\bgpu_([A-Za-z0-9_]+)_(\d+)G\b", block)
        if fm:
            model = fm.group(1).replace("_", " ")
            vram_gb = float(fm.group(2))
            result[current_node] = (model, vram_gb)
            continue

        # Try generic patterns in the block
        vram = 0.0
        # Search for sequences like 40GB, 80_GB, 45G, etc in AvailableFeatures or Gres
        vm = re.search(r"\b(\d+)_?GB\b", block, re.IGNORECASE)
        if vm:
            vram = float(vm.group(1))
        else:
            vm = re.search(r"\b(?i:vram)_?(\d+)G\b", block, re.IGNORECASE)
            if vm:
                vram = float(vm.group(1))

        # Try to guess model from Gres like gpu:a100:2
        model = ""
        gm = re.search(r"Gres=[^\s]*\bgpu:([A-Za-z0-9_]+):", block)
        if gm:
            model = gm.group(1).replace("_", " ")
        else:
            gm2 = re.search(r"AvailableFeatures=[^\s]*\bgpu_([A-Za-z0-9_]+)\b", block)
            if gm2:
                model = gm2.group(1).replace("_", " ")

        if model or vram > 0:
            result[current_node] = (model, vram)

    return result


def sstat_batch(jobids: List[str]) -> Dict[str, Dict[str, str]]:
    if not jobids:
        return {}
    jlist = ",".join(jobids)
    rows = run(
        f"sstat -j {shlex.quote(jlist)} --noheader --parsable2 "
        f"--format=JobID,AveCPU,NTasks,MaxRSS,MaxPages,MaxDiskWrite,MaxDiskRead,TRESUsageInMax"
    )
    result: Dict[str, Dict[str, str]] = {}
    for ln in rows.splitlines():
        parts = ln.split("|")
        if len(parts) < 6:
            continue

        jobstep, avecpu, ntasks, mrss, mpages, mdisk = parts[:6]
        mdiskread = ""
        tres = ""

        if len(parts) > 7:
            mdiskread = parts[6].strip()
            tres = parts[7].strip()
        elif len(parts) == 7:
            # Backward compatibility for old output shape without MaxDiskRead.
            candidate = parts[6].strip()
            if "=" in candidate or "gres/" in candidate:
                tres = candidate
            else:
                mdiskread = candidate

        jid = jobstep.split(".")[0]
        row = {
            "avecpu": avecpu.strip(),
            "ntasks": ntasks.strip(),
            "maxrss": mrss.strip(),
            "maxpages": mpages.strip(),
            "maxdisk": mdisk.strip(),
            "maxdiskread": mdiskread,
            "tres_in_max": tres,
        }

        merged = result.setdefault(
            jid,
            {
                "avecpu": "",
                "ntasks": "0",
                "maxrss": "",
                "maxpages": "",
                "maxdisk": "",
                "maxdiskread": "",
                "tres_in_max": "",
            },
        )

        has_payload = any(
            row[k] not in {"", "UNKNOWN"}
            for k in ("maxrss", "maxpages", "maxdisk", "maxdiskread", "tres_in_max")
        )

        # sstat may emit multiple lines per job (e.g. .batch/.extern/.0).
        # Keep per-field maxima so sparse rows cannot erase real usage.
        if has_payload and (
            not merged["avecpu"]
            or to_seconds(row["avecpu"]) > to_seconds(merged["avecpu"])
        ):
            merged["avecpu"] = row["avecpu"]
        if _safe_int(row["ntasks"]) > _safe_int(merged["ntasks"]):
            merged["ntasks"] = row["ntasks"]
        if row["maxrss"] and (
            not merged["maxrss"]
            or metric_to_gb(row["maxrss"], "K") > metric_to_gb(merged["maxrss"], "K")
        ):
            merged["maxrss"] = row["maxrss"]
        if row["maxpages"] and (
            not merged["maxpages"]
            or parse_pages(row["maxpages"]) > parse_pages(merged["maxpages"])
        ):
            merged["maxpages"] = row["maxpages"]
        if row["maxdisk"] and (
            not merged["maxdisk"]
            or metric_to_gb(row["maxdisk"], "B") > metric_to_gb(merged["maxdisk"], "B")
        ):
            merged["maxdisk"] = row["maxdisk"]
        if row["maxdiskread"] and (
            not merged["maxdiskread"]
            or metric_to_gb(row["maxdiskread"], "B")
            > metric_to_gb(merged["maxdiskread"], "B")
        ):
            merged["maxdiskread"] = row["maxdiskread"]
        if _tres_score(row["tres_in_max"]) > _tres_score(merged["tres_in_max"]):
            merged["tres_in_max"] = row["tres_in_max"]
    return result
