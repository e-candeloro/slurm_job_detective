from __future__ import annotations

import re
import shlex
import subprocess
from dataclasses import dataclass, field
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
RUN_TIMEOUT_SECONDS = 60.0


class SlurmCommandError(RuntimeError):
    def __init__(self, command: str, detail: str) -> None:
        self.command = command
        self.detail = detail
        super().__init__(f"{command}: {detail}")


class SlurmCommandNotFoundError(SlurmCommandError):
    def __init__(self, command: str) -> None:
        super().__init__(command, "command not found")


# ----------------------------- shell utils ----------------------------- #


def run(cmd: str, timeout: float = RUN_TIMEOUT_SECONDS) -> str:
    """Run a shell command and return stdout (stripped).

    Raises SlurmCommandError on timeout, or when the command fails without
    producing any output (partial output from e.g. a batched sstat is kept).
    """
    args = shlex.split(cmd)
    if not args:
        return ""

    command = args[0].rsplit("/", 1)[-1]
    try:
        out = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        if command in SLURM_BINARIES:
            raise SlurmCommandNotFoundError(command) from exc
        raise
    except subprocess.TimeoutExpired as exc:
        raise SlurmCommandError(command, f"timed out after {timeout:.0f}s") from exc

    stdout = out.stdout.strip()
    if out.returncode != 0 and not stdout:
        stderr_lines = (out.stderr or "").strip().splitlines()
        detail = stderr_lines[0] if stderr_lines else f"exit code {out.returncode}"
        raise SlurmCommandError(command, detail)
    return stdout


# ----------------------------- parsing utils ----------------------------- #

_TIME_PATTERNS = [
    re.compile(r"^(\d+)-(\d{1,2}):(\d{2}):(\d{2})(?:\.\d+)?$"),  # D-HH:MM:SS
    re.compile(r"^(\d{1,2}):(\d{2}):(\d{2})(?:\.\d+)?$"),  # HH:MM:SS
    re.compile(r"^(\d{1,2}):(\d{2})(?:\.\d+)?$"),  # MM:SS[.mmm]
]

# Slurm's convert_num_unit() scales every unit by 1024, including plain counts.
_UNIT_POWERS = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5}


def to_seconds(ts: str) -> int:
    """Parse [D-]HH:MM:SS, HH:MM:SS or MM:SS[.mmm] to seconds."""
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
    """Convert Slurm memory/disk metrics to GiB.

    `default_unit` applies to values without a suffix ("B" for sstat byte
    counters, "M" for squeue memory requests).
    """
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

    power = _UNIT_POWERS.get("" if unit == "B" else unit, 0)
    return val * (1024.0**power) / 1073741824.0


def parse_pages(s: str) -> int:
    """Parse a Slurm count such as '1500' or '3K' (Slurm uses 1024 multiples)."""
    s = (s or "").strip().upper()
    if not s or s == "UNKNOWN":
        return 0
    m = re.match(r"^([\d\.]+)([KMGTP]?)$", s)
    if not m:
        return 0
    return int(float(m.group(1)) * (1024 ** _UNIT_POWERS[m.group(2)]))


def parse_tres_value(tres: str, key: str) -> str:
    """Extract the raw value string for a key from a TRES string.

    e.g. parse_tres_value('gres/gpumem=4096,gres/gpuutil=72', 'gres/gpumem') -> '4096'
    """
    m = re.search(rf"(?:^|,){re.escape(key)}=([^,\s]+)", tres or "")
    return m.group(1) if m else ""


def _tres_gb(tres: str, key: str) -> float:
    return metric_to_gb(parse_tres_value(tres, key), "B")


def _tres_float(tres: str, key: str) -> float:
    try:
        return float(parse_tres_value(tres, key) or 0)
    except ValueError:
        return 0.0


_GRES_GPU_TOKEN = re.compile(r"^(?:gres[/:])?gpu(?=$|:)(?::([^:,=()]+))?(?::(\d+))?")


def parse_gres_gpus(spec: str) -> Tuple[int, str]:
    """Return (gpu_count, gpu_type) from a GRES/TRES-per-* request string.

    Handles 'gpu:2', 'gres:gpu:2', 'gres/gpu:a100:2', and types containing
    '-' or '.' such as 'gres/gpu:nvidia_a100_3g.40gb:1'.
    """
    count = 0
    gpu_type = ""
    for token in (spec or "").split(","):
        m = _GRES_GPU_TOKEN.match(token.strip())
        if not m:
            continue
        token_type, token_count = m.group(1) or "", m.group(2)
        if token_count is None and token_type.isdigit():
            token_type, token_count = "", token_type
        count += int(token_count) if token_count else 1
        gpu_type = gpu_type or token_type
    return count, gpu_type


def parse_tres_gpus(tres: str) -> Tuple[int, str]:
    """Return (gpu_count, gpu_type) from a TRES string like squeue tres-alloc.

    e.g. 'cpu=8,mem=64G,node=1,gres/gpu=2,gres/gpu:a100=2' -> (2, 'a100')
    """
    untyped = 0
    typed = 0
    gpu_type = ""
    for token in (tres or "").split(","):
        key, _, value = token.strip().partition("=")
        if key == "gres/gpu":
            untyped += _safe_int(value)
        elif key.startswith("gres/gpu:"):
            typed += _safe_int(value)
            gpu_type = gpu_type or key.split(":", 1)[1]
    return (untyped or typed), gpu_type


def first_hostname(nodelist: str) -> str:
    """Return the first host of a compressed nodelist, e.g. 'gpu[01-02]' -> 'gpu01'."""
    m = re.match(r"^([^\[,]*)(?:\[([^\],\-]+)[^\]]*\])?([^,\[]*)", (nodelist or "").strip())
    if not m:
        return ""
    return f"{m.group(1)}{m.group(2) or ''}{m.group(3)}"


def _safe_int(s: str) -> int:
    try:
        return int((s or "").strip())
    except (TypeError, ValueError):
        return 0


# ----------------------------- data model ----------------------------- #


@dataclass
class CounterMetric:
    """A cumulative counter from sstat and its rate between fresh samples."""

    total: float = 0.0
    rate_per_sec: float = 0.0
    rate_change_pct: Optional[float] = None
    reset: bool = False


@dataclass
class JobUsage:
    """Usage summed over all running steps of a job, from one sstat sample."""

    cpu_seconds: float = 0.0
    rss_gb: float = 0.0
    pages: float = 0.0
    io_read_gb: float = 0.0
    io_write_gb: float = 0.0
    gpu_mem_gb: float = 0.0  # summed across all GPUs used by the job
    gpu_util_sum_pct: float = 0.0  # summed across GPUs (2 GPUs at 50% -> 100)


@dataclass
class LiveRow:
    jobid: str  # display id; array tasks look like '123_4'
    name: str
    state: str
    elapsed: str
    cpus: int
    req_mem_gb: float  # total memory allocated to the job (all nodes)
    stat_jobid: str = ""  # unique numeric id used to query sstat
    node: str = ""
    gpu_count: int = 0  # total GPUs allocated (all nodes)
    gpu_type: str = ""  # GPU model if available (e.g. 'a100')
    gpu_total_gb: float = 0.0  # VRAM per GPU from node features (gpu_A40_45G -> 45)
    sampled: bool = False  # sstat data is available for this job
    has_history: bool = False  # a previous fresh sample exists for comparison
    cpu_eff_pct: float = 0.0
    cpu_eff_change_pct: Optional[float] = None
    maxrss_gb: float = 0.0  # best lower bound of the job's peak total RSS
    maxrss_change_pct: Optional[float] = None
    gpu_mem_gb: float = 0.0  # VRAM used, summed across GPUs
    gpu_util_pct: float = 0.0  # mean utilization per allocated GPU
    gpu_vram_change_pct: Optional[float] = None
    pages: CounterMetric = field(default_factory=CounterMetric)
    io_read: CounterMetric = field(default_factory=CounterMetric)
    io_write: CounterMetric = field(default_factory=CounterMetric)

    @property
    def gpu_capacity_gb(self) -> float:
        return self.gpu_total_gb * self.gpu_count

    @property
    def gpu_vram_pct(self) -> float:
        if self.gpu_capacity_gb > 0:
            pct = 100.0 * self.gpu_mem_gb / self.gpu_capacity_gb
        else:
            pct = self.gpu_util_pct
        return max(0.0, min(100.0, pct))


# ----------------------------- SLURM helpers ----------------------------- #

# squeue -O pads every field to its size; '|' suffixes make parsing unambiguous.
# Name comes last so a '|' inside a job name cannot shift the other columns.
_SQUEUE_FIELDS = [
    ("JobArrayId", 64),  # same as %i: '123_4' for array tasks
    ("JobId", 64),  # same as %A: unique id per array task, used for sstat
    ("State", 32),
    ("TimeUsed", 32),
    ("NumCPUs", 16),
    ("NumNodes", 16),
    ("MinMemory", 32),
    ("tres-alloc", 1024),  # allocated TRES if running, requested otherwise
    ("tres-per-job", 512),
    ("tres-per-node", 512),
    ("NodeList", 1024),
    ("Name", 512),
]


def list_live_squeue(user: str) -> List[LiveRow]:
    fmt = ",".join(
        f"{name}:{size}" + ("|" if i < len(_SQUEUE_FIELDS) - 1 else "")
        for i, (name, size) in enumerate(_SQUEUE_FIELDS)
    )
    out = run(f"squeue -u {shlex.quote(user)} -h -O {shlex.quote(fmt)}")
    rows: List[LiveRow] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = [p.strip() for p in line.split("|", len(_SQUEUE_FIELDS) - 1)]
        if len(parts) < len(_SQUEUE_FIELDS):
            continue
        (
            jid,
            raw_jid,
            state,
            elapsed,
            cpus,
            num_nodes,
            min_mem,
            tres_alloc,
            tres_per_job,
            tres_per_node,
            node,
            name,
        ) = parts

        # tres-alloc covers --mem and --mem-per-cpu alike and spans all nodes;
        # MinMemory may be a per-CPU value, so it is only a fallback.
        alloc_mem = parse_tres_value(tres_alloc, "mem")
        req_mem_gb = metric_to_gb(alloc_mem or min_mem, "M")

        gpu_count, gpu_type = parse_tres_gpus(tres_alloc)
        job_count, job_type = parse_gres_gpus(tres_per_job)
        node_count, node_type = parse_gres_gpus(tres_per_node)
        if not gpu_count:
            gpu_count = job_count or node_count * max(_safe_int(num_nodes), 1)
        gpu_type = gpu_type or job_type or node_type

        rows.append(
            LiveRow(
                jobid=jid,
                name=name,
                state=state,
                elapsed=elapsed,
                cpus=_safe_int(cpus),
                req_mem_gb=req_mem_gb,
                stat_jobid=raw_jid or jid,
                node=node,
                gpu_count=gpu_count,
                gpu_type=gpu_type,
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


SSTAT_FORMAT = "JobID,AveCPU,NTasks,TRESUsageInMax,TRESUsageInTot,TRESUsageOutTot"


def sstat_batch(jobids: List[str]) -> Dict[str, JobUsage]:
    """Return per-job usage summed over all running steps, keyed by job id."""
    if not jobids:
        return {}
    jlist = ",".join(jobids)
    rows = run(
        f"sstat -a -j {shlex.quote(jlist)} --noheader --parsable2 --noconvert "
        f"--format={SSTAT_FORMAT}"
    )
    result: Dict[str, JobUsage] = {}
    for ln in rows.splitlines():
        parts = [p.strip() for p in ln.split("|")]
        if len(parts) < 6:
            continue
        jobstep, avecpu, ntasks, tres_max, tres_tot, tres_out_tot = parts[:6]
        usage = result.setdefault(jobstep.split(".")[0], JobUsage())

        # Steps (.batch/.extern/.0/...) are accounted separately, so the job
        # total is the sum over its running steps.
        usage.cpu_seconds += to_seconds(avecpu) * _safe_int(ntasks)

        # TRESUsageInMax holds the peak of the single largest task, while
        # TRESUsageInTot holds the current sum over all tasks; the larger one
        # is the best available lower bound of the step's peak total.
        usage.rss_gb += max(_tres_gb(tres_max, "mem"), _tres_gb(tres_tot, "mem"))
        usage.gpu_mem_gb += max(
            _tres_gb(tres_max, "gres/gpumem"), _tres_gb(tres_tot, "gres/gpumem")
        )
        usage.gpu_util_sum_pct += max(
            _tres_float(tres_max, "gres/gpuutil"),
            _tres_float(tres_tot, "gres/gpuutil"),
        )

        # Cumulative counters: totals over all tasks give the real throughput.
        usage.pages += parse_pages(parse_tres_value(tres_tot, "pages"))
        usage.io_read_gb += _tres_gb(tres_tot, "fs/disk")
        usage.io_write_gb += _tres_gb(tres_out_tot, "fs/disk")
    return result
