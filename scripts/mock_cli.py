import sys
import json
import time
import os
from unittest.mock import patch

from sjdet.cache import cache_file
from sjdet.cli import SNAPSHOT_VERSION, main
from sjdet.slurm import _SQUEUE_FIELDS


MOCK_USER = "fake_user"
GiB = 1024**3

# JobArrayId, JobId, State, TimeUsed, NumCPUs, NumNodes, MinMemory, tres-alloc,
# tres-per-job, tres-per-node, NodeList, Name  (same order as `squeue -O ...`)
SQUEUE_JOBS = [
    # --gres=gpu:a100:2
    ("1001", "1001", "RUNNING", "1:15:30", "8", "1", "64G",
     "cpu=8,mem=64G,node=1,billing=8,gres/gpu=2,gres/gpu:a100=2",
     "N/A", "gres/gpu:a100:2", "gpu-node-01", "train_resnet"),
    # --mem-per-cpu=4G: MinMemory is per CPU, tres-alloc holds the 16G total
    ("1002", "1002", "RUNNING", "5:10", "4", "1", "4G",
     "cpu=4,mem=16G,node=1,billing=4", "N/A", "N/A", "cpu-node-05", "data_prep"),
    ("1003", "1003", "PENDING", "0:00", "2", "1", "8G",
     "cpu=2,mem=8G,node=1,billing=2,gres/gpu=1", "N/A", "gres/gpu:v100:1",
     "", "eval_model"),
    # --gpus=4 (tres-per-job only) with 4 tasks
    ("1004", "1004", "RUNNING", "2-05:00:00", "16", "1", "128G",
     "cpu=16,mem=128G,node=1,billing=16,gres/gpu=4", "gres/gpu:4", "N/A",
     "gpu-node-02", "big_train"),
    # array task: display id 1005_3, sstat id 1010
    ("1005_3", "1010", "RUNNING", "20:00", "2", "1", "8G",
     "cpu=2,mem=8G,node=1,billing=2", "N/A", "N/A", "cpu-node-05", "sweep"),
    # 2 nodes x 4 GPUs
    ("1006", "1006", "RUNNING", "1:00:00", "32", "2", "100G",
     "cpu=32,mem=200G,node=2,billing=32,gres/gpu=8", "N/A", "gres/gpu:a100:4",
     "gpu-node-[03-04]", "dist_train"),
]

# JobID|AveCPU|NTasks|TRESUsageInMax|TRESUsageInTot|TRESUsageOutTot (--noconvert)
SSTAT_SAMPLE = "\n".join(
    [
        f"1001.batch|00:00:05|1|mem={200 * 2**20},pages=10,fs/disk={2**20}"
        f"|mem={200 * 2**20},pages=10,fs/disk={2**20}|fs/disk={2**19}",
        f"1001.0|05:00:00|1|mem={45 * GiB},pages=1500,fs/disk={500 * 2**20},"
        f"gres/gpumem={int(46.6 * GiB)},gres/gpuutil=150"
        f"|mem={43 * GiB},pages=1500,fs/disk={500 * 2**20},"
        f"gres/gpumem={45 * GiB},gres/gpuutil=140|fs/disk={250 * 2**20}",
        f"1002.batch|00:10:00|1|mem={int(9.8 * GiB)},pages=250,fs/disk={3 * 2**20}"
        f"|mem={int(9.5 * GiB)},pages=250,fs/disk={3 * 2**20}|fs/disk={10 * 2**20}",
        f"1004.batch|00:00:01|1|mem={50 * 2**20},pages=0,fs/disk=0"
        f"|mem={50 * 2**20},pages=0,fs/disk=0|fs/disk=0",
        f"1004.0|3-18:00:00|4|mem={30 * GiB},pages=20000,fs/disk={2 * GiB},"
        f"gres/gpumem={25 * GiB},gres/gpuutil=45"
        f"|mem={118 * GiB},pages=50000,fs/disk={2 * GiB},"
        f"gres/gpumem={92 * GiB},gres/gpuutil=180|fs/disk={10 * GiB}",
        f"1010.batch|00:15:00|1|mem={GiB},pages=5,fs/disk={2**20}"
        f"|mem={GiB},pages=5,fs/disk={2**20}|fs/disk={2**20}",
        f"1006.0|01:00:00|2|mem={40 * GiB},pages=100,fs/disk={GiB},"
        f"gres/gpumem={160 * GiB},gres/gpuutil=320"
        f"|mem={80 * GiB},pages=200,fs/disk={2 * GiB},"
        f"gres/gpumem={300 * GiB},gres/gpuutil=600|fs/disk={GiB}",
    ]
)


def _counter(total: float, rate: float) -> dict:
    return {"total": total, "rate_per_sec": rate, "rate_change_pct": None, "reset": False}


def _seed_mock_cache() -> None:
    prev_ts = time.time() - 60.0
    seeded = {
        "ts": 0,
        "joblist": "",
        "node_info": {
            "gpu-node-01": ["A100", 80.0],
            "gpu-node-02": ["v100", 0.0],
        },
        "metric_snapshot": {
            "version": SNAPSHOT_VERSION,
            "jobs": {
                "1001": {
                    "ts": prev_ts,
                    "cpu_eff_pct": 45.0,
                    "maxrss_gb": 41.0,
                    "gpu_vram_pct": 27.0,
                    "pages": _counter(1370.0, 3.2),
                    "io_read": _counter(0.43, 0.00100),
                    "io_write": _counter(0.20, 0.00035),
                },
                "1002": {
                    "ts": prev_ts,
                    "cpu_eff_pct": 60.0,
                    "maxrss_gb": 11.0,
                    "gpu_vram_pct": 0.0,
                    "pages": _counter(218.0, 0.9),
                    "io_read": _counter(0.0019296875, 0.00001),
                    "io_write": _counter(0.009765625, 0.0),
                },
                "1004": {
                    "ts": prev_ts,
                    "cpu_eff_pct": 42.45,
                    "maxrss_gb": 114.0,
                    "gpu_vram_pct": 45.0,
                    "pages": _counter(49200.0, 20.0),
                    "io_read": _counter(1.60, 0.006),
                    "io_write": _counter(9.58, 0.007),
                },
            },
        },
    }
    path = cache_file(MOCK_USER)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(seeded))


def _cleanup_mock_cache() -> None:
    try:
        cache_file(MOCK_USER).unlink(missing_ok=True)
    except Exception:
        pass


def _squeue_output() -> str:
    lines = []
    for job in SQUEUE_JOBS:
        cells = []
        for i, (value, (_, size)) in enumerate(zip(job, _SQUEUE_FIELDS)):
            suffix = "|" if i < len(_SQUEUE_FIELDS) - 1 else ""
            cells.append(value.ljust(size) + suffix)
        lines.append("".join(cells))
    return "\n".join(lines)


def fake_run(cmd: str, timeout: float = 0.0) -> str:
    """Mock the slurm commands to return fake data."""
    if cmd == "whoami":
        return MOCK_USER

    if cmd.startswith("squeue"):
        return _squeue_output()

    if cmd.startswith("scontrol"):
        return (
            "NodeName=gpu-node-01\n"
            "AvailableFeatures=broadwell,gpu_A100_80G\n"
            "Gres=gpu:a100:2\n"
            "NodeName=gpu-node-02\n"
            "AvailableFeatures=haswell,some_weird_gpu\n"  # no VRAM info -> util fallback
            "Gres=gpu:v100:4\n"
            "NodeName=gpu-node-03\n"
            "AvailableFeatures=icelake,gpu_A100_80G\n"
            "Gres=gpu:a100:4\n"
        )

    if cmd.startswith("sstat"):
        return SSTAT_SAMPLE

    return ""


if __name__ == "__main__":
    os.environ.setdefault("COLUMNS", "240")
    os.environ["USER"] = MOCK_USER
    _seed_mock_cache()
    with (
        patch("sjdet.cli.run", side_effect=fake_run),
        patch("sjdet.slurm.run", side_effect=fake_run),
    ):
        # You can pass flags like `python scripts/mock_cli.py --headroom 0.5`
        if len(sys.argv) == 1:
            sys.argv.extend(["--force-update-nodes"])

        print("--- Running Fake Slurm CLI Session (single-run seeded deltas) ---")
        try:
            main()
        finally:
            _cleanup_mock_cache()
