import sys
import json
import time
import os
from pathlib import Path
from unittest.mock import patch

from sjdet.cli import main


CACHE_DIR = Path.home() / ".cache" / "slurm_stats"
SSTAT_SAMPLE = (
    "1001.0|05:00:00|1|45000000K|1500|500M|250M|gres/gpumem=50000000000,gres/gpuutil=92\n"
    "1002.0|00:10:00|1|10240000K|250|10M|3M|\n"
    "1004.0|15-00:00:00|1|120000000K|50000|10G|2G|gres/gpumem=25000000000,gres/gpuutil=45\n"
)


def _seed_mock_cache() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    seeded = {
        "ts": 0,
        "joblist": "1001,1002,1004",
        "data": {},
        "node_info": {
            "gpu-node-01": ["A100", 80.0],
            "gpu-node-02": ["v100", 0.0],
        },
        "metric_snapshot": {
            "jobs": {
                "1001": {
                    "ts": time.time() - 60.0,
                    "cpu_eff_pct": 45.0,
                    "maxrss_gb": 39.0,
                    "gpu_vram_pct": 54.0,
                    "maxpages": 1368.0,
                    "maxdisk_gb": 0.42828125,
                    "maxdiskread_gb": 0.21828125,
                    "maxpages_rate_per_sec": 3.20,
                    "maxdisk_rate_gb_per_sec": 0.00100,
                    "maxdiskread_rate_gb_per_sec": 0.00035,
                },
                "1002": {
                    "ts": time.time() - 60.0,
                    "cpu_eff_pct": 60.0,
                    "maxrss_gb": 11.0,
                    "gpu_vram_pct": 0.0,
                    "maxpages": 218.0,
                    "maxdisk_gb": 0.009765625,
                    "maxdiskread_gb": 0.0019296875,
                    "maxpages_rate_per_sec": 0.90,
                    "maxdisk_rate_gb_per_sec": 0.0,
                    "maxdiskread_rate_gb_per_sec": 0.00001,
                },
                "1004": {
                    "ts": time.time() - 60.0,
                    "cpu_eff_pct": 42.45,
                    "maxrss_gb": 114.44091796875,
                    "gpu_vram_pct": 45.0,
                    "maxpages": 49200.0,
                    "maxdisk_gb": 9.58,
                    "maxdiskread_gb": 1.60,
                    "maxpages_rate_per_sec": 20.00,
                    "maxdisk_rate_gb_per_sec": 0.00700,
                    "maxdiskread_rate_gb_per_sec": 0.00600,
                },
            }
        },
    }

    for cache_name in ("live_cache_default.json", "live_cache_fake_user.json"):
        try:
            (CACHE_DIR / cache_name).write_text(json.dumps(seeded))
        except Exception:
            pass


def _cleanup_mock_cache() -> None:
    for cache_name in (
        "live_cache_default.json",
        "live_cache_fake_user.json",
        "mock_cli_state.json",
    ):
        cache_file = CACHE_DIR / cache_name
        try:
            if cache_file.exists():
                cache_file.unlink()
        except Exception:
            pass


def fake_run(cmd: str) -> str:
    """Mock the slurm commands to return fake data."""
    if cmd == "whoami":
        return "fake_user"

    if cmd.startswith("squeue"):
        # JobID|State|Elapsed|CPUs|ReqMem|Name|Node|Gres
        # %i|%T|%M|%C|%m|%j|%N|%b
        return (
            "1001|RUNNING|01:15:30|8|64G|train_resnet|gpu-node-01|gres/gpu:a100:2\n"
            "1002|RUNNING|00:05:10|4|16G|data_prep|cpu-node-05|\n"
            "1003|PENDING|00:00:00|2|8G|eval_model||gres/gpu:v100:1\n"
            "1004|RUNNING|2-05:00:00|16|128G|big_train|gpu-node-02|gres/gpu:4\n"
        )

    if cmd.startswith("scontrol"):
        # We simulate the nodes returned in the fake squeue above
        return (
            "NodeName=gpu-node-01\n"
            "AvailableFeatures=broadwell,gpu_A100_80G\n"
            "Gres=gpu:a100:2\n"
            "NodeName=gpu-node-02\n"
            "AvailableFeatures=haswell,some_weird_gpu\n"  # missing explicitly formatted vram or regex matches
            "Gres=gpu:v100:4\n"
        )

    if cmd.startswith("sstat"):
        # JobStep|AveCPU|NTasks|MaxRSS|MaxPages|MaxDiskWrite|MaxDiskRead|TRESUsageInMax
        return SSTAT_SAMPLE

    return ""


if __name__ == "__main__":
    os.environ.setdefault("COLUMNS", "240")
    _seed_mock_cache()
    # We patch run in both modules just in case
    with (
        patch("sjdet.cli.run", side_effect=fake_run),
        patch("sjdet.slurm.run", side_effect=fake_run),
    ):
        # Override sys.argv so we don't accidentally parse real args,
        # though you can pass flags like `python scripts/mock_cli.py --headroom 0.5`
        if len(sys.argv) == 1:
            sys.argv.extend(["--force-update-nodes"])

        print("--- Running Fake Slurm CLI Session (single-run seeded deltas) ---")
        try:
            main()
        finally:
            _cleanup_mock_cache()
