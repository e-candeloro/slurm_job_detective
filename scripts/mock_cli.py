import sys
from unittest.mock import patch

from sjdet.cli import main


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
        # JobStep|AveCPU|NTasks|MaxRSS|MaxPages|MaxDiskWrite|TRESUsageInMax
        # Note: gpumem is raw bytes, gpuutil is %
        return (
            "1001.0|05:00:00|1|45000000K|1500|500M|gres/gpumem=50000000000,gres/gpuutil=92\n"
            "1002.0|00:10:00|1|10240000K|250|10M|\n"
            "1004.0|15-00:00:00|1|120000000K|50000|10G|gres/gpumem=25000000000,gres/gpuutil=45\n"
        )

    return ""


if __name__ == "__main__":
    # We patch run in both modules just in case
    with patch("sjdet.cli.run", side_effect=fake_run), \
         patch("sjdet.slurm.run", side_effect=fake_run):
        
        # Override sys.argv so we don't accidentally parse real args, 
        # though you can pass flags like `python scripts/mock_cli.py --headroom 0.5`
        if len(sys.argv) == 1:
            sys.argv.extend(["--force-update-nodes"])

        print("--- Running Fake Slurm CLI Session ---")
        main()
