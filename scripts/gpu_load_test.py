#!/usr/bin/env python3
"""Burn GPU VRAM and compute to make sjdet GPU columns show real values.

Usage (inside a SLURM GPU job):
    python scripts/gpu_load_test.py            # ~4 GB VRAM, moderate util
    python scripts/gpu_load_test.py --gb 8     # request more VRAM
    python scripts/gpu_load_test.py --seconds 300
"""
import argparse
import time

import torch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gb", type=float, default=4.0, help="VRAM to allocate in GB")
    parser.add_argument("--seconds", type=int, default=120, help="How long to run")
    args = parser.parse_args()

    if not torch.cuda.is_available():
        print("No CUDA device visible — are you inside a GPU SLURM job?")
        return

    device = torch.device("cuda")
    print(f"Device: {torch.cuda.get_device_name(device)}")
    print(f"Allocating ~{args.gb:.1f} GB VRAM for {args.seconds}s ...")

    # Allocate a large tensor to fill VRAM
    n = int((args.gb * 1024**3) / 4)  # float32 = 4 bytes
    x = torch.randn(n, device=device)

    print("Running matmuls to drive utilization — run `sjdet` in another terminal.")
    t0 = time.time()
    while time.time() - t0 < args.seconds:
        # Small repeated matmul to keep SM utilization up
        a = torch.randn(2048, 2048, device=device)
        b = torch.randn(2048, 2048, device=device)
        _ = a @ b
        elapsed = time.time() - t0
        free, total = torch.cuda.mem_get_info(device)
        used_gb = (total - free) / 1024**3
        print(f"\r  {elapsed:5.0f}s  VRAM used: {used_gb:.2f} GB / {total/1024**3:.0f} GB", end="", flush=True)

    print("\nDone.")


if __name__ == "__main__":
    main()
