# 🕵️ SLURM Job Detective

Live CPU and memory stats for your running and pending SLURM jobs — in your terminal, instantly.

## What it does

- Shows all your **RUNNING** and **PENDING** jobs in one table
- CPU efficiency bar (how well you're using requested cores)
- Memory usage vs. requested, with a **suggested `--mem`** based on actual MaxRSS
- Tracks MaxPages and MaxDiskWrite trends (↑ / ↓) between polls
- Batches all `sstat` calls into a single request — no per-job RPC spam
- Throttles polling with a local cache (default: 60s minimum interval)

## Install

No root needed. Requires [`uv`](https://docs.astral.sh/uv/) (recommended) or [`pipx`](https://pipx.pypa.io).

Both methods install `sjdet` to `~/.local/bin` — just type `sjdet` from anywhere, no virtual environment activation needed.

### uv (recommended)
```bash
# install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
# install the tool
uv tool install git+https://github.com/e-candeloro/slurm_job_detective
```
### pipx
```bash
# install pipx
python3 -m pip install --user pipx
pipx install git+https://github.com/e-candeloro/slurm_job_detective
```

## Usage

```bash
sjdet                          # auto-detects $USER
sjdet --user alice             # specific user
sjdet --max-jobs 20            # show more jobs (default: 10)
sjdet --interval 120           # cache TTL in seconds (min: 60)
sjdet --headroom 0.30          # suggest mem with 30% headroom (default: 20%)
```

## Development

```bash
git clone https://github.com/YOU/slurm-job-detective
cd slurm-job-detective
uv sync          # creates .venv and installs in editable mode
uv run sjdet     # run directly without activating the venv
```

## Project layout

```
src/sjdet/
├── cli.py      ← argument parsing, main() entrypoint
├── slurm.py    ← squeue/sstat calls, data model, parsing utils
├── display.py  ← rich table and progress bar rendering
└── cache.py    ← local JSON cache for throttling sstat calls
```
