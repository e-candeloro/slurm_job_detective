# SLURM sstat Reference

Official documentation:
- https://slurm.schedmd.com/sstat.html

Purpose:
- `sstat` displays status information for running SLURM jobs and steps.

Fields currently used by this repository:
- `JobID`
- `AveCPU`
- `NTasks`
- `TRESUsageInMax`
- `TRESUsageInTot`
- `TRESUsageOutTot`

Project command shape:
```bash
sstat -a -j <jobids> --noheader --parsable2 --noconvert --format=JobID,AveCPU,NTasks,TRESUsageInMax,TRESUsageInTot,TRESUsageOutTot
```

Semantics this project relies on (checked against the Slurm docs and source):
- `*Max` TRES values are the peak of the single largest task; `*Tot` values are the sum over all tasks
  (https://slurm.schedmd.com/accounting.html). `MaxRSS` is the same as `TRESUsageInMax` `mem`.
- `AveCPU` is the average CPU time per task, so `AveCPU * NTasks` is the step's total CPU time.
- Each step (`.batch`, `.extern`, `.0`, ...) is accounted separately; job usage is summed over steps.
  Completed steps are no longer reported by `sstat`.
- `fs/disk` comes from `rchar`/`wchar` in `/proc/<pid>/io` (all read/write syscalls, not only block I/O).
- `gres/gpumem` and `gres/gpuutil` are summed over all GPUs a task uses (`gpu/nvml` plugin).
- Without `--noconvert`, values are only converted when the division by 1024 is exact; with it,
  byte counters are raw bytes and counts are raw counts.
- Non-root users can only query their own jobs.

Operational notes from official docs:
- Metric availability depends on `jobacct_gather` plugins and configuration.
- Avoid excessive `sstat` polling loops to reduce load on `slurmctld`.
- Use parsable output (`--parsable2`) for script-friendly parsing.
- Use `--allsteps`/`-a` so batch jobs report their `.batch` step instead of only the lowest numbered running step.

See also:
- https://slurm.schedmd.com/sacct.html
