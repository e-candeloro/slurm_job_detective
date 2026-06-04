# SLURM sstat Reference

Official documentation:
- https://slurm.schedmd.com/sstat.html

Purpose:
- `sstat` displays status information for running SLURM jobs and steps.

Fields currently used by this repository:
- `JobID`
- `AveCPU`
- `NTasks`
- `MaxRSS`
- `MaxPages`
- `MaxDiskWrite`
- `MaxDiskRead`
- `TRESUsageInMax`

Project command shape:
```bash
sstat -a -j <jobids> --noheader --parsable2 --format=JobID,AveCPU,NTasks,MaxRSS,MaxPages,MaxDiskWrite,MaxDiskRead,TRESUsageInMax
```

Operational notes from official docs:
- Metric availability depends on `jobacct_gather` plugins and configuration.
- Avoid excessive `sstat` polling loops to reduce load on `slurmctld`.
- Use parsable output (`--parsable2`) for script-friendly parsing.
- Use `--allsteps`/`-a` so batch jobs report their `.batch` step instead of only the lowest numbered running step.

See also:
- https://slurm.schedmd.com/sacct.html
