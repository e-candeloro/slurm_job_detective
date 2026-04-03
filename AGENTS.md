# AGENTS.md

This file gives coding agents the minimum repository-specific context needed to work safely and effectively in this project.

## Repository Overview

- Project name: `slurm-job-detective`
- Language: Python
- Package manager / runner: `uv`
- Build backend: Hatchling
- Python requirement: `>=3.10`
- Main dependency: `rich`
- Purpose: inspect live SLURM jobs and render a Rich-based terminal table with CPU, memory, disk, and GPU diagnostics.

## Important Rule Files

- `AGENTS.md` is the active workspace instruction file for this repository.
- Keep using only one workspace instruction type (`AGENTS.md` or `.github/copilot-instructions.md`), not both.
- No Cursor/Windsurf/Cliner rules are currently present in this repository.

## Documentation Links

- For user-facing usage and installation details, prefer linking to `README.md` instead of repeating large sections.
- SLURM stats reference (sstat): https://slurm.schedmd.com/sstat.html
- Local sstat notes for this repo: `SLURM_SSTAT_REFERENCE.md`
- Keep this file focused on agent-critical development behavior and repository conventions.

## Project Layout

- `src/sjdet/cli.py`: argument parsing and top-level orchestration.
- `src/sjdet/slurm.py`: shell execution, SLURM parsing helpers, and the `LiveRow` dataclass.
- `src/sjdet/display.py`: Rich rendering and table formatting.
- `src/sjdet/cache.py`: local JSON cache helpers.
- `src/sjdet/__main__.py`: module entry point.
- `scripts/mock_cli.py`: local mocked CLI runner without a real SLURM scheduler.
- `scripts/gpu_load_test.py`: GPU stress helper for manual verification on a real cluster.

## Setup Commands

Use `uv` for all routine development commands.

```bash
uv sync --dev
```

Notes:

- `uv sync --dev` creates `.venv` and installs runtime + dev dependencies.
- The current `dev` dependency group includes `torch`, mainly for `scripts/gpu_load_test.py`.
- The first sync may be large because CUDA-enabled Torch wheels can be downloaded.

## Run Commands

Primary CLI entry points:

```bash
uv run sjdet
uv run python -m sjdet
```

Useful variants:

```bash
uv run sjdet --user alice
uv run sjdet --max-jobs 20
uv run sjdet --interval 120
uv run sjdet --headroom 0.30
uv run sjdet --force-update-nodes
uv run sjdet --clear-cache
```

Mocked local demo:

```bash
uv run python scripts/mock_cli.py
```

GPU manual verification helper:

```bash
uv run scripts/gpu_load_test.py --gb 8 --seconds 300
```

## Build Commands

Build the package with:

```bash
uv build
```

Other packaging-related checks:

```bash
uv run python -m sjdet --help
uv run sjdet --help
```

## Lint Commands

There is currently no configured linter in `pyproject.toml`.

- No `ruff` config was found.
- No `black` config was found.
- No `isort` config was found.
- No `mypy` or `pyright` config was found.

Because lint tooling is not configured, agents should:

- follow the existing code style manually,
- keep diffs small and consistent,
- avoid introducing formatting churn unrelated to the task.

## Test Commands

There is currently no dedicated `tests/` directory and no configured automated test runner.

What exists today:

- `uv run python scripts/mock_cli.py` for a mocked end-to-end sanity check.
- `uv run scripts/gpu_load_test.py ...` for manual GPU verification on a real SLURM job.

Recommended verification flow after code changes:

```bash
uv run python scripts/mock_cli.py
uv run sjdet --help
```

If you changed packaging or entry points, also run:

```bash
uv build
```

## Single-Test Command

There is no single-test command today because there is no automated test suite yet.

If pytest is added later, the preferred single-test forms should be:

```bash
uv run pytest tests/test_file.py
uv run pytest tests/test_file.py -k test_name
uv run pytest tests/test_file.py::test_exact_case
```

Until then, use the smallest relevant manual verification command and mention exactly what was or was not exercised.

## Code Style Guidelines

### Imports

- Use `from __future__ import annotations` at the top of Python modules, matching the existing codebase.
- Order imports as: standard library, third-party, local package imports.
- Keep import groups separated by a single blank line.
- Prefer explicit imports over wildcard imports.
- Import only what is used.

### Formatting

- Follow existing PEP 8-style formatting.
- Keep functions relatively small and purpose-specific.
- Prefer early returns to reduce nesting.
- Preserve concise, readable expressions; do not compress logic into unreadable one-liners.
- Keep line lengths reasonable even though no formatter is enforced.
- Use ASCII by default unless a file already relies on Unicode output semantics.

### Types

- Add type hints to public functions and structured helpers.
- Preserve existing explicit return annotations such as `-> None`, `-> str`, `-> Table`, and `-> Dict[...]`.
- Use dataclasses for structured row/state containers when that matches existing patterns.
- Prefer concrete types already used in the project (`List`, `Dict`, `Tuple`, `Optional`) unless you are making a broader typing cleanup.

### Naming

- Use `snake_case` for variables, functions, and module-level helpers.
- Use `ALL_CAPS` for module-level constants such as cache paths.
- Use descriptive parser/helper names like `parse_tres_value`, `metric_to_gb`, and `scontrol_node_gpu_info`.
- Keep CLI flag names descriptive and consistent with current conventions.

### Error Handling

- Be defensive around external system data and shell output.
- Treat missing, malformed, or `UNKNOWN` SLURM values as normal inputs.
- Return safe defaults when parsing fails, matching existing behavior.
- Avoid crashing on cache read/write failures; `cache.py` intentionally fails soft.
- Narrow exceptions when practical, but preserve user-facing resilience.
- Do not silently swallow errors in new code unless the surrounding module already uses a deliberate fail-soft strategy.

### Shell and SLURM Integration

- Centralize shell execution through `run()` in `src/sjdet/slurm.py` unless there is a strong reason not to.
- Quote shell arguments carefully; current code uses `shlex.quote` for SLURM command arguments.
- Keep scheduler calls batched when possible; avoiding scheduler spam is part of the tool's value.
- Preserve cache-aware behavior around `sstat` and node metadata.

### Display / UI Conventions

- Keep terminal rendering logic in `src/sjdet/display.py`.
- Use Rich primitives already present in the project (`Table`, `Text`, `Group`, `ProgressBar`, `box`).
- Keep display helpers pure where possible: pass data in, return renderables out.
- Preserve clear separation between data collection and presentation.
- If changing color semantics, ensure they remain consistent across CPU, memory, GPU, and trend indicators.

### CLI Conventions

- Keep argument parsing in `src/sjdet/cli.py`.
- Keep `main()` as the orchestration boundary for the CLI.
- Prefer additive flags over breaking changes to current CLI behavior.
- Maintain helpful defaults for cluster etiquette, especially polling interval and cache usage.

### Comments and Docstrings

- Keep comments sparse and useful.
- Add comments only when logic is non-obvious or domain-specific.
- Prefer short docstrings on parsing helpers and externally meaningful functions.
- Avoid restating what straightforward code already says.

## Architecture Expectations

- Do not mix SLURM parsing logic into `display.py`.
- Do not move rendering concerns into `slurm.py`.
- Keep cache-specific filesystem behavior inside `cache.py`.
- If adding new metrics, thread them through `LiveRow` cleanly rather than passing loose dicts around the UI layer.

## Agent Workflow Guidance

- Start by reading the smallest relevant file set.
- Prefer minimal, localized changes.
- Preserve existing behavior unless the task explicitly calls for a behavior change.
- When changing CLI output, verify with `uv run python scripts/mock_cli.py`.
- When changing entry points or packaging, verify with `uv build`.
- When a task touches real SLURM behavior, note whether validation used the mock script or an actual cluster.

## Things Not To Assume

- Do not assume a live SLURM environment is available locally.
- Do not assume GPU hardware is present.
- Do not assume there is an automated CI pipeline.
- Do not assume lint or test tooling exists unless it has been added to the repo.

## When Adding Tooling

If you introduce a formatter, linter, or test runner:

- add the config to `pyproject.toml` when appropriate,
- document the exact commands in this file,
- include a single-test command if tests are added,
- prefer `uv run ...` wrappers for consistency.

## Quick Command Reference

```bash
uv sync --dev
uv run sjdet
uv run python -m sjdet
uv run python scripts/mock_cli.py
uv run scripts/gpu_load_test.py --gb 8 --seconds 300
uv build
```
