---
id: emuwo6
status: open
type: task
effort: normal
children:
  - hurel4
  - betoj9
  - lufan7
created: 2026-11-21T02:21:10Z
---
# 0.3a Training baseline on a fixed-size cluster with phase timings

Child of 0.3. Run the existing `ci/databricksTests/test.scala` / `test.py` (H2OContext internal backend, H2OXGBoost on prostate) with release `3.46.0.6-1-3.5` (Maven `ai.h2o:sparkling-water-package_2.12:3.46.0.6-1-3.5`, PyPI `h2o_pysparkling_3.5==3.46.0.6.post1`) on DBR 16.4 LTS, a fixed cluster of 3 workers (the scripts expect 3 H2O nodes), dedicated access.

- `run_databricks.sh`: select the test suite (scoring | training) with its scripts, libraries and worker count; keep the scoring default unchanged.
- Phase timings, machine-readable: each script times its phases with one shared helper and returns them as JSON via the notebook exit value. Training: Spark session, `H2OContext.getOrCreate` (internal backend start), data import, `asSparkFrame`, train, predict. Scoring scripts: MOJO load, score, pipeline save and reload. The runner adds the platform phases from the Jobs API (cluster setup incl. library install, execution, cleanup) and writes one JSON per run (suite, release, DBR, node type, workers, run id, phase -> seconds).
- Repeat runs: `RUNS=<n>` (default 1); record at least 3 runs for the baseline and store median and min/max.
- Record: the timing baseline as `design/baseline-timings/databricks-16.4-3.46.0.6.json` (no workspace, user or run URLs), the summary and library install path in `design/databricks-baseline-2026-11.md`.

Verify: both training tasks SUCCESS on 3 runs; baseline JSON committed and reproducible with one documented command; `shellcheck` clean.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under emuwo6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.