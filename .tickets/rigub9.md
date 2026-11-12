---
id: rigub9
status: open
type: bug
effort: easy
created: 2026-11-11T03:56:37Z
---
# 1.12a Drop pkg_resources from the PySparkling initializer

Step 1.12a of design/RENOVATION_PLAN.md. Found by 0.2a.

`ai/h2o/sparkling/Initializer.py` (`isRunningViaDBCConnect`, used by `getVersion`) imports `pkg_resources`, which recent setuptools releases removed. Released 3.46.0.6 PySparkling and PySparkling scoring fail at import time in any environment with current setuptools (`ModuleNotFoundError: No module named 'pkg_resources'`); `ci/databricksTests/scoring/run_local.sh` pins `setuptools<80` as a workaround. Replace with `importlib.metadata` (Python 3.8+) in `py-scoring` (the only copy; `py` packages it too), add a unit test that imports the initializer without setuptools installed, then remove the pin from `run_local.sh`. Verify: `:sparkling-water-py-scoring:testPython`; `run_local.sh` green with the locally built scoring wheel and no setuptools pin.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under rigub9`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.