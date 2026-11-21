---
id: lufan7
status: open
type: task
effort: normal
created: 2026-11-21T03:12:24Z
---
# 0.3a-3 Training suite on Databricks and the recorded timing baseline

Child of 0.3a. Training scripts move to ci/databricksTests/training/ with phase timings, summary and marker; JDK 17 --add-exports default for training clusters; 3 runs per suite recorded in design/baseline-timings/databricks-16.4-3.46.0.6.json and design/databricks-baseline-2026-11.md; bug 2.1b filed. Verify: 3 of 3 runs green for both suites.