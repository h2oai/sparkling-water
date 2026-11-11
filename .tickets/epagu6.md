---
id: epagu6
status: open
type: task
effort: easy
created: 2026-11-10T23:07:37Z
---
# 5.10 Release

Step 5.10 of design/RENOVATION_PLAN.md (Phase 5 – Breadth, CI, docs, release).

Version suffixes `-4.0/-4.1/-4.2/-4.3`, Maven `_2.13`, PyPI `h2o_pysparkling_4.x` and `h2o_pysparkling_scoring_4.x`, S3 layout.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under epagu6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.