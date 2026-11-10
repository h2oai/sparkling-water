---
id: cetov5
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 5.2 Spark 4.3 upstream build

Step 5.2 of design/RENOVATION_PLAN.md (Phase 5 – Breadth, CI, docs, release).

(M4). Property file + `-Pspark=4.3` build and tests on standalone; 1.2 fixtures load in the 4.3 scoring suite; guard test extended so the 4.3 artifact refuses 4.1/4.2; CI entry.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.