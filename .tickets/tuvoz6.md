---
id: tuvoz6
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 3.4 `assembly-scoring` on 4.0

Step 3.4 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

Exclusions re-derived from the 4.0 dependency tree; `bin/check-duplicate-classes.sh` committed. Verify: `:sparkling-water-assembly-scoring:shadowJar -Pspark=4.0` + the checker; 3.5 still green.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under tuvoz6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.