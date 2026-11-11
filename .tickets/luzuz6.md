---
id: luzuz6
status: open
type: task
effort: easy
created: 2026-11-10T23:07:36Z
---
# 3.5 `py-scoring` on 4.0

Step 3.5 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

(Python 3.12). Verify: `:sparkling-water-py-scoring:testPython -Pspark=4.0`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under luzuz6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.