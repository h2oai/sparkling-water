---
id: melfa7
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 4.1 Property files 4.1 and 4.2 + scoring lane build

Step 4.1 of design/RENOVATION_PLAN.md (Phase 4 – Spark 4.1 / 4.2 → M3 (DBR 18 LTS / 19, JDK 21)).

`macros`, `utils`, `api-generation`, `scoring`, `assembly-scoring`, `py-scoring` on `-Pspark=4.1` and `-Pspark=4.2`; cross-minor shims only. Verify: `:sparkling-water-scoring:test :sparkling-water-py-scoring:testPython` per minor.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under melfa7`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.