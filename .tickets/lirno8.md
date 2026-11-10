---
id: lirno8
status: open
type: task
effort: easy
created: 2026-11-10T23:07:37Z
---
# 4.4 Cross-minor guard test

Step 4.4 of design/RENOVATION_PLAN.md (Phase 4 – Spark 4.1 / 4.2 → M3 (DBR 18 LTS / 19, JDK 21)).

Each artifact refuses the other minor (4.1 vs 4.2). Verify: new test in `core`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.