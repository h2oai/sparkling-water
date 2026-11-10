---
id: veniw1
status: open
type: chore
effort: normal
created: 2026-11-10T23:07:35Z
---
# 0.1 Baseline build on current tree

Step 0.1 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

`./gradlew build -x check` then `./gradlew :sparkling-water-core:test :sparkling-water-utils:test :sparkling-water-ml:test` on the installed JDK 17. Record pass/fail per module in `design/baseline-2026-11.md`. No code changes.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.