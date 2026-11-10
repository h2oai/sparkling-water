---
id: figim5
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 4.2 M3a: scoring on JDK 21

Step 4.2 of design/RENOVATION_PLAN.md (Phase 4 – Spark 4.1 / 4.2 → M3 (DBR 18 LTS / 19, JDK 21)).

`scoring` + `py-scoring` need no H2O node. Verify: scoring tests on JDK 21; 0.2 runner with the 1.2 fixtures on DBR 18 LTS and 19. **Milestone M3a.**

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.