---
id: walep8
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 1.11 Fix unsafe params

Step 1.11 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Store "needs JSON" params from the 1.3 audit as JSON/arrays via the Spark ML paramMap; keep reading the old form. Verify: both `CrossVersion*` suites + scoring + ml tests.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under walep8`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.