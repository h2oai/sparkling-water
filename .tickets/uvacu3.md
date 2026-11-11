---
id: uvacu3
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 3.14 Full `assembly` + `assembly-extensions` on 4.0; advertise 4.0

Step 3.14 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

Exclusions, checker, `supportedSparkVersions=3.5 4.0`. Verify: `:sparkling-water-assembly:shadowJar -Pspark=4.0` + checker; `./gradlew build -Pspark=3.5` still green.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under uvacu3`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.