---
id: yanaj5
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 4.3 Full-tree build on 4.1 and 4.2; advertise

Step 4.3 of design/RENOVATION_PLAN.md (Phase 4 – Spark 4.1 / 4.2 → M3 (DBR 18 LTS / 19, JDK 21)).

`./gradlew build test -Pspark=4.1` / `-Pspark=4.2`; add both to `supportedSparkVersions`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.