---
id: sojit9
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 2.7 Spark 3.5 / Scala 2.13: `ml`, `examples`, `py`/`r`, `package`, `assembly` (`_2.13` artifact names)

Step 2.7 of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)).

Verify: `./gradlew test -Pspark=3.5 -PscalaVersion=2.13.<dbr>`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.