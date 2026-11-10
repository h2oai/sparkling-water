---
id: pelaw5
status: open
type: chore
effort: normal
created: 2026-11-10T23:07:36Z
---
# 2.1 Gradle 8.x

Step 2.1 of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)).

Wrapper 8.10+, shadow 8.x, `gradle-use-python` 4.x, spotless 6.x, docker plugin 9.x, release 3.x. Verify: `./gradlew build -x check`, `distPython`, `spotlessCheck`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.