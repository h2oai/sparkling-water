---
id: maluz9
status: open
type: chore
effort: easy
created: 2026-11-10T23:07:36Z
---
# 1.16 log4j2 test config

Step 1.16 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Replace `core/src/test/resources/log4j.properties` and the other modules' test logging config with log4j2. Verify: `:sparkling-water-core:test :sparkling-water-ml:test` (no "log4j:WARN" output).

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.