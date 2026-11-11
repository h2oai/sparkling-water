---
id: vatig8
status: open
type: chore
effort: normal
created: 2026-11-10T23:07:36Z
---
# 2.2 Separate JDK toolchain from bytecode target

Step 2.2 of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)).

New properties `javaToolchainVersion=17` (global) and `bytecodeTargetVersion` (8 in `gradle-spark3.5.properties`, 17 for 4.x files) replacing the dual use of `minSupportedJavaVersion` in `gradle/scala.gradle`; scalac `-release`, javac `--release`; CI images `eclipse-temurin:17`. Verify: `./gradlew clean build -x check -Pspark=3.5` on JDK 17, class files report major version 52.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under vatig8`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.