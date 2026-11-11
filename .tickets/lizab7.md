---
id: lizab7
status: open
type: task
effort: easy
created: 2026-11-11T03:18:49Z
---
# 2.1a Spark Java 17 module flags for test JVMs

Step 2.1a of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1). Found by baseline 0.1.

Add Spark's `--add-opens` options (the list in Spark's `JavaModuleOptions`) as `jvmArgs` of the `test`, `integTest` and `bench` tasks in `gradle/sparkTest.gradle` and `gradle/scala.gradle`, only when the test JVM is 17 or newer (baseline: every Spark suite aborts on JDK 17 with IllegalAccessError on sun.nio.ch.DirectBuffer). Verify: `:sparkling-water-utils:test :sparkling-water-core:test` on JDK 17 match the JDK 11 baseline in design/baseline-2026-11.md.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under lizab7`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.