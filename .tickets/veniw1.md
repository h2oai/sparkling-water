---
id: veniw1
status: closed
type: chore
effort: normal
created: 2026-11-10T23:07:35Z
---
# 0.1 Baseline build on current tree

Step 0.1 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

`./gradlew build -x check` then `./gradlew :sparkling-water-core:test :sparkling-water-utils:test :sparkling-water-ml:test` on JDK 11 (reference: upstream CI target) and JDK 17 (renovation target; the test JVMs get no Spark `--add-opens` flags, so failures there are expected and recorded). Record pass/fail per module in `design/baseline-2026-11.md`. No code changes.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under veniw1`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.

## Note 2026-11-11T03:18:49Z

JDK 11: build OK except R tasks; core 206/210, utils 12/12, ml 297/322. All 28 failures environment (XGBoost arm64 x7, DAI license x17, SPARK_HOME x2) or REPL code removed in 1.13 (x2). JDK 17: Spark suites abort without --add-opens -> new step 2.1a. Details: design/baseline-2026-11.md.

## Note 2026-11-11T03:37:05Z

Rerun 2026-11-11 on JDK 11 with SPARK_HOME=Spark 3.5.9 and DRIVERLESS_AI_LICENSE_FILE: config suites 2/2 pass, H2OMOJOPipelineModelTestSuite 19/19 pass, ScalaInterpreterServletTestSuite still 2 failures. JDK 11 now core 208/210, ml 314/322 (7 XGBoost failures, arm64 only).