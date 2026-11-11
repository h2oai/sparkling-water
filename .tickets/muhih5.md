---
id: muhih5
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 1.12 Rewire `py-scoring` to the scoring assembly

Step 1.12 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

`distPython` depends on `:sparkling-water-assembly-scoring:shadowJar` and the `apiGeneration` configuration uses `assembly-scoring` (shadow) + `api-generation` (shadow) + spark-repl, with no reference to `:sparkling-water-assembly`, so the scoring wheel never compiles `core`/`ml`. Verify: generated `src-gen` byte-identical on 3.5; `:sparkling-water-py-scoring:testPython`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under muhih5`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.

## Note 2026-11-10T23:07:48Z

Open question from review: does MOJOModelAPIRunner (api-generation) load any class from core/ml at generation time? Run generateMOJOApi with only assembly-scoring + api-generation on the classpath first; if it does, split api-generation before rewiring.