---
id: ihofa2
status: open
type: task
effort: normal
created: 2026-11-10T23:07:35Z
---
# 1.9 Collections compat: `utils`, `extensions`, `scoring` main sources

Step 1.9 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Add `scala-collection-compat`; `JavaConverters` → `scala.jdk.CollectionConverters`; procedure syntax, `WrappedArray`/`Traversable`/`Stream`. Verify: `compileScala` of the three modules with `-Xfatal-warnings` (pre-existing unrelated warnings are fixed or excluded in the same PR), then their tests.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.