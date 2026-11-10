---
id: wefuv8
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 2.4 Spark 3.5 / Scala 2.13: scoring artifacts

Step 2.4 of design/RENOVATION_PLAN.md (Phase 2 – Toolchain and M1 (Scala 2.13 on Spark 3.5 → DBR 16.4 LTS)).

`assembly-scoring` shadow jar and `py-scoring` wheel with `_2.13` artifact names; `scoring-package` POM artifact id. Verify: `:sparkling-water-assembly-scoring:shadowJar :sparkling-water-py-scoring:distPython -Pspark=3.5 -PscalaVersion=2.13.<dbr>`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.