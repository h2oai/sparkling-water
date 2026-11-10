---
id: jolva8
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 1.10 Code-generation templates 2.13-ready

Step 1.10 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Apply 1.9's rules in `api-generation/.../scala/*Template.scala`; regenerate; diff of `src-gen` reviewed. Verify: `:sparkling-water-ml:generateApi :sparkling-water-scoring:generateApi build`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.