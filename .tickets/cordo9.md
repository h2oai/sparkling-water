---
id: cordo9
status: open
type: task
effort: hard
created: 2026-11-10T23:07:36Z
---
# 3.9 `core` – internal backend on 4.0

Step 3.9 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

RPC/scheduler private APIs (`RpcEnv`, `ThreadSafeRpcEndpoint`, `CoarseGrainedSchedulerBackend`). Verify: `-Pspark=4.0 :sparkling-water-core:test` incl. `H2OContextTestSuite`; no `javax.servlet` import outside `extensions`.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.