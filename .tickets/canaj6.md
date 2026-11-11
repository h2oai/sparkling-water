---
id: canaj6
status: open
type: task
effort: normal
created: 2026-11-10T23:07:36Z
---
# 3.10 `core` – Kubernetes external backend on fabric8 7.x

Step 3.10 of design/RENOVATION_PLAN.md (Phase 3 – Spark 4.0 → M2 (DBR 17.3 LTS, JDK 17)).

`K8sExternalBackendClient`, `K8sHeadlessService`, `K8sH2OStatefulSet`. Verify: core compiles on 3.5 and 4.0; K8s unit tests.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under canaj6`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.