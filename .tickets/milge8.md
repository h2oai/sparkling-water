---
id: milge8
status: open
type: task
effort: hard
created: 2026-11-10T23:07:37Z
---
# 4.5 JDK 21 for training per ADR-001

Step 4.5 of design/RENOVATION_PLAN.md (Phase 4 – Spark 4.1 / 4.2 → M3 (DBR 18 LTS / 19, JDK 21)).

Rewritten from the 0.7 decision. If OSS H2O-3: set `sys.ai.h2o.debug.allowJavaVersions` programmatically in `InternalH2OBackend.startH2OWorker` (and driver client start) gated by an `H2OConf` flag; H2O-dependent suites on JDK 21. Verify: `./gradlew test -Pspark=4.2` on JDK 21.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.

## Note 2026-11-10T23:07:48Z

Rewrite this ticket from the ADR-001 decision (ticket 0.7) before starting; the OSS allowJavaVersions path is only one of three options.