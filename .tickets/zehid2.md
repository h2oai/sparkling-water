---
id: zehid2
status: open
type: task
effort: easy
created: 2026-11-10T23:07:35Z
---
# 0.6 ADR-003 Flow UI + ADR-004 REPL removal contract

Step 0.6 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

Remove `ProxyStarter`, `water/webserver/jetty9/*`, `backend/api/*`, the `repl` module and `core`'s driver-side Jetty 9 dependencies. API contract: `H2OContext.flowURL` and `openFlow` (Scala, Python, R) throw `UnsupportedOperationException` with a pointer to the migration guide; `H2OClusterInfo` loses `flowURL`; proxy-only options (`spark.ext.h2o.flow.ip/port`, proxy login, `repl.*`) are removed; options forwarded to H2O nodes (`flow.dir`, extra HTTP headers) stay. Replacement out of scope. Verify: `design/adr/ADR-003-*.md` and `ADR-004-*.md` merged and linked from this plan.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = ADR merged under design/adr and linked from the plan; then tk note + tk close.