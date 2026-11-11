---
id: rimwu9
status: open
type: task
effort: easy
created: 2026-11-10T23:07:35Z
---
# 0.7 ADR-001 H2O-3 line and JDK 21

Step 0.7 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

`∥` Options: 3.46.0.6 OSS + `sys.ai.h2o.debug.allowJavaVersions=21` set by Sparkling Water in the executor before `H2OStarter.start`; H2O-3 Secure with Java 21; external backend only on DBR 18/19. Needed before M3, not before M1/M2 (both are JDK 17). Ticket 4.5 is rewritten from the chosen option when this closes. Verify: `design/adr/ADR-001-*.md` merged and linked from this plan.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under rimwu9`) before continuing, then work them via `tk ready`. Done = ADR merged under design/adr and linked from the plan; then tk note + tk close.