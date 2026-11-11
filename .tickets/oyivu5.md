---
id: oyivu5
status: open
type: task
effort: easy
created: 2026-11-10T23:07:35Z
---
# 0.5 ADR-005 Supported Databricks mode

Step 0.5 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

Azure Databricks dedicated classic compute only; standard/serverless and Spark Connect out of scope; AWS/GCP best-effort. Verify: `design/adr/ADR-005-*.md` merged and linked from this plan.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under oyivu5`) before continuing, then work them via `tk ready`. Done = ADR merged under design/adr and linked from the plan; then tk note + tk close.