---
id: vuzid9
status: closed
type: task
effort: easy
created: 2026-11-10T23:07:35Z
---
# 0.4 ADR-002 Support matrix

Step 0.4 of design/RENOVATION_PLAN.md (Phase 0 – Baseline, harness and decisions).

Keep only 3.5 on `master`; drop 2.3, 2.4, 3.0–3.4 (no Databricks runtime ships 3.4). A 4.x minor is added to `supportedSparkVersions` only when its build is green; default follows the Databricks LTS in use. Verify: `design/adr/ADR-002-*.md` merged and linked from this plan.

Done = ADR merged under design/adr and linked from the plan; then tk note + tk close.

## Note 2026-11-25T06:56:45Z

ADR-002 written: design/adr/ADR-002-support-matrix.md, linked from RENOVATION_PLAN 0.4 and the goal doc phase 0 list.