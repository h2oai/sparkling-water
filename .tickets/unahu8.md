---
id: unahu8
status: open
type: task
effort: normal
created: 2026-11-10T23:07:37Z
---
# 5.9 Docs

Step 5.9 of design/RENOVATION_PLAN.md (Phase 5 – Breadth, CI, docs, release).

`migration_guide.rst` 3.46→4.x incl. Flow/REPL removal and `flowURL` behaviour; Azure Databricks pages updated with dedicated-mode, MOJO deployment and autoscaling notes; README badges; `REPO_INDEX.md` refresh.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under unahu8`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.