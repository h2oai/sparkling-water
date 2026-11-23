---
id: fibik8
status: open
type: bug
effort: normal
created: 2026-11-21T02:39:58Z
---
# 2.1b Remove JDK-internal URL handlers from RestCommunication

Step 2.1b of design/RENOVATION_PLAN.md (Phase 2). Found by 0.3a on DBR 16.4 LTS (JDK 17).

`core/.../backend/utils/RestCommunication.scala` `resolveUrl` creates `sun.net.www.protocol.http(s).Handler` directly, presumably to bypass URL stream handlers that platforms such as Databricks install. On JDK 17 `H2OContext.getOrCreate` therefore fails with `IllegalAccessError: ... cannot access class sun.net.www.protocol.http.Handler (in module java.base) because module java.base does not export sun.net.www.protocol.http`, unless the driver and executors run with `--add-exports=java.base/sun.net.www.protocol.http=ALL-UNNAMED` (and `.https`). Spark 4 requires JDK 17+, so every M1–M3 target is affected.

Fix: find why the explicit handler was added (git blame/history), then reach the same behaviour without JDK internals (for example `HttpURLConnection` via the JDK's default handler obtained through a public API, or an HTTP client) — or, if no public route exists, add the exports to the documented cluster configuration and to the `spark-submit` defaults. Test: a unit test that builds a URL through `resolveUrl` on JDK 17 without the flags; then 0.3a's training suite passes on DBR 16.4 with `JAVA_OPTIONS=` (empty).

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). If the change will exceed it, split this ticket into ordered child tickets that each fit the budget (`tk new "..." --under fibik8`) before continuing, then work them via `tk ready`. Done = the Verify condition holds; then tk note + tk close.