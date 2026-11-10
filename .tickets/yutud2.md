---
id: yutud2
status: open
type: task
effort: hard
created: 2026-11-10T23:07:36Z
---
# 1.14 Remove the driver Flow proxy and servlets

Step 1.14 of design/RENOVATION_PLAN.md (Phase 1 – Shrink the surface (Spark 3.5 / Scala 2.12; `./gradlew build` green after every step)).

Delete `ProxyStarter`, `water/webserver/jetty9/*`, the rest of `backend/api/*`, proxy-only tests (`LdapProxyOnlyModeAuthTestSuite`, `LdapAuthTestSuite`, `LdapAesEncryptedBindPasswordLoginModuleSuite`, SSL-proxy suites), driver Jetty 9 dependencies in `core/build.gradle` (H2O's own `h2o-jetty-9` transitive stays). Node-forwarded `flow.*` options stay. Verify: `:sparkling-water-core:test`; `H2OContextTestSuite` asserts no driver Jetty server is started.

PR budget: < 300 changed lines (whole-directory deletions and regenerated code excluded). Done = the Verify condition holds; then tk note + tk close.