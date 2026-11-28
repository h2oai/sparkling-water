# ADR-002: Spark support matrix for `master`

- Status: accepted
- Date: 2026-11-24 (dropping 3.0–3.4 decided 2026-11-10)
- Context documents: `design/RENOVATION_GOAL_SPARK_4.x.md` (section 1, Databricks Runtime matrix),
  `design/RENOVATION_PLAN.md` (step 0.4)

## Context

`gradle.properties` lists `supportedSparkVersions=2.3 2.4 3.0 3.1 3.2 3.3 3.4 3.5`. Each minor has its own
`gradle-spark<X.Y>.properties`, its own published artifacts (version suffix `-<X.Y>`) and, for several minors, its own
`scala_spark_<X.Y>` source directories. Every minor multiplies the build, the CI matrix and the shims.

The renovation targets Azure Databricks first. The Databricks runtimes that matter ship Spark 3.5 (DBR 16.4 LTS), 4.0
(DBR 17.3 LTS), 4.1 (DBR 18 LTS) and 4.2 (DBR 19); upstream 4.3 comes later. No supported Databricks runtime ships
Spark 2.x or 3.0–3.4. Users of those minors keep the released 3.46.0.x artifacts, which stay published.

Spark 4.x is Scala 2.13 only and changes private APIs between minors, so the per-minor artifact and the runtime guard
in `H2OContext` (`runningOnCorrectSpark`) stay.

## Decision

1. **`master` supports Spark 3.5 only, until the first 4.x minor is green.** Spark 2.3, 2.4 and 3.0–3.4 are dropped:
   their property files, `scala_spark_<X.Y>` directories, CI entries and docs go (steps 1.4 and 1.5).
2. **Spark 4.x minors are added one at a time: 4.0, then 4.1 and 4.2, then 4.3.** A minor joins
   `supportedSparkVersions` only when its full build and tests are green. Until then it can be built explicitly
   (`-Pspark=<X.Y>`) but is not advertised, released or in the default CI matrix.
3. **One source tree for all minors.** Differences between 4.x minors live in `gradle-spark<X.Y>.properties`; where a
   private Spark signature differs, a shim in `utils` handles it, not a per-minor source directory.
4. **One artifact per minor**, with the existing `-<X.Y>` version suffix.
5. **The default `spark=` follows the Databricks LTS that is the primary target:** 3.5 today, 4.0 at M2, 4.1 once
   DBR 18 LTS is the primary target. 4.3 is an upstream CI entry and never the default while no Databricks runtime
   ships it.
6. **A minor is dropped from `supportedSparkVersions` only by a new ADR.** This includes 3.5 once DBR 16.4 LTS leaves
   support.

Out of scope: the Scala binary version (Phase 2 ports the build to Scala 2.13; Spark 4.x has no 2.12 build) and the
Python range of PySparkling. Both follow the plan and get their own ADR if they need a decision.

## Consequences

- The build, CI matrix and shims shrink to one Spark line before the Spark 4 port starts; steps 1.4 and 1.5 do the
  deletions with `./gradlew build` green after each.
- New releases from `master` no longer cover Spark 2.x or 3.0–3.4. Fixes for those lines need a maintenance branch
  from a release tag; none is planned.
- At any time, `supportedSparkVersions` lists only minors with a green build, so a release never advertises a minor
  that does not work.
- Supporting several 4.x minors from one tree relies on shims; a private API change too large for a shim needs a new
  ADR (for example a separate source set) rather than a quiet per-minor directory.
