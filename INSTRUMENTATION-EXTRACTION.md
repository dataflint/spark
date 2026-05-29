# Instrumentation extraction

The SQL-plan **instrumentation** feature (per-operator `TimedExec` timing) has
been extracted out of this repo into a standalone project that depends on the
published `io.dataflint:dataflint-common` artifact and publishes its own
artifacts to GitHub Packages.

The new project's seed lives at [`instrumentation-repo/`](instrumentation-repo/)
on this branch — see [`instrumentation-repo/NEW-REPO-SETUP.md`](instrumentation-repo/NEW-REPO-SETUP.md)
for how to lift it into `dataflint/spark-instrumentation`. Once that repo is
live, delete `instrumentation-repo/` from this branch.

## Why it splits cleanly

The wiring was already loosely coupled:

* `DataflintSparkUICommonLoader.registerInstrumentationExtension` adds
  `org.apache.spark.dataflint.DataFlintInstrumentationExtension` to
  `spark.sql.extensions` **by class name** — no compile dependency on it.
* The extension's only inbound needs from common are the `INSTRUMENT_*` config
  constants and `api.ResolvedStageGroup` — both **kept here**.
* `TimedExec`/`MetricsUtils`/`Accelerator`/`GraphDurationAttribution` had no
  inbound references from the UI / listener / saas code.

So at runtime users add the new instrumentation jar to the classpath; common
auto-registers it by name when an `spark.dataflint.instrument.*` flag is set.

## Removed from this repo

Moved to the new repo (`core` module):
* `plugin/src/main/scala/org/apache/spark/dataflint/TimedExec.scala`
* `plugin/src/main/scala/org/apache/spark/dataflint/MetricsUtils.scala`
* `plugin/src/main/scala/org/apache/spark/dataflint/Accelerator.scala`
* `plugin/src/main/scala/org/apache/spark/dataflint/GraphDurationAttribution.scala`

Moved (version-specific extensions):
* `pluginspark3/.../DataFlintInstrumentationExtension.scala` → new `spark3` module
* `pluginspark4/.../DataFlintInstrumentationExtension.scala` → new `spark4` module

Moved (entire `pluginspark3/src/test` — all of it was instrumentation tests):
* `TimedExecMetricsSpec`, `DataFlintCodegenExecSpec`, `DataFlintCodegenFallbackSpec`,
  `DataFlintPythonExecSpec`, `DataFlintSqlNodesSpec`, `DataFlintWindowExecSpec`,
  `DataFlintWriteMetricsSpec`, `GraphDurationAttributionSpec`, `GraphStageGroupSpec`,
  `SqlMetricTestHelper`, and the `org/apache/spark/sql/execution/ExplicitRepartition.scala`
  test helper.

Build change:
* Removed the `pluginspark4` `Test / unmanagedSources` block that pulled in the
  now-moved `DataFlintCodegenFallbackSpec`.

## Deliberately kept here (the contract)

* `DataflintSparkUICommonLoader`: the `INSTRUMENT_*` constants and
  `registerInstrumentationExtension` (loads the extension by name).
* `api/DurationTypes.scala` (`ResolvedStageGroup`).
* The Delta Lake instrumentation listener (`DeltaLakeInstrumentationListener` &
  friends) — out of scope for this split; it is compile-coupled to the loader.
