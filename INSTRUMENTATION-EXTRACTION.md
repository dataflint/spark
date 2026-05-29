# Instrumentation extraction

The SQL-plan **instrumentation** feature (per-operator `TimedExec` timing) has
been extracted out of this repo into the standalone
[`dataflint/spark-instrumentation`](https://github.com/dataflint/spark-instrumentation)
project, which depends on the published `io.dataflint:dataflint-common`
artifact and publishes its own artifacts to GitHub Packages.

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

Also removed (this repo no longer knows anything about the SQL-plan
instrumentation extension):
* `DataflintSparkUICommonLoader`: the `INSTRUMENT_*` config constants, the
  `DATAFLINT_EXTENSION_CLASS` name, and `registerInstrumentationExtension`.
* The `registerInstrumentationExtension(sc)` call in `SparkDataflintPlugin`
  (pluginspark3 + pluginspark4) and its now-unused import.

Build change:
* Removed the `pluginspark4` `Test / unmanagedSources` block that pulled in the
  now-moved `DataFlintCodegenFallbackSpec`.

## Registration is now the new repo's responsibility

Because `dataflint-common` no longer adds the extension to `spark.sql.extensions`,
the instrumentation jar must register itself (e.g. via its own `SparkPlugin` or
a `spark.sql.extensions` entry the user sets). The `org.apache.spark.dataflint`
package + `api.ResolvedStageGroup` remain here as ordinary classes — they are no
longer an instrumentation "contract", just shared Spark-side types.

## Kept here (unrelated to the SQL-plan extension)

* The Delta Lake instrumentation listener (`DeltaLakeInstrumentationListener` &
  friends), driven by the separate `spark.dataflint.instrument.deltalake.*`
  config keys read directly in `install()` — a distinct feature, left in place.
