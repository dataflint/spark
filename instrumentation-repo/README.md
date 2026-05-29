# dataflint-instrumentation

SQL-plan instrumentation for [DataFlint](https://github.com/dataflint/spark),
extracted into its own repository so it can ship and version independently of
the core DataFlint plugin.

This module injects a `SparkSessionExtensions` / `ColumnarRule` that wraps
selected physical operators with `TimedExec`, adding a per-operator `duration`
metric to Spark's SQL UI (and the DataFlint UI). It coexists with native
accelerators (Gluten/Velox, Comet, RAPIDS, Photon) by running in
`postColumnarTransitions` and only wrapping operators no accelerator claimed.

## How it relates to dataflint/spark

```
┌──────────────────────────────┐          ┌────────────────────────────────────┐
│ dataflint/spark               │          │ dataflint/spark-instrumentation     │
│  io.dataflint:dataflint-common│──Maven──▶│  depends on dataflint-common         │
│   • INSTRUMENT_* config keys  │ Central  │  (provided)                          │
│   • registerInstrumentation() │          │  • DataFlintInstrumentationExtension │
│     loads the extension       │◀─runtime─│    (spark3 + spark4)                 │
│     BY CLASS NAME             │ classpath│  • TimedExec / MetricsUtils          │
│   • api.ResolvedStageGroup    │   only   │  • Accelerator                       │
└──────────────────────────────┘          │  • GraphDurationAttribution          │
                                           └────────────────────────────────────┘
```

* **Build-time:** this repo depends on the published
  `io.dataflint:dataflint-common` artifact for the `INSTRUMENT_*` config
  constants and `api.ResolvedStageGroup`.
* **Runtime:** there is **no** code dependency from `dataflint-common` back to
  this repo. `dataflint-common`'s `registerInstrumentationExtension` adds
  `org.apache.spark.dataflint.DataFlintInstrumentationExtension` to
  `spark.sql.extensions` **by class name**. As long as this jar is on the
  classpath and an `spark.dataflint.instrument.*` flag is enabled, the
  extension is picked up automatically.

## Modules

| Module                              | Scala       | Spark | Artifact                                  |
|-------------------------------------|-------------|-------|-------------------------------------------|
| `core`                              | 2.12 / 2.13 | 3.5   | `dataflint-instrumentation-core`          |
| `spark3`                            | 2.12 / 2.13 | 3.x   | `dataflint-instrumentation-spark3` (fat)  |
| `spark4`                            | 2.13        | 4.x   | `dataflint-instrumentation-spark4` (fat)  |

The `spark3` / `spark4` artifacts are fat JARs that bundle `core` (Scala and
the provided deps are excluded).

## Usage

Add the DataFlint plugin (which provides `dataflint-common`) **and** the
matching instrumentation jar to your Spark job, then enable instrumentation:

```
spark.plugins                                   io.dataflint.spark.SparkDataflintPlugin
spark.dataflint.instrument.spark.enabled        true
```

Per-feature flags (any one enables the extension): see `INSTRUMENT_*` keys in
`DataflintSparkUICommonLoader`, e.g.
`spark.dataflint.instrument.spark.sqlNodes.enabled`,
`spark.dataflint.instrument.spark.window.enabled`,
`spark.dataflint.instrument.spark.mapInPandas.enabled`, …

## Consuming the artifacts (GitHub Packages)

```scala
resolvers += "dataflint-instrumentation" at
  "https://maven.pkg.github.com/dataflint/spark-instrumentation"

credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env("GITHUB_ACTOR"),
  sys.env("GITHUB_TOKEN") // a PAT with read:packages
)

libraryDependencies += "io.dataflint" %% "dataflint-instrumentation-spark3" % "0.1.0"
```

> GitHub Packages requires authentication even for public packages — consumers
> need a token with `read:packages`.

## Build & test

```bash
sbt core/compile spark3/compile spark4/compile
sbt spark3/test          # full regression suite, runs against Spark 3.5 (Java 17)
sbt +publish             # cross-publish to GitHub Packages (CI does this)
```

## Releasing

Publishing happens via `.github/workflows/publish.yml` on a `v*` tag or manual
dispatch, using the built-in `GITHUB_TOKEN`. Bump `versionNum` in `build.sbt`
and tag `vX.Y.Z`.
