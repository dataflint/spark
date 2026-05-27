# Release Notes

### Version 1.0.0

The first **1.0** release — DataFlint is now stable on Databricks Runtime, has first-class **executor-level cluster metadata**, exposes its own version on `SparkConf`, and is guaranteed to never abort your Spark job during plugin init.

What changed:

🆕 **Executor metadata collection plugin (cloud VM + Kubernetes)**

A new `DataflintExecutorPlugin` runs on every executor at startup, collects machine metadata, and reports it back to the driver via Spark's plugin RPC (`PluginContext.send()` / `DriverPlugin.receive()`):

* **Cloud VM detection** reads `/sys/class/dmi/id/sys_vendor` locally to identify AWS / GCP / Azure, then issues a single provider-specific POSIX `sh` one-liner against the cloud metadata API to fetch `instanceType` and `lifecycleType` (spot vs on-demand). The DMI-detected provider is preserved even when the metadata endpoint is blocked, so consumers can distinguish "detected but blocked" from "not on cloud".
* **Kubernetes support** — when `KUBERNETES_SERVICE_HOST` is set, the metadata service is skipped and values are read from user-configured env vars (exposed from node labels via the downward API), so EKS / GKE / AKS clusters that block the IMDS still get the same data.
* **System basics** always collected via JVM APIs: `osName`, `osArch`, `jvmVersion`, `availableProcessors`, `executorJvmMaxHeapBytes`.
* **Opt-in** via `spark.dataflint.executor.metadata.enabled=true` (default `false`).
* **Event pipeline** follows the existing DataFlint pattern: `DataflintExecutorMetadataEvent` → `DataflintListener` → `DataflintStore` → export.

K8s pod spec example using the downward API:

```yaml
env:
  - name: NODE_INSTANCE_TYPE
    valueFrom:
      fieldRef:
        fieldPath: metadata.labels['node.kubernetes.io/instance-type']
  - name: NODE_LIFECYCLE
    valueFrom:
      fieldRef:
        fieldPath: metadata.labels['karpenter.sh/capacity-type']
```

Then set:

```
spark.dataflint.executor.metadata.k8s.instanceType.envVar=NODE_INSTANCE_TYPE
spark.dataflint.executor.metadata.k8s.lifecycleType.envVar=NODE_LIFECYCLE
```

🆕 **Plugin version exposed on `SparkConf`**

`DriverPlugin.init` now reads `Implementation-Version` from the plugin JAR's manifest and sets `spark.dataflint.version` on `SparkConf`. Any job can ask which DataFlint build it's running on:

```python
spark.conf.get("spark.dataflint.version")
# → "1.0.0"
```

🛠 **Fixed `StackOverflowError` in `TimedExec.executeCollect` on Databricks Runtime**

DBR's `SparkPlan.executeCollect` routes through `ResultCacheManager → Collector.collect(this)`, and `Collector` re-invokes `plan.executeCollect` via virtual dispatch. The base `TimedExec` override calling `super.executeCollect()` sent the call straight back into `Collector`, recursing on any `df.collect()` / small `df.count()` / broadcast prep until the stack blew up.

Fixed by removing the base-class override entirely. Per-partition timing is still applied: both vanilla Spark and Databricks reach our `doExecute` through their inherited `executeCollect → execute → doExecute` chain, which wraps the child RDD with the duration metric. The `DataWritingCommandExec` special-case (which needs its own `executeCollect` to wrap the eager write I/O with `RDDTimingWrapper`) lives in a new `TimedDataWritingCommandExec` subclass, dispatched by `TimedExec.apply`.

🛠 **`DriverPlugin.init` is now fully guarded**

`DriverPlugin.init` failures abort `SparkContext` start. The `init` body in both `pluginspark3` and `pluginspark4` is now wrapped in a top-level `try/catch` — a bug anywhere in DataFlint setup logs a warning and returns an empty `extraConf` instead of crashing the user's job.

🛠 **Hardening**

* Executor flag parse: `_.toBoolean` (throws on bad input) replaced with `java.lang.Boolean.parseBoolean`, so a malformed config value can no longer crash plugin init.
* Cloud-detect commands now run under `sh` instead of `bash` — the scripts are POSIX-only and minimal / distroless container images often ship without `bash`.
* `runBashCommand` now waits briefly after `destroyForcibly()` on timeout so the killed shell is reaped before we move on, avoiding zombie processes under some JVM/OS combos.
* `parseJson` filters `isNull` before `asText()`, so JSON nulls no longer become the literal string `"null"`.
* All log sites that included optional cloud fields now use `getOrElse("unknown")` instead of interpolating `Option`, so logs read `provider=aws` instead of `provider=Some(aws)`.
* `collectionError` falls back to the exception class name when `Throwable#getMessage` returns `null`, so the error string is always informative.
* Renamed `totalMemoryBytes` → `executorJvmMaxHeapBytes` to accurately reflect that it's `Runtime.maxMemory()` (JVM Xmx) and not container/system memory.
* Renamed config keys from `spark.dataflint.experimental.executor.metadata.*` → `spark.dataflint.executor.metadata.*` — the path is stable for 1.0.

**Configs added in 1.0.0**

| Property | Default | Description |
| --- | --- | --- |
| `spark.dataflint.executor.metadata.enabled` | `false` | Enable executor metadata collection |
| `spark.dataflint.executor.metadata.k8s.instanceType.envVar` | _(unset)_ | Env var holding the instance type when running in K8s |
| `spark.dataflint.executor.metadata.k8s.lifecycleType.envVar` | _(unset)_ | Env var holding the lifecycle (spot / on-demand) when running in K8s |
| `spark.dataflint.executor.metadata.k8s.cloudProvider.envVar` | _(unset)_ | Env var holding the cloud provider when running in K8s |
| `spark.dataflint.version` | _auto_ | Set by the plugin at init time from the JAR manifest's `Implementation-Version` (read-only) |

Compatible with Spark 3.0 → 4.x.

### Version 0.9.9

This release adds first-class support for native query accelerators - Apache Gluten/Velox, Apache DataFusion Comet, NVIDIA RAPIDS, and Databricks Photon - so you can now use DataFlint side-by-side with the accelerator of your\
choice and still see the full SQL plan, stage breakdown, and operator-level metrics.

What changed:

🆕 Native accelerator support in the DataFlint UI

The SQL plan view now recognizes accelerated operators and renders them as first-class nodes, with an accelerator badge so you can tell at a glance whether a node ran on Velox, Photon, RAPIDS, or DataFusion:

* Apache Gluten / Velox - WholeStageCodegenTransformer, VeloxResizeBatches, RowToVeloxColumnar, TakeOrderedAndProjectExecTransformer, and related boundary nodes are now classified, named, and assigned to stages correctly. Native\
  Velox timing metrics - aggregation / filter / sort / window time, peak memory, and spill - are surfaced on the plan nodes.
* Apache DataFusion Comet - CometExchange, CometColumnarExchange, and CometHashAggregate are now recognized in stage assignment, shuffle metrics, aggregate naming, and Comet's Keys: / Functions: plan-description format.
* NVIDIA RAPIDS - GpuColumnarExchange is recognized and Gpu\* blocking operators are now treated as stage boundaries.
* Databricks Photon - Photon\* columnar exchanges and blocking operators are now treated as stage boundaries.

Columnar exchanges (ColumnarExchange, CometExchange, GpuColumnarExchange) are now split into separate write/read visual nodes across stage boundaries, the same way Spark's native Exchange already was.

🛠 DataFlint instrumentation now coexists with native accelerators  \
───────────────────────────────────────────────Previously, enabling spark.dataflint.instrument.spark.enabled = true on a cluster that also had Gluten / Comet / RAPIDS / Photon on the classpath would silently disable native acceleration - DataFlint's TimedExec wrapper hid Spark operators from the accelerator's plan-substitution rules, and the accelerator quietly fell back to the JVM path.

In 0.9.9, DataFlint now:

* Detects each supported accelerator via a classpath probe at startup and logs which ones are active.
* Runs its instrumentation pass after every accelerator's pre-columnar phase (via postColumnarTransitions, which Spark applies in reverse registration order), so accelerator-substituted nodes like FilterExecTransformer and the Comet/Gpu/Photon equivalents are never wrapped.

The result: instrumentation and native acceleration coexist out of the box, with no extra configuration and no manual skip list. You can leave spark.dataflint.instrument.spark.enabled on.

Bug fixes:

* Broadcast duration was under-counted. SqlReducer.calcBroadcastExchangeDuration was only summing the "time to broadcast" component - the "time to build" and "collect time" terms were being dropped due to a statement-termination\
  bug. All three are now correctly summed into the reported broadcast exchange duration.
* Plan descriptions for nodes with spaces. SqlReducer.buildFallbackPlanDescriptions could not match nodes whose names contained spaces (Scan csv, WholeStageCodegenTransformer (1)), so their plan descriptions were silently dropped from the UI. Fixed.
* Fallback plan descriptions for native engines. Re-added parsing of the SQL-level planDescription so that nodes from native engines display their plan text even when DataFlint's custom endpoint returns empty.
* Stage propagation through Gluten boundary nodes - AQE codegen renumbering and Gluten's wrapped boundary operators no longer cause stages to be lost when walking the plan graph.

Examples and getting started:

* New docker/gluten example - Spark 3.5 + Apache Gluten 1.2 + Velox, including a runnable GlutenVeloxExample and a run-gluten-example.sh that builds the UI, the plugin, and the example jar and brings everything up via Docker\
  Compose.
* New docker/comet example - Spark 3.5.7 + Apache DataFusion Comet 0.4.0, mirroring the Gluten setup. The released Comet jar bundles both linux\_amd64 and linux\_aarch64 natives, so a single image works on Intel and Apple Silicon\
  (via Rosetta).

Both examples enable DataFlint instrumentation by default, so they're the fastest way to see the coexistence path in action.

Compatible with Spark 3.0 → 4.x.

### Version 0.9.8 <a href="#version-0.9.6" id="version-0.9.6"></a>

**Bug fixes:**\
&#x20; \#74 — Fix `NullPointerException` in `Block.code` when `from_json` (or any other\
&#x20;   `CodegenFallback` expression) is used under whole-stage codegen with DataFlint\
&#x20;   instrumentation enabled. `TimedWithCodegenExec` now reports `supportCodegen = false`\
&#x20;   for wrapped operators that contain a `CodegenFallback`, mirroring Spark's own\
&#x20;   `CollapseCodegenStages` check that the transparent wrapper had been hiding.\
&#x20; \* Compatible with Spark 3.0 → 4.x\
\
**Hardening:**\
&#x20; \* `TimedExec.postRddId` now overwrites the `rddId` metric instead of summing across\
&#x20;   re-executions of the same plan instance.\
&#x20; \* `TimedExec` and `TimedWithCodegenExec` no longer compare equal — fixes a corner\
&#x20;   case in plan canonicalization / AQE plan reuse.\
&#x20; \* `executeCollect` write-path is now bounds-safe; falls back to the standard path\
&#x20;   on unexpected plan shapes (vendor write commands, future Spark layouts).\
&#x20; \* `rddId` metric switched from a "size" type to plain sum (no longer rendered as\
&#x20;   bytes — `"12 B"` — in the SparkUI).

### Version 0.9.7 <a href="#version-0.9.6" id="version-0.9.6"></a>

This release adds first-class support for **Databricks Runtime 17.3 LTS and newer**, and fixes a metric formatting issue that could blank parts of the DataFlint SQL plan view on Databricks.

**What changed:**&#x20;

🆕 **New artifact: `dataflint-spark4-databricks_2.13`**

Databricks Runtime 17.3 is Spark 4–based but ships `javax.servlet` instead of the standard `jakarta.servlet`. The regular `dataflint-spark4_2.13` jar was crashing the cluster at startup with `NoClassDefFoundError: jakarta/servlet/Servlet`.

We now publish a separate jar built for Databricks runtimes:

```
io.dataflint:dataflint-spark4-databricks_2.13:0.9.7
```

It’s the same plugin, same `spark.plugins` class — only the jar coordinate changes.

🛠 **Duration metric now displays correctly on Databricks**

DataFlint’s `TimedExec` instrumentation wraps each operator with a "duration" timing metric. On Databricks runtimes, this was previously appearing as a bare number (e.g. `1058`) in the Spark UI rather than the expected `5s (1s, 2s, 3s)` formatting, which also crashed the DataFlint SQL plan view with `Unsupported time unit`. Fixed.

The DataFlint UI is also more defensive about unexpected metric formats — it now logs a warning and skips the value rather than blanking the page.

**Stock Spark 4.x (non-Databricks)**

Unchanged. `dataflint-spark4_2.13` continues to be the right artifact for stock Spark 4, EMR, and any other vanilla Spark 4 deployment.

### Version 0.9.6 <a href="#version-0.9.6" id="version-0.9.6"></a>

* Fixed a startup crash on Databricks Runtime `17.3 and up, using spark 4`\
  The **DataFlint** tab will not be registed

### Version 0.9.0 <a href="#version-0.9.5" id="version-0.9.5"></a>

#### UI and usability

* Added a **YouTube tutorial link** in the footer.
* Added a **new version notification**.
* DataFlint now fetches the latest version from Sonatype Central, It shows a chip when a newer version is available. Version checks use semver comparison. The check fails silently if Sonatype Central is unreachable.
* Fixed duration display for zero values, Nodes with `duration=0` now show `0 ms` instead of being hidden.

#### Instrumentation

* `DataFlintRDDUtils` now uses a custom RDD instead of `mapPartitions` for duration timing.
* The custom RDD captures `startTime` inside `compute()` It does this before `firstParent.iterator()` This now captures eager parent work correctly.\
  That includes operators like `SortExec` and `HashAggregateExec`\
  Previously, `mapPartitions` started timing too late That missed full partition sort work and hash map build time.
* `TimedExec.executeCollect` now records **per-partition write duration** It reconstructs `DataWritingCommandExec` with the data plan wrapped in `RDDTimingWrapper` On Spark `3.4+`, this happens inside `WriteFilesExec` On older Spark versions, it wraps the data plan directly The write command then consumes the timed RDD via `sparkContext.runJob` This captures both data production time and write I/O per partition Previously, write duration used driver-side wall-clock timing That was inconsistent with other per-partition metrics.
* `doProduce` now wraps child code in `try/finally` for blocking operators Duration metrics now flush even when operators exit early through `shouldStop()` or `return` This fixes `duration=0` in codegen paths for operators like `SortExec`
* `doProduce` now sanitizes `ctx.freshNamePrefix` This strips non-alphanumeric characters from generated code variable prefixes It fixes invalid Java identifiers for nodes with spaces in `nodeName` That includes names like `Scan ExistingRDD` and `Execute InsertIntoHadoopFsRelationCommand`.
* Added `RDDScanExec` to instrumented nodes `Scan ExistingRDD` nodes now get duration metrics on Spark `3` and Spark `4`.

### Version 0.8.9 <a href="#version-0.8.9" id="version-0.8.9"></a>

#### Instrumentation

* A single generic `TimedExec` wrapper now replaces 19 per-type `DataFlint*Exec` classes `TimedExec` adds a `duration` metric and an `rddId` metric.
* Existing Spark metrics stay intact on the wrapped node The wrapper exposes `child.children` as its own children The SQL graph stays as one node You do not get double nodes in Spark UI or DataFlint UI.
* `InMemoryTableScanExec` and all `Exchange` nodes are never wrapped.
* Version-specific nodes are matched by class name string This avoids `NoClassDefFoundError` on older Spark versions.
* Join codegen is cancelled where codegen instrumentation does not work.
* `DataWritingCommandExec` now gets duration support through `doPrepare` delegation.

#### Stage grouping and duration attribution

* Stage grouping is now topology-based from the SQL plan graph, `Exchange` boundaries define the stage graph, The result is deterministic across live runs and history server.
* Stage view now supports **Inclusive** and **Exclusive** duration modes, **Inclusive** shows native Spark metrics as-is **Exclusive** is the default **Exclusive** normalizes stage durations to `executorRunTime`.
* Attribution mode auto-enables when any instrumented node exists.
* Exchange read and write durations now come from shuffle metrics.
* Producer and consumer stages split those durations correctly.
* Metric reduction is now null-safe throughout the reducer.

### Version 0.8.8 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. Fix for instumenation on Spark 3.1.
2. Better extraction for Iceberg and bigQuery save operation.

### Version 0.8.7 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. New spark instrumentation - which adds duration metrics for more Python actions and window operation nodes ( spark versions 3.1+) enabled using : spark.dataflint.instrument.spark.window.enabled\
   spark.dataflint.instrument.spark.arrowEvalPython.enabled\
   spark.dataflint.instrument.spark.batchEvalPython.enabled\
   spark.dataflint.instrument.spark.flatMapGroupsInPandas.enabled spark.dataflint.instrument.spark.flatMapCoGroupsInPandas.enabled

### Version 0.8.3 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. New spark instrumentation - spark.dataflint.instrument.spark.mapInArrow.enabled and spark.dataflint.instrument.spark.mapInPandas.enabled, which adds duration metrics to all spark versions 3.3+ with these new operators. So you can know exactly how much a UDF took.
2. Add full table name to iceberg table reads

### Version 0.8.3 <a href="#version-0.3.0" id="version-0.3.0"></a>

Fix to stage identification algorithm\
Fix to stage read visuals to show hashed/ranges fields\
Visual improvements to stage sidebar

### Version 0.8.2 <a href="#version-0.3.0" id="version-0.3.0"></a>

* Improvement to sql plan layout with stage nodes
  * Button to switch to plan without stage nodes
* Improvements to stage identification

### Version 0.8.1 <a href="#version-0.3.0" id="version-0.3.0"></a>

* Small fix for fixing the support of spark version 3.3.X
* Small fix for shuffle write metrics real time update

### Version 0.8.0 <a href="#version-0.3.0" id="version-0.3.0"></a>

✨ New Features\
New Flow Graph UI with Stage Parent Nodes - Completely redesigned the SQL flow graph visualization with stage parent nodes for better understanding of query execution\
Task View in Stage Nodes - Added task progress indicators directly within stage nodes for real-time task monitoring\
"Rows Aggregated" Metric - Added a new metric to track aggregated row counts\
Exchange Node Separation - Shuffle read and write operations are now displayed separately when 2 stages exist, providing clearer visibility into data exchange operations

🎨 UI Improvements\
Enhanced visual consistency across UI components\
Replaced CheckIcon with CancelIcon for clearer error representation\
Updated FlowLegend to include task progress indicators\
Improved styling for node elements in SQL flow\
General SQL plan UI improvements\
⚡ Performance Improvements\
SQLNodeStageReducer Optimization - Implemented O(1) access with lookup maps for nodes and stages, significantly improving efficiency in SQL node stage calculations\
Smarter Update Cycles - Skip OnCycleEnd calculations when there are no changes in SQL and stages\
Stage Map for Alerts - Use stage map for faster alert processing

🐛 Bug Fixes\
Fixed wall clock duration calculation\
Fixed duration node hover display issue\
Fixed idle cores bug where idle cores showed 0% when all executors closed (Spark incorrectly detected local mode)

🔧 Other Changes\
Removed Scala distribution from Spark distribution package

### Version 0.7.0 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. Delta lake collector (experimental)
2. Stage identification improvements
3. UI Improvements

### Version 0.6.1 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. Add better delta lake support - delta write command, optimize command, optimize shuffle before write
2. Add support for SortAggregate nodes
3. Fix maven dependency issues with spark 3 POM fiels<br>

### Version 0.6.0 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. Spark 4 support
2. Better stage identification using metrics with statistics
3. Shrinking metrics text in case of high number of metrics

### Version 0.5.1 <a href="#version-0.3.0" id="version-0.3.0"></a>

Visual improvements for the new SQL plan UI

### Version 0.5.0 <a href="#version-0.3.0" id="version-0.3.0"></a>

New and updated design for the sql plan nodes

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FD62vQJ2DEPM4eQOsKrSo%2Fimage.png?alt=media&#x26;token=926a13d5-7160-4e26-8aef-1a8cb10e709d" alt=""><figcaption></figcaption></figure>

### Version 0.4.4 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. Improvement to query presentation
2. Support Extended node:

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2Fvt6s3ZkFkaeoI4Kr2YBA%2Fimage.png?alt=media&#x26;token=72e7d6ea-557b-4c33-a7de-ab6974af21b9" alt=""><figcaption></figcaption></figure>

### Version 0.4.3 <a href="#version-0.3.0" id="version-0.3.0"></a>

Support query params, like sql-id and node-ids in link

### Version 0.4.2 <a href="#version-0.3.0" id="version-0.3.0"></a>

Supports partition pruning

Enriching filter/select nodes with the UDF python function names:

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FvgZDaa1KJXDiMowzEJ3W%2Fimage.png?alt=media&#x26;token=12adc961-b007-436d-a614-2e8dcbc17b38" alt=""><figcaption></figcaption></figure>

Add support for Generate node - explode, inline and more!

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FLyLKV44RZd9jeTQqiiYJ%2Fimage.png?alt=media&#x26;token=8349652a-477e-4741-9ad0-c264ebc8a5dc" alt=""><figcaption></figcaption></figure>

### Version 0.4.1 <a href="#version-0.3.0" id="version-0.3.0"></a>

Support newer DBX versions - 14 and up.

### Version 0.4.0 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. Support map by pandas and arrow functions
2.  Added new flag to silence alert for a job -

    ```
    spark.dataflint.alert.disabled
    Which accepts a column seperated list of alerts such as:
    smallTasks,idleCoresTooHigh

    ```
3. Added short recommendation on top of alert
4. Updated DataFlint logo
5. Support better stage identifications for varios readers
6. Shows stage failures with an orange V on sql node and list of complete stage failures

### Version 0.3.2 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. New alerts - cross joins, change join to broadcast, large partition size
2. Better support for cross joins
3. Additional metrics for joins and shuffles

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FpO2asUeALtWyrGVLvsPo%2Fimage.png?alt=media&#x26;token=f8d08936-334e-4fa6-9232-fa01c9670007" alt=""><figcaption></figcaption></figure>

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FGcy2lxSuFZ5nFrstV7VH%2Fimage.png?alt=media&#x26;token=bc9ac1d3-fe9e-49df-9190-5b38d9a4a98d" alt=""><figcaption></figcaption></figure>

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FVt7UhJ2EilqKM2NeJnPh%2Fimage.png?alt=media&#x26;token=5cc2ba3b-3cc1-4bbf-979b-e3eb8a469632" alt=""><figcaption></figcaption></figure>

### Version 0.3.1 <a href="#version-0.3.0" id="version-0.3.0"></a>

1. Support Distinct, skewed join, coalesce nodes
2. Sorting by default by duration on history server mode
3. Filter sql's without any job/stage automatically, add a switch to show them.
4. Add rows filtered percentage for filter and distinct sql nodes

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FAsDTx9zxwS0jkCNvbi2J%2Fimage.png?alt=media&#x26;token=0c7ab3ec-ac47-42b0-bf96-ce8c506618f1" alt="" width="563"><figcaption></figcaption></figure>

Version 0.3.0

1. Support for window functions
2. Better support for databricks
3. Detecting resource configuration better in configuration tab
4. Bug and UI fixes

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FJz58gpY8rv2lBUTtKiyi%2Fimage.png?alt=media&#x26;token=12b792d2-5978-4012-a3c1-b7a6a6694950" alt="" width="292"><figcaption></figcaption></figure>

## Version 0.2.7

1. DataFusion Comet support
2. bug fixes

## Version 0.2.6

1. Nvidia RAPIDS for Spark support
2. bug fixes

## Version 0.2.5

1. Bug Fixes

## Version 0.2.4

1. New visibility & alerts - Driver's memory
2. Updated README
3. Bug Fixes

## Version 0.2.3

1. Driver memory monitoring & alert
2. Updated readme
3. Bug fixes

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2F9eN2Rb02x7MYKTaGBjsF%2Fimage.png?alt=media&#x26;token=20f22e66-a888-4e88-8e00-f8a70fb1baf9" alt="" width="325"><figcaption></figcaption></figure>

## Version 0.2.3

1. New alert - Large data Broadcast, for requesting to broadcast large data sets with the broadcast() function
2. New alert - Large filter conditions, for wiring long filter conditions instead of using join logic
3. UI Improvements

## Version 0.2.2

1. Support spark versions 2.4 logs in history server with version later than 3.2 Limited feature-set is available due to events having less data than spark 3.0 and up

## Version 0.2.1

1. Better Databricks stage to node support
2. Support spark.dataflint.runId in custom history server providers when appId is not the spark appId

## Version 0.2.0

1. Better support for Databricks Photon plans
2. Input nodes shows partitions filters and push down filters
3. Stage Breakdown - press the blue down arrow on sql node to see stage information
4. New alert - large number of small tasks (see [#large-number-of-small-tasks](../../advanced/alerts#large-number-of-small-tasks "mention"))

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FSh0P7NGaiAVUlnbKdmLc%2Fimage.png?alt=media&#x26;token=0aa1d85b-51bb-4517-ad12-9be7e672f682" alt=""><figcaption></figcaption></figure>

## Version 0.1.7

1. Apache Iceberg alerts improvements
2. Add avg file size in read/write
3. More information when hovering on stage

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FJ4WzeWUOaBLP3GZ9ZxCi%2Fimage.png?alt=media&#x26;token=a8e16eb4-d045-4ce9-be79-42a86f1e592d" alt=""><figcaption></figcaption></figure>

## Version 0.1.6

1. Apache Iceberg support
   1. Better node naming
   2. Read metrics and reading small files alerts
   3. Write metrics and overwriting most of table alerts
      1.  Require enabling iceberg metric reporter, can be done for you by enabling **spark.dataflint.iceberg.autoCatalogDiscovery** to true, or setting the iceberg metric reporter manually for each catalog, for example:

          ```
          spark.sql.catalog.[catalog name].metrics-reporter-impl org.apache.spark.dataflint.iceberg.DataflintIcebergMetricsReporter
          ```

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FNpzDDSPDoB3V6aUGNqow%2Fimage.png?alt=media&#x26;token=68b114f3-5367-45c8-932f-3c46b0863630" alt=""><figcaption><p>Replacing entire table only to change 1% of records (1 in a 100)</p></figcaption></figure>

## Version 0.1.5

1. Add support for history server with cluster-mode jobs (i.e. with attempt numbet)
2. Fix "wasted cores" calculation
3. Fix status tab SQL is flickering when there is SQL with sub queriers

## Version 0.1.4

Fix scala 2.13 support

## Version 0.1.3

1. DataFlint SaaS support
2. partition Skew Alert:

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FOk7qojKHCIJ34XISY9FU%2Fimage.png?alt=media&#x26;token=65a7125f-0c59-485e-acf7-7b2bfd438c86" alt=""><figcaption></figcaption></figure>

## Version 0.1.2

1. Scala 2.13 support
2. A spark flag to disable web app mixpanel telemetries - `spark.dataflint.telemetry.enabled`(true/false)
3. Renamed Core Activity Rate to Wasted Cores Ratio (which is 100 - Core Activity Rate), and added an alert for wasted cores too high

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FfRtqPlTpwStQaW7hlaEc%2Fimage.png?alt=media&#x26;token=34733ceb-d830-4569-842c-987b9ab734ff" alt=""><figcaption></figcaption></figure>

##

## Version 0.1.1

1. Resources tab - see a graph of your cluster executors count over time, use it to tune your resource allocation settings and save cost!
2. Minor visual fixes

DataFlint Resource Tab:

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2F7sHKQLyxH9JyBbGCP4Hy%2Fimage.png?alt=media&#x26;token=0e84649d-d0d0-41ba-b0ce-f59c11585526" alt=""><figcaption></figcaption></figure>

## Version 0.1.0

1. Small fix to platform identification

## Version 0.0.8

1. Databricks support
2. Visual improvements
3. public release

## Version 0.0.7

Heat map

<img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2F5IxRZvdq1wlp8pTYexzo%2Fimage.png?alt=media&#x26;token=f74c3ef6-023d-40a2-9d4d-3b604df8edf1" alt="" data-size="original">

## Version 0.0.6

Flint Assistant, require OpenAI Key

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2F1dR2Sx5jWRJxEU72Cq5i%2Fimage.png?alt=media&#x26;token=7b038381-bd83-4e87-9842-7b0e93420d06" alt=""><figcaption></figcaption></figure>

## Version 0.0.5

#### Syntax highlighting for SQL plan parts

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FM6v9udJyqi52zq60mV6q%2Fimage.png?alt=media&#x26;token=582c94b7-53ae-47ee-9143-b10e3f2fa4df" alt="" width="359"><figcaption><p>Show selected fields</p></figcaption></figure>

#### Calculating container memory usage and using it for GB memory/hour calculations

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2F0ote8UAW0sVfrmzCOVvn%2Fimage.png?alt=media&#x26;token=f21d4b57-6193-4051-a9d4-7d4c4c6e2ae8" alt=""><figcaption></figcaption></figure>

## Version 0.0.4

1. Minor fix relates to spark operator and nginx

## Version 0.0.3

#### SQL plan modes

IO only, shows only input, joins and output:

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FT6DJujjnn5gTvbI1ZxfD%2Fimage.png?alt=media&#x26;token=4c432350-610b-428a-be06-c86765af148a" alt=""><figcaption></figcaption></figure>

Basic mode (default), shows also transformations like filters, aggregations and selects:

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2F7YjGaJBHl53AkEWSq1xh%2Fimage.png?alt=media&#x26;token=c4c02c14-37c4-4d74-ab03-94fe5247756c" alt=""><figcaption></figcaption></figure>

#### Advances, shows repartitions, broadcasts and sorts

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FMxDjkSqfOmRyeFmv0yVv%2Fimage.png?alt=media&#x26;token=72c0aa0b-d86d-4a0f-a111-a5cd3bc58cd1" alt=""><figcaption></figcaption></figure>

Also there is plans informations for:

1. Joins
2. Sorts
3. Selects
4. Repartitions

## Version 0.0.2

#### DBU calculation instead of core/hour in summary bar

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2F9A7VxinZQ3SVukftNjX0%2Fimage.png?alt=media&#x26;token=c94f4752-1259-4a2e-990d-c74dee2501da" alt="" width="563"><figcaption></figcaption></figure>

#### Add memory config to configuration tab

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FYlr3hjYozlOdIgYw8iL5%2Fimage.png?alt=media&#x26;token=e74753ae-8415-4d69-a372-a0b0a0e25f8a" alt=""><figcaption></figcaption></figure>

#### Filter Nodes has condition:

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2FoHQP2EX26HlBz3L2PsHT%2Fimage.png?alt=media&#x26;token=1220ea44-17ce-4399-bb31-a35d8023a790" alt="" width="239"><figcaption></figcaption></figure>

#### Advanced mode for SQL plan, that also presents shuffle nodes

<figure><img src="https://2982210886-files.gitbook.io/~/files/v0/b/gitbook-x-prod.appspot.com/o/spaces%2Fcg8pTm3VgVaeMncRl8LP%2Fuploads%2Fw95ximBELAJC0GCnrLKK%2Fimage.png?alt=media&#x26;token=993ff051-ab90-4394-95c3-71770fb0bd35" alt=""><figcaption></figcaption></figure>

#### Additional changes

1. Support both http and https access with enabling mix-content only on https mode
2. Support for spark 3.5.X

## Version 0.0.1

Initial version, includes:

1. Status page
2. Summary page
3. Configuration Page
4. Alerts page
