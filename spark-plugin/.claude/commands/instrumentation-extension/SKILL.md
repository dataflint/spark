# Instrumentation Extension Expert

You are an expert on the `DataFlintInstrumentationExtension` — the Spark SQL extension that injects duration metrics into MapInPandas and MapInArrow physical plan nodes across Spark 3.0–4.1.

## How to use this skill

Respond to any question or task about the instrumentation extension. Common tasks:
- Add a new plan node type (new `case` branch in the ColumnarRule)
- Debug why a metric isn't appearing
- Add support for a new Spark version
- Understand how a specific version implementation works

---

## Architecture Overview

### Entry Points
- **Spark 3.x**: `pluginspark3/src/main/scala/org/apache/spark/dataflint/DataFlintInstrumentationExtension.scala`
- **Spark 4.x**: `pluginspark4/src/main/scala/org/apache/spark/dataflint/DataFlintInstrumentationExtension.scala`
- **Config/registration**: `plugin/src/main/scala/org/apache/spark/dataflint/listener/DataflintSparkUICommonLoader.scala`

### Registration Flow
1. `DataflintSparkUICommonLoader.registerInstrumentationExtension()` appends the extension class to `spark.sql.extensions` if any instrumentation config is enabled.
2. The extension registers a `DataFlintInstrumentationColumnarRule` via `injectColumnar`.
3. The rule's `preColumnarTransitions` hook runs `transformUp` on the physical plan, replacing target nodes.

### Config Flags
```
spark.dataflint.instrument.spark.enabled
spark.dataflint.instrument.spark.mapInPandas.enabled
spark.dataflint.instrument.spark.mapInArrow.enabled
```

---

## ColumnarRule Pattern

### Spark 3.x (two separate methods to avoid ClassNotFoundError on old versions)
```scala
def replaceMapInPandas(plan: SparkPlan): SparkPlan = plan.transformUp {
  case mapInPandas: MapInPandasExec if mapInPandasEnabled =>
    sparkMinorVersion match {
      case "3.0" => DataFlintMapInPandasExec_3_0(...)
      case "3.1" | "3.2" => DataFlintMapInPandasExec_3_1(...)
      case "3.3" => DataFlintMapInPandasExec_3_3(...)
      case "3.4" => DataFlintMapInPandasExec_3_4(...)
      case _     => DataFlintMapInPandasExec_3_5(...)  // 3.5+
    }
}

def replaceMapInArrow(plan: SparkPlan): SparkPlan = plan.transformUp {
  case mapInArrow: PythonMapInArrowExec if mapInArrowEnabled =>
    sparkMinorVersion match {
      case "3.3" => DataFlintPythonMapInArrowExec_3_3(...)
      case "3.4" => DataFlintPythonMapInArrowExec_3_4(...)
      case _     => DataFlintPythonMapInArrowExec_3_5(...)  // 3.5+
    }
}

override def preColumnarTransitions: SparkPlan => SparkPlan = plan => {
  var result = replaceMapInPandas(plan)
  try { result = replaceMapInArrow(result) } catch { case _: ClassNotFoundException => }
  result
}
```

### Spark 4.x (single combined transformUp)
```scala
override def preColumnarTransitions: SparkPlan => SparkPlan = plan =>
  plan.transformUp {
    case mapInPandas: MapInPandasExec if mapInPandasEnabled =>
      sparkMinorVersion match {
        case "4.0" => DataFlintMapInPandasExec_4_0(...)
        case _     => DataFlintMapInPandasExec_4_1(...)
      }
    case mapInArrow: MapInArrowExec if mapInArrowEnabled =>
      sparkMinorVersion match {
        case "4.0" => DataFlintPythonMapInArrowExec_4_0(...)
        case _     => DataFlintPythonMapInArrowExec_4_1(...)
      }
  }
```

---

## Version-Specific Implementation Files

All live under:
- `pluginspark3/src/main/scala/org/apache/spark/sql/execution/python/`
- `pluginspark4/src/main/scala/org/apache/spark/sql/execution/python/`

### Key differences by version

| Version | File suffix | Notable changes |
|---------|-------------|-----------------|
| 3.0 | `_3_0` | No MapInBatchExec trait, no PythonSQLMetrics, 6-arg ArrowPythonRunner via reflection |
| 3.1/3.2 | `_3_1` | Same as 3.0, 6-arg runner |
| 3.3 | `_3_3` | MapInBatchExec trait added; MapInArrow support introduced; still 6-arg runner |
| 3.4 | `_3_4` | PythonSQLMetrics mixin; 7-arg runner (adds pythonMetrics param) |
| 3.5 | `_3_5` | Major refactor: MapInBatchEvaluatorFactory; barrier mode; JobArtifactSet (Spark Connect) |
| 4.0 | `_4_0` | ResourceProfile field; chainedFunc includes resultId (`Seq[(ChainedPythonFunctions, Long)]`) |
| 4.1 | `_4_1` | sessionUUID for worker logging; pythonUDFProfiler; 13-arg EvaluatorFactory via reflection |

### Duration metric pattern (all versions)
```scala
val durationMetric = SQLMetrics.createTimingMetric(sparkContext, "duration")
// ... measured in nanoseconds, reported in milliseconds
val startTime = System.nanoTime()
// ... execute the original logic ...
durationMetric += (System.nanoTime() - startTime) / 1000000
```

### Why reflection?
Constructor signatures change between Spark minor versions. Rather than maintaining separate binary artifacts per patch version, reflection allows a single compiled class to work across minor versions within a series.

---

## How to Add a New Plan Node Type

### Step 1: Identify the target node
Find the physical plan node class (e.g., `FlatMapGroupsInPandasExec`) and its constructor parameters across all relevant Spark versions.

### Step 2: Add config flag (optional)
In `DataflintSparkUICommonLoader.scala`, add a new constant and check it in `registerInstrumentationExtension()`.

### Step 3: Create instrumented wrapper classes
For each Spark version that supports the node, create a new file:
```
DataFlint<NodeName>Exec_<major>_<minor>.scala
```

The wrapper must:
1. Extend the original node class (copy all constructor params)
2. Override `metrics` to add the `"duration"` timing metric
3. Wrap the execution logic to measure and record duration

### Step 4: Add case branches
In both `pluginspark3` and `pluginspark4` extension files, add a new `case` branch:
```scala
case target: TargetExec if targetEnabled =>
  sparkMinorVersion match {
    case "3.3" => DataFlintTargetExec_3_3(...)
    // ...
  }
```

For Spark 3.x: if the node doesn't exist in 3.0/3.1, wrap the replacement in a separate method with try-catch for `ClassNotFoundException`.

### Step 5: Wire the guard flag
Add the enabled flag lookup:
```scala
val targetEnabled = conf.getBoolean("spark.dataflint.instrument.spark.<nodeName>.enabled", defaultValue = true)
```

---

## Debugging Tips

- **Metric not showing**: Check if the config flag is enabled and the node class name matches exactly (including package).
- **ClassNotFoundException on old Spark**: The replacement node class likely references a class that doesn't exist in that version. Move the `case` branch into a separate method and wrap with `try { } catch { case _: ClassNotFoundException => }`.
- **Wrong constructor args via reflection**: Print the available constructors with `clazz.getConstructors.foreach(println)` to find the right signature.
- **Version detection**: Version is extracted as `SPARK_VERSION.split("\\.").take(2).mkString(".")` giving `"3.5"`, `"4.1"`, etc.