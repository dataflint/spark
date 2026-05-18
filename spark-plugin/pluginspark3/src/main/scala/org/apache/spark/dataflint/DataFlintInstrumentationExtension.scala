package org.apache.spark.dataflint

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}

/**
 * A SparkSessionExtensions that injects DataFlint instrumentation into Spark's physical planning phase.
 */
class DataFlintInstrumentationExtension extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo("Registering DataFlint Instrumentation Extension")

    extensions.injectColumnar { session =>
      DataFlintInstrumentationColumnarRule(session)
    }
  }
}

/**
 * A ColumnarRule that wraps instrumented physical plan nodes with TimedExec to add a `duration`
 * metric.
 *
 * ## Why postColumnarTransitions, not pre
 *
 * Native accelerators (Gluten/Velox, Comet, RAPIDS, Photon) replace Spark operators with their
 * own variants via `injectColumnar`. TimedExec is transparent on Spark 3.2+
 * (`children = child.children`), which hides the wrapped operator from any other rule's plan
 * traversal — so wrapping `FilterExec` in `preColumnarTransitions` makes the accelerator's
 * transformation rule walk past it and the plan silently falls back to the Spark JVM path.
 *
 * `ApplyColumnarRulesAndInsertTransitions.apply` (Spark `Columnar.scala`) applies `pre` rules
 * in registration order then `post` rules in **reverse** registration order:
 *
 * {{{
 *   columnarRules.foreach(r => plan = r.preColumnarTransitions(plan))
 *   plan = insertTransitions(plan)
 *   columnarRules.reverse.foreach(r => plan = r.postColumnarTransitions(plan))
 * }}}
 *
 * Since the DataFlint plugin's `init()` runs first (it auto-registers this extension before
 * the accelerator's plugin appends its own), DataFlint sits at index 0 in `columnarRules` and
 * therefore runs **last** in post. By that point every accelerator has already substituted
 * its `*ExecTransformer` / `Comet*` / `Gpu*` / `Photon*` variants in `pre`, so the Spark
 * class names in `enabledNodeNames` only match operators no accelerator claimed (writes,
 * Python UDFs, fallback ops) — exactly what we want to instrument.
 *
 * ## Idempotence
 *
 * AQE re-runs `prepareForExecution` for each new query stage, so this rule may run more than
 * once over the same subtree. The `!isInstanceOf[TimedExec]` guard makes re-application a
 * no-op.
 *
 * ## Spark 3.0/3.1
 *
 * Version-specific class names (e.g. `PythonMapInArrowExec`, added in Spark 3.3) are matched
 * by simple class name string to avoid NoClassDefFoundError at load time.
 */
case class DataFlintInstrumentationColumnarRule(session: SparkSession) extends ColumnarRule with Logging {

  // Set of physical operator simple class names this rule will wrap with TimedExec.
  // Respects the per-feature flags; the global INSTRUMENT_SPARK_ENABLED implies all.
  private val enabledNodeNames: Set[String] = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val sqlNodes = Set(
      "FilterExec", "ProjectExec", "ExpandExec", "GenerateExec",
      "SortMergeJoinExec", "BroadcastHashJoinExec", "BroadcastNestedLoopJoinExec",
      "CartesianProductExec", "WindowGroupLimitExec", "SortAggregateExec", "SortExec", "HashAggregateExec",
      "DataWritingCommandExec",
      "FileSourceScanExec", "RowDataSourceScanExec", "BatchScanExec", "RDDScanExec",
    )
    val all = Set(
      "BatchEvalPythonExec",
      "ArrowEvalPythonExec",
      "MapInPandasExec",
      "PythonMapInArrowExec",       // Spark 3.3+ — safe via name string (no direct class ref)
      "FlatMapGroupsInPandasExec",
      "FlatMapCoGroupsInPandasExec",
      "WindowExec",
      "WindowInPandasExec"
    ) ++ sqlNodes
    if (globalEnabled) all
    else {
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, false))         sqlNodes                                          else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_BATCH_EVAL_PYTHON_ENABLED, false))     Set("BatchEvalPythonExec")                    else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_ARROW_EVAL_PYTHON_ENABLED, false))     Set("ArrowEvalPythonExec")                    else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_MAP_IN_PANDAS_ENABLED, false))         Set("MapInPandasExec")                        else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_MAP_IN_ARROW_ENABLED, false))          Set("PythonMapInArrowExec")                   else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_GROUPS_PANDAS_ENABLED, false)) Set("FlatMapGroupsInPandasExec")             else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_COGROUPS_PANDAS_ENABLED, false)) Set("FlatMapCoGroupsInPandasExec")         else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_WINDOW_ENABLED, false))                Set("WindowExec", "WindowInPandasExec")       else Set.empty[String])
    }
  }

  // Surface detected accelerators in logs so users know the coexistence path is engaged.
  if (enabledNodeNames.nonEmpty) {
    val active = Accelerator.active
    if (active.nonEmpty) {
      logInfo(
        s"DataFlint: native accelerator(s) detected — ${active.map(_.name).mkString(", ")}. " +
          "DataFlint runs in postColumnarTransitions and only wraps operators no accelerator transformed.")
    }
  }

  override def postColumnarTransitions: Rule[SparkPlan] = { plan =>
    if (enabledNodeNames.isEmpty) plan
    else plan.transformUp {
      case node if enabledNodeNames.contains(node.getClass.getSimpleName)
                && !node.isInstanceOf[TimedExec] =>
        logInfo(s"DataFlint: wrapping ${node.getClass.getSimpleName} with TimedExec")
        TimedExec(node)
    }
  }
}

