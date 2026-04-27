package org.apache.spark.dataflint

import org.apache.spark.SPARK_VERSION
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
 * metric. Runs in preColumnarTransitions so it sees the fully-planned physical tree.
 *
 * Exchange nodes (ShuffleExchangeExec, BroadcastExchangeExec) are never wrapped.
 *
 * Version-specific class names (e.g. PythonMapInArrowExec, added in Spark 3.3) are matched by
 * simple class name string to avoid NoClassDefFoundError on Spark 3.0/3.1 at load time.
 *
 * The !isInstanceOf[TimedExec] guard on the child makes the rule idempotent — safe to re-run
 * under AQE prepareForExecution.
 */
case class DataFlintInstrumentationColumnarRule(session: SparkSession) extends ColumnarRule with Logging {

  // Eagerly compute the set of node simple-class-names to wrap, respecting per-type flags.
  // When the global flag is on everything is enabled; otherwise only nodes whose specific
  // flag is enabled are included.
  // TimedExec uses a transparent wrapper pattern (children = child.children) that is incompatible
  // with Spark 3.0/3.1's withNewChildren (which maps product elements via containsChild).
  // On 3.0/3.1, CollapseCodegenStages cannot update children through the wrapper, causing
  // ClassCastExceptions. SQL nodes participate in codegen pipelines and are affected;
  // Python exec nodes do not participate in codegen and are safe to instrument on all versions.
  private val isLegacySpark: Boolean = {
    val parts = SPARK_VERSION.split("\\.")
    parts.length >= 2 && parts(0) == "3" && (parts(1) == "0" || parts(1) == "1")
  }

  private val enabledNodeNames: Set[String] = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val sqlNodes = if (isLegacySpark) {
      logInfo("DataFlint: Spark 3.0/3.1 detected — skipping SQL node instrumentation (codegen incompatibility)")
      Set.empty[String]
    } else {
      Set(
        "FilterExec", "ProjectExec", "ExpandExec", "GenerateExec",
        "SortMergeJoinExec", "BroadcastHashJoinExec", "BroadcastNestedLoopJoinExec",
        "CartesianProductExec", "WindowGroupLimitExec", "SortAggregateExec", "SortExec", "HashAggregateExec",
        "DataWritingCommandExec",
        "FileSourceScanExec", "RowDataSourceScanExec", "BatchScanExec", "RDDScanExec",
      )
    }
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

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    if (enabledNodeNames.isEmpty) plan
    else plan.transformUp {
      case node if enabledNodeNames.contains(node.getClass.getSimpleName)
                && !node.isInstanceOf[TimedExec] =>
        logInfo(s"DataFlint: wrapping ${node.getClass.getSimpleName} with TimedExec")
        TimedExec(node)
    }
  }
}
