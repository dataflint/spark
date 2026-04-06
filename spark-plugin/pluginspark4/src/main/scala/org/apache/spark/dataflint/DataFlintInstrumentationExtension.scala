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
 * metric. Runs in preColumnarTransitions so it sees the fully-planned physical tree.
 *
 * Exchange nodes are never wrapped. Version-specific class names (ArrowWindowPythonExec, added
 * in Spark 4.1) are matched by simple class name string to avoid NoClassDefFoundError on Spark 4.0.
 */
case class DataFlintInstrumentationColumnarRule(session: SparkSession) extends ColumnarRule with Logging {

  private val enabledNodeNames: Set[String] = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val sqlNodes = Set(
      "FilterExec", "ProjectExec", "ExpandExec", "GenerateExec",
      "SortMergeJoinExec", "BroadcastHashJoinExec", "BroadcastNestedLoopJoinExec",
      "CartesianProductExec", "WindowGroupLimitExec", "SortAggregateExec",
      "DataWritingCommandExec"
    )
    val all = Set(
      "BatchEvalPythonExec",
      "ArrowEvalPythonExec",
      "MapInPandasExec",
      "MapInArrowExec",               // Spark 4.x name (was PythonMapInArrowExec in Spark 3.x)
      "FlatMapGroupsInPandasExec",
      "FlatMapCoGroupsInPandasExec",
      "WindowExec",
      "WindowInPandasExec",           // Spark 4.0
      "ArrowWindowPythonExec"         // Spark 4.1+ — safe via name string (no direct class ref)
    ) ++ sqlNodes
    if (globalEnabled) all
    else {
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, false))              sqlNodes                                                                    else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_BATCH_EVAL_PYTHON_ENABLED, false))      Set("BatchEvalPythonExec")                          else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_ARROW_EVAL_PYTHON_ENABLED, false))      Set("ArrowEvalPythonExec")                          else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_MAP_IN_PANDAS_ENABLED, false))          Set("MapInPandasExec")                              else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_MAP_IN_ARROW_ENABLED, false))           Set("MapInArrowExec")                               else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_GROUPS_PANDAS_ENABLED, false)) Set("FlatMapGroupsInPandasExec")                    else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_COGROUPS_PANDAS_ENABLED, false)) Set("FlatMapCoGroupsInPandasExec")               else Set.empty[String]) ++
      (if (conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_WINDOW_ENABLED, false))                 Set("WindowExec", "WindowInPandasExec", "ArrowWindowPythonExec") else Set.empty[String])
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