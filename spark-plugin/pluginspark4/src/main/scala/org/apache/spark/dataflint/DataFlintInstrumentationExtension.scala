package org.apache.spark.dataflint

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.expressions.{NamedExpression, WindowFunctionType}
import org.apache.spark.sql.catalyst.planning.PhysicalWindow
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window => LogicalWindow}
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, DataFlintArrowEvalPythonExec, DataFlintArrowWindowPythonExec_4_1, DataFlintFlatMapCoGroupsInPandasExec, DataFlintFlatMapGroupsInPandasExec, DataFlintMapInPandasExec_4_0, DataFlintMapInPandasExec_4_1, DataFlintPythonMapInArrowExec_4_0, DataFlintPythonMapInArrowExec_4_1, DataFlintWindowInPandasExec_4_0, FlatMapCoGroupsInPandasExec, FlatMapGroupsInPandasExec, MapInArrowExec, MapInPandasExec}
import org.apache.spark.sql.execution.window.DataFlintWindowExec

/**
 * A SparkSessionExtension that injects DataFlint instrumentation into Spark's physical planning phase.
 * This extension replaces:
 *   - MapInPandasExec with the version-appropriate DataFlintMapInPandasExec (adds duration metric)
 *   - MapInArrowExec with the version-appropriate DataFlintPythonMapInArrowExec (adds duration metric)
 *
 * Supports Spark 4.0.x and 4.1.x with version-specific implementations that match
 * each version's MapInBatchExec API.
 *
 * The extension is automatically registered by SparkDataflintPlugin when any instrumentation flag is enabled:
 *   - spark.dataflint.instrument.spark.enabled (global)
 *   - spark.dataflint.instrument.spark.mapInPandas.enabled
 *   - spark.dataflint.instrument.spark.mapInArrow.enabled
 *
 * Can also be manually registered via:
 *   .config("spark.sql.extensions", "org.apache.spark.dataflint.DataFlintInstrumentationExtension")
 */
class DataFlintInstrumentationExtension extends (SparkSessionExtensions => Unit) with Logging {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    logInfo("Registering DataFlint Instrumentation Extension")

    extensions.injectColumnar { session =>
      DataFlintInstrumentationColumnarRule(session)
    }

    extensions.injectPlannerStrategy { session =>
      DataFlintWindowPlannerStrategy(session)
    }
  }
}

/**
 * A ColumnarRule that replaces physical plan nodes with DataFlint instrumented versions.
 * The replacement happens in preColumnarTransitions to ensure it runs before any columnar optimizations.
 *
 * Uses runtime Spark version detection to select the correct implementation:
 *   - 4.0.x: DataFlintMapInPandasExec_4_0 / DataFlintPythonMapInArrowExec_4_0
 *   - 4.1.x: DataFlintMapInPandasExec_4_1 / DataFlintPythonMapInArrowExec_4_1
 */
case class DataFlintInstrumentationColumnarRule(session: SparkSession) extends ColumnarRule with Logging {

  private val sparkMinorVersion: String = {
    val parts = SPARK_VERSION.split("\\.")
    if (parts.length >= 2) s"${parts(0)}.${parts(1)}" else SPARK_VERSION
  }

  private val mapInPandasEnabled: Boolean = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val specificEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_MAP_IN_PANDAS_ENABLED, defaultValue = false)
    globalEnabled || specificEnabled
  }

  private val mapInArrowEnabled: Boolean = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val specificEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_MAP_IN_ARROW_ENABLED, defaultValue = false)
    globalEnabled || specificEnabled
  }

  private val arrowEvalPythonEnabled: Boolean = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val specificEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_ARROW_EVAL_PYTHON_ENABLED, defaultValue = false)
    globalEnabled || specificEnabled
  }

  private val flatMapGroupsEnabled: Boolean = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val specificEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_GROUPS_PANDAS_ENABLED, defaultValue = false)
    globalEnabled || specificEnabled
  }

  private val flatMapCoGroupsEnabled: Boolean = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val specificEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_COGROUPS_PANDAS_ENABLED, defaultValue = false)
    globalEnabled || specificEnabled
  }

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    if (!mapInPandasEnabled && !mapInArrowEnabled && !arrowEvalPythonEnabled && !flatMapGroupsEnabled && !flatMapCoGroupsEnabled) plan
    else {
      var result = plan

      if (mapInPandasEnabled || mapInArrowEnabled) {
        result = result.transformUp {
          case mapInPandas: MapInPandasExec if mapInPandasEnabled =>
            logInfo(s"Replacing MapInPandasExec with DataFlint version for Spark $sparkMinorVersion")
            sparkMinorVersion match {
              case "4.0" =>
                DataFlintMapInPandasExec_4_0(
                  func = mapInPandas.func,
                  output = mapInPandas.output,
                  child = mapInPandas.child,
                  isBarrier = mapInPandas.isBarrier,
                  profile = mapInPandas.profile
                )
              case _ =>
                DataFlintMapInPandasExec_4_1(
                  func = mapInPandas.func,
                  output = mapInPandas.output,
                  child = mapInPandas.child,
                  isBarrier = mapInPandas.isBarrier,
                  profile = mapInPandas.profile
                )
            }
          case mapInArrow: MapInArrowExec if mapInArrowEnabled =>
            logInfo(s"Replacing MapInArrowExec with DataFlint version for Spark $sparkMinorVersion")
            sparkMinorVersion match {
              case "4.0" =>
                DataFlintPythonMapInArrowExec_4_0(
                  func = mapInArrow.func,
                  output = mapInArrow.output,
                  child = mapInArrow.child,
                  isBarrier = mapInArrow.isBarrier,
                  profile = mapInArrow.profile
                )
              case _ =>
                DataFlintPythonMapInArrowExec_4_1(
                  func = mapInArrow.func,
                  output = mapInArrow.output,
                  child = mapInArrow.child,
                  isBarrier = mapInArrow.isBarrier,
                  profile = mapInArrow.profile
                )
            }
        }
      }

      if (arrowEvalPythonEnabled) {
        result = result.transformUp {
          case arrowEval: ArrowEvalPythonExec =>
            logInfo(s"Replacing ArrowEvalPythonExec with DataFlint version for Spark $sparkMinorVersion")
            DataFlintArrowEvalPythonExec(
              udfs = arrowEval.udfs,
              resultAttrs = arrowEval.resultAttrs,
              child = arrowEval.child,
              evalType = arrowEval.evalType
            )
        }
      }

      if (flatMapGroupsEnabled) {
        result = result.transformUp {
          case exec: FlatMapGroupsInPandasExec =>
            logInfo(s"Replacing FlatMapGroupsInPandasExec with DataFlint version for Spark $sparkMinorVersion")
            DataFlintFlatMapGroupsInPandasExec(
              groupingAttributes = exec.groupingAttributes,
              func = exec.func,
              output = exec.output,
              child = exec.child
            )
        }
      }

      if (flatMapCoGroupsEnabled) {
        result = result.transformUp {
          case exec: FlatMapCoGroupsInPandasExec =>
            logInfo(s"Replacing FlatMapCoGroupsInPandasExec with DataFlint version for Spark $sparkMinorVersion")
            DataFlintFlatMapCoGroupsInPandasExec(
              leftGroup = exec.leftGroup,
              rightGroup = exec.rightGroup,
              func = exec.func,
              output = exec.output,
              left = exec.left,
              right = exec.right
            )
        }
      }

      result
    }
  }
}

/**
 * A planner Strategy that converts the logical Window plan node directly into
 * DataFlintWindowExec, bypassing WindowExec entirely.
 *
 * Using a Strategy (rather than a ColumnarRule) is correct for row-based operators
 * like WindowExec that do not participate in columnar execution.
 */
case class DataFlintWindowPlannerStrategy(session: SparkSession) extends SparkStrategy with Logging {

  private val sparkMinorVersion: String = {
    val parts = SPARK_VERSION.split("\\.")
    if (parts.length >= 2) s"${parts(0)}.${parts(1)}" else SPARK_VERSION
  }

  private val windowEnabled: Boolean = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val specificEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_WINDOW_ENABLED, defaultValue = false)
    globalEnabled || specificEnabled
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    if (!windowEnabled) return Nil
    plan match {
      case PhysicalWindow(
        WindowFunctionType.SQL, windowExprs, partitionSpec, orderSpec, child) =>
        logInfo("Replacing logical Window with DataFlintWindowExec")
        DataFlintWindowExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case PhysicalWindow(
        WindowFunctionType.Python, windowExprs, partitionSpec, orderSpec, child) =>
        logInfo(s"Replacing logical Window (Python UDF) with DataFlint version for Spark $sparkMinorVersion")
        sparkMinorVersion match {
          case "4.0" =>
            DataFlintWindowInPandasExec_4_0(windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil
          case _ => DataFlintArrowWindowPythonExec_4_1(windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil
        }

      case _ => Nil
    }
  }
}
