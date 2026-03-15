package org.apache.spark.dataflint

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.expressions.WindowFunctionType
import org.apache.spark.sql.catalyst.planning.PhysicalWindow
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Window => LogicalWindow}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec, DataFlintArrowEvalPythonExec, DataFlintBatchEvalPythonExec, DataFlintFlatMapCoGroupsInPandasExec, DataFlintFlatMapGroupsInPandasExec, DataFlintMapInPandasExec_3_0, DataFlintMapInPandasExec_3_5, DataFlintPythonMapInArrowExec_3_3, DataFlintPythonMapInArrowExec_3_5, DataFlintWindowInPandasExec, FlatMapCoGroupsInPandasExec, FlatMapGroupsInPandasExec, MapInPandasExec, WindowInPandasExec}
import org.apache.spark.sql.execution.window.DataFlintWindowExec

/**
 * A SparkSessionExtensions that injects DataFlint instrumentation into Spark's physical planning phase.
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
 *   - 3.0.x: DataFlintMapInPandasExec_3_0 (mapInPandas only)
 *   - 3.1.x: DataFlintMapInPandasExec_3_1 (mapInPandas only)
 *   - 3.2.x: DataFlintMapInPandasExec_3_1 (mapInPandas only, same API as 3.1)
 *   - 3.3.x: DataFlintMapInPandasExec_3_3 / DataFlintPythonMapInArrowExec_3_3
 *   - 3.4.x: DataFlintMapInPandasExec_3_4 / DataFlintPythonMapInArrowExec_3_4
 *   - 3.5.x: DataFlintMapInPandasExec_3_5 / DataFlintPythonMapInArrowExec_3_5
 *
 * The mapInPandas and mapInArrow replacements are performed in separate transforms so that
 * PythonMapInArrowExec (which doesn't exist before Spark 3.3) doesn't cause NoClassDefFoundError
 * on older Spark versions.
 *
 * Each replace method guards against double-wrapping with isInstanceOf checks so that
 * re-runs (e.g. under AQE prepareForExecution) are idempotent.
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

  private val batchEvalPythonEnabled: Boolean = {
    val conf = session.sparkContext.conf
    val globalEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, defaultValue = false)
    val specificEnabled = conf.getBoolean(DataflintSparkUICommonLoader.INSTRUMENT_BATCH_EVAL_PYTHON_ENABLED, defaultValue = false)
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
    if (!mapInPandasEnabled && !mapInArrowEnabled && !arrowEvalPythonEnabled && !batchEvalPythonEnabled && !flatMapGroupsEnabled && !flatMapCoGroupsEnabled) plan
    else {
      val inputNodes = plan.collect { case n => n.getClass.getSimpleName }.mkString(", ")
      logWarning(s"DataFlint preColumnarTransitions ENTER — plan nodes: [$inputNodes]")

      var result = plan

      if (mapInPandasEnabled) {
        result = replaceMapInPandas(result)
      }

      if (mapInArrowEnabled) {
        result = replaceMapInArrow(result)
      }

      if (arrowEvalPythonEnabled) {
        result = replaceArrowEvalPython(result)
      }

      if (batchEvalPythonEnabled) {
        result = replaceBatchEvalPython(result)
      }

      if (flatMapGroupsEnabled) {
        result = replaceFlatMapGroupsInPandas(result)
      }

      if (flatMapCoGroupsEnabled) {
        result = replaceFlatMapCoGroupsInPandas(result)
      }

      val outputNodes = result.collect { case n => n.getClass.getSimpleName }.mkString(", ")
      logWarning(s"DataFlint preColumnarTransitions EXIT  — plan nodes: [$outputNodes]")
      result
    }
  }

  private def replaceMapInPandas(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case mapInPandas: MapInPandasExec =>
        logWarning(s"Replacing MapInPandasExec with DataFlint version for Spark $sparkMinorVersion")
        sparkMinorVersion match {
          case "3.5" =>
            DataFlintMapInPandasExec_3_5(
              func = mapInPandas.func,
              output = mapInPandas.output,
              child = mapInPandas.child,
              isBarrier = mapInPandas.isBarrier
            )
          case _ =>
            // 3.0–3.4: all share the same 3-arg constructor, handled via reflection
            DataFlintMapInPandasExec_3_0(
              func = mapInPandas.func,
              output = mapInPandas.output,
              child = mapInPandas.child
            )
        }
    }
  }

  /**
   * Replaces ArrowEvalPythonExec nodes (pandas_udf SCALAR) with DataFlint instrumented versions.
   *
   * Only supported on Spark 3.2+ which uses the 4-param constructor (udfs, resultAttrs, child,
   * evalType). Spark 3.0–3.1 used a 3-param constructor incompatible with our wrapper.
   *
   * The isInstanceOf guard makes the rule idempotent — safe to re-run under AQE.
   */
  private def replaceArrowEvalPython(plan: SparkPlan): SparkPlan = {
    if (sparkMinorVersion == "3.0" || sparkMinorVersion == "3.1") {
      logWarning("ArrowEvalPython instrumentation requires Spark 3.2+ — skipping on Spark " + sparkMinorVersion)
      return plan
    }
    try {
      plan.transformUp {
        case arrowEval: ArrowEvalPythonExec if !arrowEval.isInstanceOf[DataFlintArrowEvalPythonExec] =>
          logWarning(s"Replacing ArrowEvalPythonExec with DataFlint version for Spark $sparkMinorVersion")
          DataFlintArrowEvalPythonExec(
            udfs = arrowEval.udfs,
            resultAttrs = arrowEval.resultAttrs,
            child = arrowEval.child,
            evalType = arrowEval.evalType
          )
      }
    } catch {
      case _: NoClassDefFoundError =>
        logWarning("ArrowEvalPythonExec not available in this Spark version — arrowEvalPython instrumentation disabled")
        plan
    }
  }

  private def replaceBatchEvalPython(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case exec: BatchEvalPythonExec if !exec.isInstanceOf[DataFlintBatchEvalPythonExec] =>
        logWarning(s"Replacing BatchEvalPythonExec with DataFlint version for Spark $sparkMinorVersion")
        DataFlintBatchEvalPythonExec(
          udfs = exec.udfs,
          resultAttrs = exec.resultAttrs,
          child = exec.child
        )
    }
  }

  private def replaceFlatMapGroupsInPandas(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case exec: FlatMapGroupsInPandasExec if !exec.isInstanceOf[DataFlintFlatMapGroupsInPandasExec] =>
        logWarning(s"Replacing FlatMapGroupsInPandasExec with DataFlint version for Spark $sparkMinorVersion")
        DataFlintFlatMapGroupsInPandasExec(
          groupingAttributes = exec.groupingAttributes,
          func = exec.func,
          output = exec.output,
          child = exec.child
        )
    }
  }

  private def replaceFlatMapCoGroupsInPandas(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case exec: FlatMapCoGroupsInPandasExec if !exec.isInstanceOf[DataFlintFlatMapCoGroupsInPandasExec] =>
        logWarning(s"Replacing FlatMapCoGroupsInPandasExec with DataFlint version for Spark $sparkMinorVersion")
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

  /**
   * Replaces PythonMapInArrowExec nodes with DataFlint instrumented versions.
   *
   * This is in a separate method because PythonMapInArrowExec was introduced in Spark 3.3
   * (SPARK-37227). On Spark 3.0–3.2, the class doesn't exist and referencing it in a pattern
   * match would cause NoClassDefFoundError. By isolating this in its own method with a
   * try-catch, we gracefully handle older Spark versions.
   */
  private def replaceMapInArrow(plan: SparkPlan): SparkPlan = {
    import org.apache.spark.sql.execution.python.PythonMapInArrowExec

    try {
      plan.transformUp {
        case mapInArrow: PythonMapInArrowExec =>
          logWarning(s"Replacing PythonMapInArrowExec with DataFlint version for Spark $sparkMinorVersion")
          sparkMinorVersion match {
            case "3.5" =>
              DataFlintPythonMapInArrowExec_3_5(
                func = mapInArrow.func,
                output = mapInArrow.output,
                child = mapInArrow.child,
                isBarrier = mapInArrow.isBarrier
              )
            case _ =>
              // 3.3–3.4: share the same 3-arg constructor, handled via reflection
              DataFlintPythonMapInArrowExec_3_3(
                func = mapInArrow.func,
                output = mapInArrow.output,
                child = mapInArrow.child
              )
          }
      }
    } catch {
      case _: NoClassDefFoundError =>
        logWarning("PythonMapInArrowExec not available in this Spark version (requires 3.3+) - mapInArrow instrumentation disabled")
        plan
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
case class DataFlintWindowPlannerStrategy(session: SparkSession) extends Strategy with Logging {

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
        logWarning("Replacing logical Window with DataFlintWindowExec")
        DataFlintWindowExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case PhysicalWindow(
        WindowFunctionType.Python, windowExprs, partitionSpec, orderSpec, child) =>
        logWarning("Replacing logical Window (Python UDF) with DataFlintWindowInPandasExec")
        DataFlintWindowInPandasExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil
      case _ => Nil
    }
  }
}