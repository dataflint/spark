package org.apache.spark.dataflint

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.python.{
  DataFlintMapInPandasExec_3_0,
  DataFlintMapInPandasExec_3_1,
  DataFlintMapInPandasExec_3_3,
  DataFlintMapInPandasExec_3_4,
  DataFlintMapInPandasExec_3_5,
  DataFlintPythonMapInArrowExec_3_3,
  DataFlintPythonMapInArrowExec_3_4,
  DataFlintPythonMapInArrowExec_3_5,
  MapInPandasExec
}

/**
 * A SparkSessionExtension that injects DataFlint instrumentation into Spark's physical planning phase.
 * This extension replaces:
 *   - MapInPandasExec with the version-appropriate DataFlintMapInPandasExec (adds duration metric)
 *   - PythonMapInArrowExec with the version-appropriate DataFlintPythonMapInArrowExec (adds duration metric)
 *
 * Supports Spark 3.0.x through 3.5.x with version-specific implementations that match
 * each version's internal API.
 *
 * Note: mapInArrow instrumentation is only available on Spark 3.3+ (PythonMapInArrowExec was
 * introduced in SPARK-37227). On Spark 3.0–3.2, only mapInPandas instrumentation is supported.
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

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    if (!mapInPandasEnabled && !mapInArrowEnabled) plan
    else {
      var result = plan

      if (mapInPandasEnabled) {
        result = replaceMapInPandas(result)
      }

      if (mapInArrowEnabled) {
        result = replaceMapInArrow(result)
      }

      result
    }
  }

  private def replaceMapInPandas(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case mapInPandas: MapInPandasExec =>
        logInfo(s"Replacing MapInPandasExec with DataFlint version for Spark $sparkMinorVersion")
        sparkMinorVersion match {
          case "3.0" =>
            DataFlintMapInPandasExec_3_0(
              func = mapInPandas.func,
              output = mapInPandas.output,
              child = mapInPandas.child
            )
          case "3.1" | "3.2" =>
            DataFlintMapInPandasExec_3_1(
              func = mapInPandas.func,
              output = mapInPandas.output,
              child = mapInPandas.child
            )
          case "3.3" =>
            DataFlintMapInPandasExec_3_3(
              func = mapInPandas.func,
              output = mapInPandas.output,
              child = mapInPandas.child
            )
          case "3.4" =>
            DataFlintMapInPandasExec_3_4(
              func = mapInPandas.func,
              output = mapInPandas.output,
              child = mapInPandas.child
            )
          case _ =>
            // Default to 3.5 implementation for 3.5.x and any future 3.x
            DataFlintMapInPandasExec_3_5(
              func = mapInPandas.func,
              output = mapInPandas.output,
              child = mapInPandas.child,
              isBarrier = mapInPandas.isBarrier
            )
        }
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
          logInfo(s"Replacing PythonMapInArrowExec with DataFlint version for Spark $sparkMinorVersion")
          sparkMinorVersion match {
            case "3.3" =>
              DataFlintPythonMapInArrowExec_3_3(
                func = mapInArrow.func,
                output = mapInArrow.output,
                child = mapInArrow.child
              )
            case "3.4" =>
              DataFlintPythonMapInArrowExec_3_4(
                func = mapInArrow.func,
                output = mapInArrow.output,
                child = mapInArrow.child
              )
            case _ =>
              // Default to 3.5 implementation for 3.5.x and any future 3.x
              DataFlintPythonMapInArrowExec_3_5(
                func = mapInArrow.func,
                output = mapInArrow.output,
                child = mapInArrow.child,
                isBarrier = mapInArrow.isBarrier
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
