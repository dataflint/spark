package org.apache.spark.dataflint

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.python.{
  DataFlintMapInPandasExec_3_3,
  DataFlintMapInPandasExec_3_4,
  DataFlintMapInPandasExec_3_5,
  DataFlintPythonMapInArrowExec_3_3,
  DataFlintPythonMapInArrowExec_3_4,
  DataFlintPythonMapInArrowExec_3_5,
  MapInPandasExec,
  PythonMapInArrowExec
}

/**
 * A SparkSessionExtension that injects DataFlint instrumentation into Spark's physical planning phase.
 * This extension replaces:
 *   - MapInPandasExec with the version-appropriate DataFlintMapInPandasExec (adds duration metric)
 *   - PythonMapInArrowExec with the version-appropriate DataFlintPythonMapInArrowExec (adds duration metric)
 *
 * Supports Spark 3.3.x, 3.4.x, and 3.5.x with version-specific implementations that match
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
  }
}

/**
 * A ColumnarRule that replaces physical plan nodes with DataFlint instrumented versions.
 * The replacement happens in preColumnarTransitions to ensure it runs before any columnar optimizations.
 *
 * Uses runtime Spark version detection to select the correct implementation:
 *   - 3.3.x: DataFlintMapInPandasExec_3_3 / DataFlintPythonMapInArrowExec_3_3
 *   - 3.4.x: DataFlintMapInPandasExec_3_4 / DataFlintPythonMapInArrowExec_3_4
 *   - 3.5.x: DataFlintMapInPandasExec_3_5 / DataFlintPythonMapInArrowExec_3_5
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
    else plan.transformUp {
      case mapInPandas: MapInPandasExec if mapInPandasEnabled =>
        logInfo(s"Replacing MapInPandasExec with DataFlint version for Spark $sparkMinorVersion")
        sparkMinorVersion match {
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
      case mapInArrow: PythonMapInArrowExec if mapInArrowEnabled =>
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
  }
}
