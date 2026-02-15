package org.apache.spark.dataflint

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.python.{
  DataFlintMapInPandasExec_4_0,
  DataFlintMapInPandasExec_4_1,
  DataFlintPythonMapInArrowExec_4_0,
  DataFlintPythonMapInArrowExec_4_1,
  MapInArrowExec,
  MapInPandasExec
}

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

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    if (!mapInPandasEnabled && !mapInArrowEnabled) plan
    else plan.transformUp {
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
            // Default to 4.1 implementation for 4.1.x and any future 4.x
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
            // Default to 4.1 implementation for 4.1.x and any future 4.x
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
}
