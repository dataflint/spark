package io.dataflint.example

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, SparkPlan}
import org.apache.spark.sql.execution.python.{DataFlintMapInPandasExec_3_5, MapInPandasExec}

/**
 * A SparkSessionExtension that injects DataFlint instrumentation into Spark's physical planning phase.
 * This extension replaces:
 *   - MapInPandasExec with DataFlintMapInPandasExec_3_5 (adds duration metric)
 * 
 * Usage:
 *   .config("spark.sql.extensions", "io.dataflint.example.DataFlintInstrumentationExtension")
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
 */
case class DataFlintInstrumentationColumnarRule(session: SparkSession) extends ColumnarRule with Logging {

  override def preColumnarTransitions: Rule[SparkPlan] = { plan =>
    plan.transformUp {
      // Replace MapInPandasExec with DataFlintMapInPandasExec_3_5
      case mapInPandas: MapInPandasExec =>
        logInfo(s"Replacing MapInPandasExec with DataFlintMapInPandasExec_3_5")
        DataFlintMapInPandasExec_3_5(
          func = mapInPandas.func,
          output = mapInPandas.output,
          child = mapInPandas.child,
          isBarrier = mapInPandas.isBarrier
        )
    }
  }
}
