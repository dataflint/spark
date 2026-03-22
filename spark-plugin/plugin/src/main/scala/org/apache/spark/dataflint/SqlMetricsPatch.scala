package org.apache.spark.dataflint

import javassist.ClassPool

import org.apache.spark.internal.Logging

private[spark] object SqlMetricsPatch extends Logging {
  // Patch SQLMetrics.stringValue to handle unknown DataSource V2 metric types (e.g. "value" from
  // the BigQuery connector) instead of throwing IllegalStateException. Runs once at ServiceLoader
  // discovery time — before any event log replay uses SQLMetrics$.
  val sqlMetricsPatchApplied: Boolean = {
    if (sys.env.getOrElse("DATAFLINT_PATCH_SQL_METRICS", "false").toLowerCase != "true") {
      logInfo("DataFlint: SQLMetrics patch disabled (set DATAFLINT_PATCH_SQL_METRICS=true to enable)")
      false
    } else
    try {
      val pool = ClassPool.getDefault
      val ct = pool.get("org.apache.spark.sql.execution.metric.SQLMetrics$")
      val method = ct.getMethod("stringValue", "(Ljava/lang/String;[J[J)Ljava/lang/String;")
      method.addCatch(
        """{
          |  if ($e.getMessage() != null && $e.getMessage().startsWith("unexpected metrics type:")) {
          |    return "N/A";
          |  }
          |  throw $e;
          |}""".stripMargin,
        pool.get("java.lang.IllegalStateException")
      )
      ct.toClass(Thread.currentThread().getContextClassLoader, null)
      logInfo("DataFlint: patched SQLMetrics.stringValue to handle unknown DataSource V2 metric types")
      true
    } catch {
      case e: Exception =>
        logWarning(
          s"DataFlint: could not patch SQLMetrics.stringValue (${e.getClass.getSimpleName}: ${e.getMessage}). " +
          "Jobs using BigQuery or other connectors with custom metric types may fail to load in history server."
        )
        false
    }
  }
}