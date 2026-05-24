package org.apache.spark.dataflint.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import scala.util.Try

/**
 * QueryExecutionListener that walks the analyzed logical plan after every action
 * and posts a [[DataflintIOEvent]] with the full (untruncated) paths and table
 * identifiers extracted from typed plan nodes.
 *
 * Registered automatically by [[org.apache.spark.dataflint.DataflintSparkUICommonLoader]]
 * via `spark.sql.queryExecutionListeners` when `spark.dataflint.io.tracking.enabled=true`.
 *
 * Must have a no-arg constructor: Spark instantiates listeners listed in
 * `spark.sql.queryExecutionListeners` via `Class.getConstructor()` reflection.
 */
class DataflintIOListener extends QueryExecutionListener with Logging {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    process(funcName, qe, durationNs)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    // Still try — the plan was constructed even if execution failed.
    process(funcName, qe, 0L)
  }

  private def process(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    Try {
      val targets = DataflintIOExtractor.extract(qe)
      if (targets.nonEmpty) {
        val durationMs = durationNs / 1000000L
        val bus = qe.sparkSession.sparkContext.listenerBus
        // One event per target — keeps each event small enough to stay well under
        // Databricks' 2 MB per-SparkListenerEvent cap and ensures we never drop a
        // path because the whole batch was too large.
        targets.foreach { target =>
          val info = DataflintIOInfo(
            executionId = qe.id,
            sqlFuncName = funcName,
            durationMs = durationMs,
            target = target
          )
          bus.post(DataflintIOEvent(info))
        }
      }
    }.recover {
      case t: Throwable => logWarning("DataflintIOListener: failed to process query", t)
    }
  }
}
