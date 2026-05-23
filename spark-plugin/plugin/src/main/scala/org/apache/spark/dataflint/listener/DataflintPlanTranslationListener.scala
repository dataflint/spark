package org.apache.spark.dataflint.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import scala.util.Try

/**
 * Generic plan-truncation listener: for every node in `qe.executedPlan`, compare
 * `simpleString(maxToStringFields)` (the form SparkUI shows) to
 * `simpleString(Int.MaxValue)` (truncation disabled). When they differ, emit the
 * full rendering as a [[DataflintPlanNodeTranslation]] keyed by pre-order
 * `nodeIndex` — the same indexing scheme SparkPlanGraph uses, so consumers can
 * look up translations for nodes whose description got cut.
 *
 * Format-agnostic: works for FileScan, Project, Aggregate, Window, custom nodes —
 * anything that overrides `simpleString` properly. Sparse: nodes that fit emit
 * nothing and cost one String#equals.
 *
 * Registered automatically by [[org.apache.spark.dataflint.DataflintSparkUICommonLoader]]
 * via `spark.sql.queryExecutionListeners` when
 * `spark.dataflint.plan.translation.enabled=true`.
 *
 * Must have a no-arg constructor: Spark instantiates listeners listed in
 * `spark.sql.queryExecutionListeners` via `Class.getConstructor()` reflection.
 */
class DataflintPlanTranslationListener extends QueryExecutionListener with Logging {

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    process(qe)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    process(qe)
  }

  private def process(qe: QueryExecution): Unit = {
    Try {
      val maxFields = qe.sparkSession.sessionState.conf.maxToStringFields
      val translations = collection.mutable.ArrayBuffer.empty[DataflintPlanNodeTranslation]
      var idx = 0
      qe.executedPlan.foreach { node =>
        val truncated = Try(node.simpleString(maxFields)).getOrElse("")
        val full = Try(node.simpleString(Int.MaxValue)).getOrElse(truncated)
        if (truncated != full) {
          translations += DataflintPlanNodeTranslation(
            nodeIndex = idx,
            nodeName = node.nodeName,
            truncatedDescription = truncated,
            fullDescription = full
          )
        }
        idx += 1
      }
      if (translations.nonEmpty) {
        val info = DataflintPlanTranslationsInfo(qe.id, translations.toSeq)
        qe.sparkSession.sparkContext.listenerBus.post(DataflintPlanTranslationsEvent(info))
      }
    }.recover {
      case t: Throwable => logWarning("DataflintPlanTranslationListener: failed to process query", t)
    }
  }
}
