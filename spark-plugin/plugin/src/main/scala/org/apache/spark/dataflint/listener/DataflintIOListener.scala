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

  /**
   * Per-event size budget for the targets payload (rough upper bound of JSON bytes).
   * Databricks drops any SparkListenerEvent larger than ~2 MB; we cap chunks at
   * 1 MB so the JSON envelope + base fields still fit comfortably.
   */
  private val maxChunkBytes = 1024 * 1024

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
        var chunkIndex = 0
        chunkTargets(targets).foreach { chunk =>
          val info = DataflintIOInfo(
            executionId = qe.id,
            sqlFuncName = funcName,
            durationMs = durationMs,
            chunkIndex = chunkIndex,
            targets = chunk
          )
          bus.post(DataflintIOEvent(info))
          chunkIndex += 1
        }
      }
    }.recover {
      case t: Throwable => logWarning("DataflintIOListener: failed to process query", t)
    }
  }

  /**
   * Greedy pack: accumulate targets in order until the next would push the chunk
   * over the budget, then flush. A single target larger than the budget gets its
   * own chunk (we prefer attempting a too-large event over dropping data).
   */
  private[listener] def chunkTargets(targets: Seq[DataflintIOTarget]): Seq[Seq[DataflintIOTarget]] = {
    val out = collection.mutable.ArrayBuffer.empty[Seq[DataflintIOTarget]]
    val current = collection.mutable.ArrayBuffer.empty[DataflintIOTarget]
    var currentBytes = 0
    targets.foreach { t =>
      val s = estimateBytes(t)
      if (current.nonEmpty && currentBytes + s > maxChunkBytes) {
        // toList — eager immutable copy. ArrayBuffer.toSeq in Scala 2.12 returns
        // `this`, so a later current.clear() would empty the seq we just stored.
        out += current.toList
        current.clear()
        currentBytes = 0
      }
      current += t
      currentBytes += s
    }
    if (current.nonEmpty) out += current.toList
    out.toList
  }

  /** Rough upper bound for the JSON-serialized size of one target. */
  private[listener] def estimateBytes(t: DataflintIOTarget): Int = {
    val strBytes =
      t.format.length +
      t.operation.length +
      t.tableName.fold(0)(_.length) +
      t.saveMode.fold(0)(_.length) +
      t.paths.iterator.map(_.length).sum +
      t.partitionColumns.iterator.map(_.length).sum +
      t.options.iterator.map { case (k, v) => k.length + v.length }.sum
    // 1.2x for JSON escaping/structure + 256 B per-target constant overhead.
    (strBytes * 12 / 10) + 256
  }
}

