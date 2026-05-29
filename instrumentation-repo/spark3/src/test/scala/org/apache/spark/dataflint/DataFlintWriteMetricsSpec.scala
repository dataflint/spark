package org.apache.spark.dataflint

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Regression test for the executeCollect write-path rebuild in TimedExec:
 *   TimedExec(DataWritingCommandExec(WriteFilesExec(dataPlan)))
 * gets rebuilt at execute-time as
 *   NewDataWritingCommandExec(NewWriteFilesExec(RDDTimingWrapper(dataPlan)))
 * and `executeCollect` runs on the rebuilt root.
 *
 * The rebuild is safe for metrics because:
 *  - DataWritingCommandExec.metrics delegates to cmd.metrics, and `withNewChildren` reuses
 *    the same cmd instance — so numOutputRows / numFiles / numOutputBytes / etc. are all
 *    the same SQLMetric objects on both the original and rebuilt nodes.
 *  - WriteFilesExec (Spark 3.4+) has no metrics of its own.
 *  - The data plan is shared by reference via RDDTimingWrapper, so its metrics are the
 *    original instances.
 *
 * If a future change breaks any of these assumptions, the metrics displayed on the original
 * (UI-visible) plan tree would go stale; this spec catches that.
 */
class DataFlintWriteMetricsSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val capturedQE = new AtomicReference[QueryExecution](null)

  private val listener = new QueryExecutionListener {
    override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
      capturedQE.set(qe)
    override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = ()
  }

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintWriteMetricsSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
    spark.listenerManager.register(listener)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.listenerManager.unregister(listener)
      spark.stop()
    }
  }

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles).foreach(_.foreach(deleteRecursively))
    f.delete()
  }

  test("DataWritingCommandExec metrics survive the executeCollect rebuild") {
    val tempDir = java.nio.file.Files.createTempDirectory("dataflint-write-test").toFile
    try {
      capturedQE.set(null)
      val session = spark
      import session.implicits._
      Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)).toDF("key", "value")
        .write.mode("overwrite").parquet(tempDir.getAbsolutePath)

      val qe = capturedQE.get()
      qe should not be null

      // The DataFlint-wrapped DataWritingCommandExec — metrics on this node feed the SparkUI.
      val timedDwce = qe.executedPlan.collect {
        case t: TimedExec if t.child.getClass.getSimpleName == "DataWritingCommandExec" => t
      }.headOption.getOrElse(fail(s"No TimedExec(DataWritingCommandExec) in plan:\n${qe.executedPlan.treeString}"))

      // cmd.metrics is shared via withNewChildren, so these must reach the original node.
      val m = timedDwce.metrics
      withClue(s"metrics: ${m.map { case (k, v) => s"$k=${v.value}" }.mkString(", ")}\n") {
        m("numOutputRows").value shouldBe 4L
        m("numFiles").value      shouldBe 1L
        m("numOutputBytes").value should be > 0L
        m("duration").value      should be >= 0L
      }

      // The wrapped data plan shares its metrics with the rebuilt RDDTimingWrapper subtree.
      val scan = qe.executedPlan.collect {
        case n if n.getClass.getSimpleName == "LocalTableScanExec" => n
      }.headOption.getOrElse(fail("No LocalTableScanExec in plan"))
      scan.metrics("numOutputRows").value shouldBe 4L
    } finally {
      deleteRecursively(tempDir)
    }
  }
}
