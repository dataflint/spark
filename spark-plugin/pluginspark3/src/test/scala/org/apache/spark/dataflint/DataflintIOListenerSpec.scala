package org.apache.spark.dataflint

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.spark.dataflint.listener.{DataflintIOEvent, DataflintIOInfo, DataflintIOListener, DataflintIOTarget}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Integration test: enable DataflintIOListener via spark.sql.queryExecutionListeners,
 * run a write+read against a deeply-nested temp path, and verify that the listener
 * captured the FULL path — i.e. nothing got cut by Spark's maxToStringFields-driven
 * plan description truncation.
 */
class DataflintIOListenerSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val captured = new CopyOnWriteArrayList[DataflintIOInfo]()

  private val collector = new SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: DataflintIOEvent => captured.add(e.ioInfo)
      case _ =>
    }
  }

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataflintIOListenerSpec")
      // Drive plan-description truncation hard, so a regression-test failure would
      // be visible as a "..." path coming through.
      .config("spark.sql.maxPlanStringLength", "32")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.queryExecutionListeners", classOf[DataflintIOListener].getName)
      .getOrCreate()
    spark.sparkContext.addSparkListener(collector)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.sparkContext.removeSparkListener(collector)
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def deleteRecursively(f: java.io.File): Unit = {
    if (f.isDirectory) Option(f.listFiles).foreach(_.foreach(deleteRecursively))
    f.delete()
  }

  /** Drain the async listener bus so any pending DataflintIOEvent is delivered to the collector. */
  private def drain(): Unit = {
    spark.sparkContext.listenerBus.waitUntilEmpty(10000)
  }

  private def find(op: String): Option[DataflintIOTarget] = {
    val all = (0 until captured.size()).map(captured.get).flatMap(_.targets)
    all.find(_.operation == op)
  }

  test("captures full write path through InsertIntoHadoopFsRelationCommand without truncation") {
    val baseDir = java.nio.file.Files.createTempDirectory("dataflint-io").toFile
    // Deliberately deep+long path so it would be truncated in plan descriptions.
    val deepPath = new java.io.File(baseDir,
      "a-very-long-and-deeply-nested-folder-name-aaaaaaaaaaaa/" +
      "another-long-segment-bbbbbbbbbbbbbbbbbbbb/" +
      "final-leaf-cccccccccccccccccccccccccccccc")
    deepPath.getParentFile.mkdirs()
    try {
      captured.clear()

      val session = spark
      import session.implicits._
      Seq(("a", 1), ("b", 2)).toDF("key", "value")
        .write.mode("overwrite").parquet(deepPath.getAbsolutePath)

      drain()
      val writeTarget = find("write")
        .getOrElse(fail(s"No write target captured. Got ${captured.size()} events."))

      writeTarget.paths should have size 1
      // Crucial assertion: full path, no truncation marker.
      writeTarget.paths.head should include (deepPath.getName)
      writeTarget.paths.head should not include "..."
      writeTarget.saveMode shouldBe Some("Overwrite")
      // nodeId is the pre-order index in qe.executedPlan; the write command exec
      // is the root or near-root, so it should be a small non-negative integer.
      writeTarget.nodeId should be >= 0

      // Now read it back and verify the read target carries the full path too.
      spark.read.parquet(deepPath.getAbsolutePath).collect()

      drain()
      val readTarget = find("read").getOrElse(fail("No read target captured."))
      readTarget.paths.exists(p => p.contains(deepPath.getName) && !p.endsWith("...")) shouldBe true
      readTarget.nodeId should be >= 0
    } finally {
      deleteRecursively(baseDir)
    }
  }
}
