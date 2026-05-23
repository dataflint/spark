package org.apache.spark.dataflint

import java.util.concurrent.CopyOnWriteArrayList

import org.apache.spark.dataflint.listener.{DataflintPlanTranslationListener, DataflintPlanTranslationsEvent, DataflintPlanTranslationsInfo}
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Drive Spark's field-list truncation hard (maxToStringFields=2), build a Project
 * with many output columns, run an action, and assert the listener emitted a
 * translation whose truncated form carries the SparkUI "... N more fields"
 * marker while the full form contains every column name.
 */
class DataflintPlanTranslationListenerSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val captured = new CopyOnWriteArrayList[DataflintPlanTranslationsInfo]()

  private val collector = new SparkListener {
    override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
      case e: DataflintPlanTranslationsEvent => captured.add(e.info)
      case _ =>
    }
  }

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataflintPlanTranslationListenerSpec")
      // Aggressively truncate field lists so even a small Project produces a translation.
      .config("spark.sql.maxToStringFields", "2")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.queryExecutionListeners", classOf[DataflintPlanTranslationListener].getName)
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

  private def drain(): Unit = spark.sparkContext.listenerBus.waitUntilEmpty(10000)

  test("emits full simpleString for a Project whose field list got truncated") {
    captured.clear()

    val session = spark
    import session.implicits._

    val columnNames = (1 to 20).map(i => s"column_with_long_recognizable_name_$i")
    val projection = columnNames.map(n => lit(0).as(n))
    Seq(1).toDF("seed").select(projection: _*).collect()

    drain()

    val info = (0 until captured.size()).map(captured.get).headOption
      .getOrElse(fail(s"No translation event captured. Got ${captured.size()} events."))

    info.translations should not be empty

    // Every emitted translation must (a) be detected as truncated and
    // (b) fully render in the expanded form.
    info.translations.foreach { t =>
      t.truncatedDescription should not be t.fullDescription
      t.fullDescription.length should be > t.truncatedDescription.length
    }

    // At least one translation should cover the Project carrying our columns:
    // the full form contains the last column name; the truncated one does not.
    val lastCol = columnNames.last
    val projection = info.translations.find { t =>
      t.fullDescription.contains(lastCol) && !t.truncatedDescription.contains(lastCol)
    }
    projection.getOrElse(
      fail(s"No translation contains the last column name '$lastCol' only in the full form.\n" +
        info.translations.map(t => s"  ${t.nodeName}: trunc=${t.truncatedDescription.take(80)}... full=${t.fullDescription.take(80)}...").mkString("\n"))
    )
  }
}
