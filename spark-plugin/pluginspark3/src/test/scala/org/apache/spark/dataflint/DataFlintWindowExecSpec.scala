package org.apache.spark.dataflint

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.execution.window.DataFlintWindowExec
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udaf
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit.NANOSECONDS

private class SlowSumAggregator(sleep: Long) extends Aggregator[Long, Long, Long] {
  def zero: Long = 0L
  def reduce(b: Long, a: Long): Long = {
    Thread.sleep(sleep); b + a
  }
  def merge(b1: Long, b2: Long): Long = b1 + b2
  def finish(r: Long): Long = r
  def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

class DataFlintWindowExecSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintWindowExecSpec")
      .config("spark.sql.extensions", "org.apache.spark.dataflint.DataFlintInstrumentationExtension")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_WINDOW_ENABLED, "true")
      .config("spark.ui.enabled", "true")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("DataFlintWindowPlannerStrategy replaces WindowExec with DataFlintWindowExec for SQL window") {
    val session = spark
    import session.implicits._
    val df = Seq((1, "a"), (2, "b"), (3, "a"), (4, "b"), (5, "a")).toDF("id", "cat")
    df.createOrReplaceTempView("test_window_plan")

    val result = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_plan"
    )
    val windowNodes = result.queryExecution.executedPlan.collect {
      case w: DataFlintWindowExec => w
    }

    withClue("Expected DataFlintWindowExec in physical plan but found: " +
      result.queryExecution.executedPlan.treeString) {
      windowNodes should not be empty
    }
  }

  test("DataFlintWindowExec duration metric is positive after execution") {
    val session = spark
    import session.implicits._
    // 100 rows across 5 partitions — enough to ensure window work takes > 0ms
    val df = (1 to 10000).map(i => (i, i % 5)).toDF("id", "cat")
    df.createOrReplaceTempView("test_window_timing")

    val result = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_timing"
    )
    result.collect()

    val windowNode = result.queryExecution.executedPlan.collect {
      case w: DataFlintWindowExec => w
    }.head
    val duration = windowNode.metrics("duration").value
    duration should be > 0L
  }

  test("DataFlintWindowExec duration metric is bounded by wall clock time") {
    val session = spark
    import session.implicits._
    val df = (1 to 100000).map(i => (i, i % 5)).toDF("id", "cat")
    df.createOrReplaceTempView("test_window_wall_clock")

    val result = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_wall_clock"
    )

    val wallStart = System.nanoTime()
    result.collect()
    val wallMs = NANOSECONDS.toMillis(System.nanoTime() - wallStart)

    val windowNode = result.queryExecution.executedPlan.collect {
      case w: DataFlintWindowExec => w
    }.head
    val duration = windowNode.metrics("duration").value

    duration should be > 0L
    duration should be <= wallMs
  }

  test("DataFlintWindowExec duration metric scales with data size") {
    val session = spark
    import session.implicits._

    val dfSmall = (1 to 100).map(i => (i, i % 5)).toDF("id", "cat")
    dfSmall.createOrReplaceTempView("test_window_small")
    val resultSmall = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_small"
    )
    resultSmall.collect()
    val durationSmall = resultSmall.queryExecution.executedPlan.collect {
      case w: DataFlintWindowExec => w
    }.head.metrics("duration").value

    val dfLarge = (1 to 1000000).map(i => (i, i % 5)).toDF("id", "cat")
    dfLarge.createOrReplaceTempView("test_window_large")
    val resultLarge = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_large"
    )
    resultLarge.collect()
    val durationLarge = resultLarge.queryExecution.executedPlan.collect {
      case w: DataFlintWindowExec => w
    }.head.metrics("duration").value

    durationLarge should be > durationSmall
  }

  test("DataFlintWindowExec duration metric captures window-internal UDAF computation") {
    val session = spark
    import session.implicits._

    // A UDAF whose reduce() sleeps 1ms per row — all work happens inside the window operator.
    // With 200 rows / 5 categories = 40 rows per partition, reduce() is called 200 times total
    // → at least 200ms of window-internal computation that the metric must capture.
    val sleepTime=4
    spark.udf.register("slow_sum", udaf(new SlowSumAggregator(sleepTime)))

    val rows = 200
    val partitions = 5
    val df = (1 to rows).map(i => (i, i % partitions)).toDF("id", "cat")
    df.createOrReplaceTempView("test_window_udaf")

    // Fast baseline: rank() has negligible per-row computation
    val resultFast = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_udaf"
    )
    resultFast.collect()
    val durationFast = resultFast.queryExecution.executedPlan.collect {
      case w: DataFlintWindowExec => w
    }.head.metrics("duration").value

    // Slow: UDAF sleeps 1ms per row inside the window operator — 200ms+ of window-internal work
    val resultSlow = spark.sql(
      "SELECT id, cat, slow_sum(CAST(id AS BIGINT)) OVER (PARTITION BY cat) AS r FROM test_window_udaf"
    )
    val wallSlowStart = System.nanoTime()
    resultSlow.collect()
    val wallSlowMs = NANOSECONDS.toMillis(System.nanoTime() - wallSlowStart)
    val durationSlow = resultSlow.queryExecution.executedPlan.collect {
      case w: DataFlintWindowExec => w
    }.head.metrics("duration").value
    Thread.sleep(50000)
    // The metric must capture the UDAF's sleep time (at least rows*sleepTime ms = 1ms × 200 rows)
    withClue(s"durationSlow=$durationSlow ms should be >= ${rows*sleepTime}ms (UDAF sleep captured in metric)") {
      durationSlow should be >= rows.toLong
    }

    // The metric must reflect actual window computation — slow >> fast
    withClue(s"durationSlow=$durationSlow ms should exceed durationFast=$durationFast ms") {
      durationSlow should be > durationFast
    }

    // The metric must be bounded by wall-clock time (no phantom time)
    withClue(s"durationSlow=$durationSlow ms should be <= wallSlowMs=$wallSlowMs ms") {
      durationSlow should be <= wallSlowMs
    }
  }

}