package org.apache.spark.dataflint

import org.apache.spark.sql.execution.{ExplicitRepartitionExtension, ExplicitRepartitionOps}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions.{col, rank, udaf}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit.NANOSECONDS

private class SlowSumAggregator(fromSleep: Long, toSleep: Long) extends Aggregator[Long, Long, Long] {
  def zero: Long = 0L
  def reduce(b: Long, a: Long): Long = {
    b + a
  }
  def merge(b1: Long, b2: Long): Long = b1 + b2
  def finish(r: Long): Long = {
    val sleep = fromSleep + (math.random() * (toSleep - fromSleep)).toLong
    Thread.sleep(sleep)
    println(s"SlowSumAggregator finished with sleep of $sleep")
    r
  }
  def bufferEncoder: Encoder[Long] = Encoders.scalaLong
  def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

class DataFlintWindowExecSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll with SqlMetricTestHelper {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintWindowExecSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_WINDOW_ENABLED, "true")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .withExtensions(new ExplicitRepartitionExtension)
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // With AQE, executedPlan is AdaptiveSparkPlanExec. After collect(), finalPhysicalPlan holds
  // the fully optimised plan. TimedExec is injected by the ColumnarRule (preColumnarTransitions),
  // so it only appears after execution (in finalPhysicalPlan), not in sparkPlan.
  private def finalPlan(df: DataFrame) = df.queryExecution.executedPlan match {
    case aqe: AdaptiveSparkPlanExec => aqe.finalPhysicalPlan
    case p                          => p
  }

  test("DataFlintInstrumentationColumnarRule wraps WindowExec with TimedExec") {
    val session = spark
    import session.implicits._
    val df = Seq((1, "a"), (2, "b"), (3, "a"), (4, "b"), (5, "a")).toDF("id", "cat")
    df.createOrReplaceTempView("test_window_plan")

    val result = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_plan"
    )
    result.collect()

    val windowNodes = finalPlan(result).collect {
      case t: TimedExec if t.child.isInstanceOf[WindowExec] => t
    }

    withClue("Expected TimedExec(WindowExec) in physical plan but found: " +
      finalPlan(result).treeString) {
      windowNodes should not be empty
    }
  }

  test("TimedExec(WindowExec) duration metric is positive after execution") {
    val session = spark
    import session.implicits._
    // 100 rows across 5 partitions — enough to ensure window work takes > 0ms
    val df = (1 to 10000).map(i => (i, i % 5)).toDF("id", "cat")
    df.createOrReplaceTempView("test_window_timing")

    val result = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_timing"
    )
    result.collect()

    val windowNode = finalPlan(result).collect {
      case t: TimedExec if t.child.isInstanceOf[WindowExec] => t
    }.head
    val duration = windowNode.metrics("duration").value
    duration should be > 0L
  }

  test("TimedExec(WindowExec) duration metric is bounded by wall clock time") {
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

    val windowNode = finalPlan(result).collect {
      case t: TimedExec if t.child.isInstanceOf[WindowExec] => t
    }.head
    val duration = windowNode.metrics("duration").value

    duration should be > 0L
    duration should be <= wallMs
  }

  test("TimedExec(WindowExec) duration metric scales with data size") {
    val session = spark
    import session.implicits._

    val dfSmall = (1 to 100).map(i => (i, i % 5)).toDF("id", "cat")
    dfSmall.createOrReplaceTempView("test_window_small")
    val resultSmall = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_small"
    )
    resultSmall.collect()
    val durationSmall = finalPlan(resultSmall).collect {
      case t: TimedExec if t.child.isInstanceOf[WindowExec] => t
    }.head.metrics("duration").value

    val dfLarge = (1 to 1000000).map(i => (i, i % 5)).toDF("id", "cat")
    dfLarge.createOrReplaceTempView("test_window_large")
    val resultLarge = spark.sql(
      "SELECT id, cat, rank() OVER (PARTITION BY cat ORDER BY id) AS r FROM test_window_large"
    )
    resultLarge.collect()
    val durationLarge = finalPlan(resultLarge).collect {
      case t: TimedExec if t.child.isInstanceOf[WindowExec] => t
    }.head.metrics("duration").value

    durationLarge should be > durationSmall
  }

  test("TimedExec(WindowExec) duration metric captures window-internal UDAF computation") {
    val session = spark
    import session.implicits._

    // A UDAF whose finish() sleeps 1ms per partition — all work happens inside the window operator.
    // With 200 rows / 5 categories = 40 rows per partition, finish() is called 5 times total
    // → at least 20ms of window-internal computation that the metric must capture.
    val sleepTime=4
    spark.udf.register("slow_sum", udaf(new SlowSumAggregator(sleepTime, 20)))

    val rows = 200
    val partitions = 5
    // Pre-repartition by cat so Spark reuses the existing HashPartitioning(cat, 5) for the
    // window exchange (skips the shuffle), guaranteeing exactly `partitions` tasks.
    val dforg = (1 to rows).map(i => (i, i % partitions)).toDF("id", "cat")
      .cache()
    dforg.count()

    //repartition by exact number of partitions (require adaptive and ExplicitRepartitionExtension)
    import ExplicitRepartitionOps._
    val df = dforg
      .adaptiveRepartition(col("cat"))
    df.createOrReplaceTempView("test_window_udaf")

    // Fast baseline: rank() has negligible per-row computation
    val resultFast = df.withColumn("r", rank().over(Window.partitionBy("cat").orderBy("id")))
    resultFast.collect()
    val durationFast = finalPlan(resultFast).collect {
      case t: TimedExec if t.child.isInstanceOf[WindowExec] => t
    }.head.metrics("duration").value

    // Set shuffle partitions = partitions so each cat group runs as its own Spark task.
    // Without this the default 200 shuffle partitions leave 195 empty tasks with ~0ms duration,
    // making min trivially 0 and the min/max variation check meaningless.

    // Slow: UDAF finish() sleeps a random time per partition inside the window operator
    val resultSlow = spark.sql(
      "SELECT id, cat, slow_sum(CAST(id AS BIGINT)) OVER (PARTITION BY cat) AS r FROM test_window_udaf"
    )
    val wallSlowStart = System.nanoTime()
    resultSlow.collect()
    val wallSlowMs = NANOSECONDS.toMillis(System.nanoTime() - wallSlowStart)
    val windowNode = finalPlan(resultSlow).collect {
      case t: TimedExec if t.child.isInstanceOf[WindowExec] => t
    }.head
    val durationSlow = windowNode.metrics("duration").value
    // The metric must capture the UDAF's sleep time (at least partitions * fromSleep ms)
    withClue(s"durationSlow=$durationSlow ms should be >= ${partitions*sleepTime}ms (UDAF sleep captured in metric)") {
      durationSlow should be >= partitions.toLong * sleepTime
    }

    // The metric must reflect actual window computation — slow >> fast
    withClue(s"durationSlow=$durationSlow ms should exceed durationFast=$durationFast ms") {
      durationSlow should be > durationFast
    }

    // The metric must be bounded by wall-clock time (no phantom time)
    withClue(s"durationSlow=$durationSlow ms should be <= wallSlowMs=$wallSlowMs ms") {
      durationSlow should be <= wallSlowMs
    }

    // Per-task breakdown: createTimingMetric records each task's value individually.
    // Randomized sleep must produce variation — min and max across partitions must differ.
    implicit val sparkImplicit: SparkSession = spark
    val stats = metricMinMax(windowNode.metrics("duration"))
    withClue(s"min=${stats.min} ms should differ from max=${stats.max} ms (randomized sleep)") {
      stats.min should be < stats.max
    }
    withClue(s"max=${stats.max} ms should be >= sleepTime=$sleepTime ms (sleep was captured in metric)") {
      stats.max should be >= sleepTime.toLong
    }
    withClue(s"min=${stats.min} ms should be >= sleepTime=$sleepTime ms (sleep was captured in metric)") {
      stats.min should be >= sleepTime.toLong
    }
  }
}
