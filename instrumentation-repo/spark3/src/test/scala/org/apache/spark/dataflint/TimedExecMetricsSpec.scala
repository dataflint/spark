package org.apache.spark.dataflint

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for TimedExec metric semantics.
 */
class TimedExecMetricsSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("TimedExecMetricsSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("TimedExec and TimedWithCodegenExec do not compare equal even when wrapping the same child") {
    // FilterExec is CodegenSupport, so TimedExec.apply picks TimedWithCodegenExec for it.
    // Pair it with a hand-built plain TimedExec wrapping the same child to exercise the
    // canEqual / equals contract.
    val df = spark.range(0, 5, 1, 1).filter("id > 1")
    df.collect()
    val timedCodegen = df.queryExecution.executedPlan.collect {
      case t: TimedWithCodegenExec if t.child.getClass.getSimpleName == "FilterExec" => t
    }.headOption.getOrElse(fail("no TimedWithCodegenExec(FilterExec) in plan"))

    val plain = new TimedExec(timedCodegen.child)

    plain.canEqual(timedCodegen) shouldBe false
    timedCodegen.canEqual(plain) shouldBe false
    (plain == timedCodegen) shouldBe false
  }

  test("postRddId overwrites the rddId metric instead of accumulating") {
    // Build a real TimedExec from the rule so we exercise the actual path.
    val df = spark.range(0, 10, 1, 1).filter("id > 5")
    df.collect()
    val timed = df.queryExecution.executedPlan.collect {
      case t: TimedExec if t.child.getClass.getSimpleName == "FilterExec" => t
    }.headOption.getOrElse(fail("no TimedExec(FilterExec) in plan"))

    // postRddId is `protected`; reach it via reflection so we can exercise it with
    // deterministic inputs and not couple the test to RDD-id allocation order.
    val postRddId = classOf[TimedExec].getDeclaredMethod("postRddId", classOf[Int])
    postRddId.setAccessible(true)

    postRddId.invoke(timed, Int.box(100))
    timed.metrics("rddId").value shouldBe 100L

    postRddId.invoke(timed, Int.box(200))
    // With `+=` semantics this would be 300; with `set` semantics it is 200.
    timed.metrics("rddId").value shouldBe 200L
  }
}
