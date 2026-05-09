package org.apache.spark.dataflint

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Regression test for issue #74: TimedWithCodegenExec.doConsume must propagate the
 * `row: ExprCode` parameter so downstream CodegenFallback expressions (e.g. from_json)
 * that reference ctx.INPUT_ROW in their generated code see a valid row variable.
 *
 * Without the fix, this throws an NPE inside Block.code interpolation:
 *   java.lang.NullPointerException: Cannot invoke "Object.getClass()" because "arg" is null
 */
class DataFlintCodegenFallbackSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintCodegenFallbackSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SQL_NODES_ENABLED, "true")
      .config("spark.sql.codegen.wholeStage", "true")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.ui.enabled", "false")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("from_json under whole-stage codegen does not NPE (issue #74)") {
    import spark.implicits._

    val schema = ArrayType(StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("kind", StringType, nullable = true)
    )))

    val df = Seq(
      ("k1", """[{"name":"a","kind":"x"}]"""),
      ("k2", null.asInstanceOf[String])
    ).toDF("key", "payload")

    val count = df
      .filter(col("payload").isNotNull)
      .withColumn("parsed", from_json(col("payload"), schema))
      .filter(col("parsed").isNotNull)
      .select(col("key"), explode(col("parsed")).alias("d"))
      .filter(col("d.name").isNotNull)
      .count()

    count shouldBe 1L
  }
}
