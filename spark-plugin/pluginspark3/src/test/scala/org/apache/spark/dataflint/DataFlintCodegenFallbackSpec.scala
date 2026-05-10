package org.apache.spark.dataflint

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, from_json}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Regression test for issue #74: TimedWithCodegenExec must exclude itself from
 * whole-stage codegen when its child contains a CodegenFallback expression (e.g.
 * from_json / JsonToStructs). The transparent `children = child.children` hides
 * the wrapped child from CollapseCodegenStages' CodegenFallback check, so that
 * exclusion has to happen in TimedWithCodegenExec.supportCodegen.
 *
 * Without the fix, the wrapped child gets wholestaged anyway and CodegenFallback's
 * generated code reads ctx.INPUT_ROW = null, NPE'ing in Block.code interpolation:
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
    val session = spark
    import session.implicits._

    val schema = ArrayType(StructType(Seq(
      StructField("name", StringType, nullable = true),
      StructField("kind", StringType, nullable = true)
    )))

    // Cache to materialize the rows so JsonToStructs is not constant-folded into the
    // LocalTableScan — otherwise from_json never reaches whole-stage codegen and the bug
    // doesn't fire.
    val raw = Seq(
      ("k1", """[{"name":"a","kind":"x"}]"""),
      ("k2", null.asInstanceOf[String])
    ).toDF("key", "payload")
      .repartition(1)
      .cache()
    raw.count()

    val rowCount: Long = raw
      .filter(col("payload").isNotNull)
      .withColumn("parsed", from_json(col("payload"), schema))
      .filter(col("parsed").isNotNull)
      .select(col("key"), explode(col("parsed")).alias("d"))
      .filter(col("d.name").isNotNull)
      .count()

    rowCount shouldBe 1L
  }
}
