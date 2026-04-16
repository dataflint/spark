package org.apache.spark.dataflint

import org.apache.spark.api.python.{PythonEvalType, SimplePythonFunction}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec, FlatMapCoGroupsInPandasExec, FlatMapGroupsInPandasExec, MapInPandasExec, PythonMapInArrowExec}
import org.apache.spark.sql.types.LongType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DataFlintPythonExecSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataFlintPythonExecSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED, "true")
      .config("spark.ui.enabled", "false")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  private def emptyChild = LocalTableScanExec(output = Seq.empty, rows = Seq.empty)

  private def fakePythonUDF(evalType: Int): PythonUDF = {
    val func = SimplePythonFunction(
      command       = Seq.empty[Byte],
      envVars       = new java.util.HashMap[String, String](),
      pythonIncludes = new java.util.ArrayList[String](),
      pythonExec    = "python3",
      pythonVer     = "3.8",
      broadcastVars = new java.util.ArrayList(),
      accumulator   = null)
    PythonUDF(
      name             = "test_udf",
      func             = func,
      dataType         = LongType,
      children         = Seq.empty,
      evalType         = evalType,
      udfDeterministic = true)
  }

  // ---- MapInPandasExec (mapInPandas) ----

  test("wraps MapInPandasExec with TimedExec") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_PANDAS_ITER_UDF)
    val original = MapInPandasExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[TimedExec]
    result.asInstanceOf[TimedExec].child shouldBe a[MapInPandasExec]
  }

  test("MapInPandasExec TimedExec has duration metric") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_PANDAS_ITER_UDF)
    val original = MapInPandasExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[TimedExec]
    result.metrics should contain key "duration"
  }

  // ---- PythonMapInArrowExec (mapInArrow) ----

  test("wraps PythonMapInArrowExec with TimedExec") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_ARROW_ITER_UDF)
    val original = PythonMapInArrowExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[TimedExec]
    result.asInstanceOf[TimedExec].child shouldBe a[PythonMapInArrowExec]
  }

  test("PythonMapInArrowExec TimedExec has duration metric") {
    val udf = fakePythonUDF(PythonEvalType.SQL_MAP_ARROW_ITER_UDF)
    val original = PythonMapInArrowExec(udf, Seq.empty, emptyChild, false)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[TimedExec]
    result.metrics should contain key "duration"
  }

  // ---- ArrowEvalPythonExec (pandas_udf SCALAR) ----

  test("wraps ArrowEvalPythonExec with TimedExec") {
    val udf = fakePythonUDF(PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val original = ArrowEvalPythonExec(Seq(udf), Seq.empty, emptyChild, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[TimedExec]
    result.asInstanceOf[TimedExec].child shouldBe a[ArrowEvalPythonExec]
  }

  test("ArrowEvalPythonExec TimedExec has duration metric") {
    val udf = fakePythonUDF(PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val original = ArrowEvalPythonExec(Seq(udf), Seq.empty, emptyChild, PythonEvalType.SQL_SCALAR_PANDAS_UDF)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[TimedExec]
    result.metrics should contain key "duration"
  }

  // ---- FlatMapGroupsInPandasExec (applyInPandas / GROUPED_MAP) ----

  test("wraps FlatMapGroupsInPandasExec with TimedExec") {
    val udf = fakePythonUDF(PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
    val original = FlatMapGroupsInPandasExec(Seq.empty, udf, Seq.empty, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[TimedExec]
    result.asInstanceOf[TimedExec].child shouldBe a[FlatMapGroupsInPandasExec]
  }

  test("FlatMapGroupsInPandasExec TimedExec has duration metric") {
    val udf = fakePythonUDF(PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF)
    val original = FlatMapGroupsInPandasExec(Seq.empty, udf, Seq.empty, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[TimedExec]
    result.metrics should contain key "duration"
  }

  // ---- BatchEvalPythonExec (regular @udf / SQL_BATCHED_UDF) ----

  test("wraps BatchEvalPythonExec with TimedExec") {
    val udf = fakePythonUDF(PythonEvalType.SQL_BATCHED_UDF)
    val original = BatchEvalPythonExec(Seq(udf), Seq.empty, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[TimedExec]
    result.asInstanceOf[TimedExec].child shouldBe a[BatchEvalPythonExec]
  }

  test("BatchEvalPythonExec TimedExec has duration metric") {
    val udf = fakePythonUDF(PythonEvalType.SQL_BATCHED_UDF)
    val original = BatchEvalPythonExec(Seq(udf), Seq.empty, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[TimedExec]
    result.metrics should contain key "duration"
  }

  // ---- FlatMapCoGroupsInPandasExec (cogroup / applyInPandas on two DataFrames) ----

  test("wraps FlatMapCoGroupsInPandasExec with TimedExec") {
    val udf = fakePythonUDF(PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF)
    val original = FlatMapCoGroupsInPandasExec(Seq.empty, Seq.empty, udf, Seq.empty, emptyChild, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original)
    result shouldBe a[TimedExec]
    result.asInstanceOf[TimedExec].child shouldBe a[FlatMapCoGroupsInPandasExec]
  }

  test("FlatMapCoGroupsInPandasExec TimedExec has duration metric") {
    val udf = fakePythonUDF(PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF)
    val original = FlatMapCoGroupsInPandasExec(Seq.empty, Seq.empty, udf, Seq.empty, emptyChild, emptyChild)
    val rule = DataFlintInstrumentationColumnarRule(spark)
    val result = rule.preColumnarTransitions(original).asInstanceOf[TimedExec]
    result.metrics should contain key "duration"
  }
}
