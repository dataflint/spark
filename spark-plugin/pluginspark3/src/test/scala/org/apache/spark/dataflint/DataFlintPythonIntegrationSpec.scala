package org.apache.spark.dataflint

import org.apache.spark.deploy.PythonRunner
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.Paths

/**
 * Integration test that runs a Python script against the instrumented SparkSession.
 *
 * PythonRunner.main() creates a Py4JServer internally and sets PYSPARK_GATEWAY_PORT
 * for the subprocess — no manual gateway setup needed. The Python script then connects
 * to this JVM via launch_gateway() and accesses the session through DataFlintStaticSession.
 */
class DataFlintPythonIntegrationSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("DataFlintPythonIntegrationSpec")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_SPARK_ENABLED,                "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_ARROW_EVAL_PYTHON_ENABLED,    "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_BATCH_EVAL_PYTHON_ENABLED,    "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_GROUPS_PANDAS_ENABLED,   "true")
      .config(DataflintSparkUICommonLoader.INSTRUMENT_FLAT_MAP_COGROUPS_PANDAS_ENABLED, "true")
      .config("spark.ui.enabled", "false")
      .withExtensions(new DataFlintInstrumentationExtension)
      .getOrCreate()

    DataFlintStaticSession.set(spark)
  }

  override def afterAll(): Unit = {
    DataFlintStaticSession.clear()
    if (spark != null) spark.stop()
  }

  test("all 4 DataFlint Python exec nodes are instrumented and visible in the plan") {
    val scriptPath = Paths.get("pyspark-testing", "dataflint_python_exec_integration_test.py")
      .toAbsolutePath.toString
    // PythonRunner sets PYSPARK_GATEWAY_PORT + PYSPARK_GATEWAY_SECRET for the subprocess,
    // wires up PYTHONPATH (pyspark + py4j), and throws SparkException on non-zero exit.
    PythonRunner.main(Array(scriptPath, ""))
  }
}