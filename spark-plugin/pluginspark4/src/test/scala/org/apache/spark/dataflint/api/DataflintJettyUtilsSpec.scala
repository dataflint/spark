package org.apache.spark.dataflint.api

import java.net.HttpURLConnection
import java.net.URI

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Regression test for Spark 4.2 Jetty 12 (ee10) static-handler support.
 *
 * Without the ee10 reflective lookup / baseResource init-param fix,
 * DataflintJettyUtils.createStaticHandler throws ClassNotFoundException and the
 * DataFlint UI returns HTTP 500.
 */
class DataflintJettyUtilsSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("DataflintJettyUtilsSpec")
      .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
      .config("spark.dataflint.telemetry.enabled", "false")
      .config("spark.ui.enabled", "true")
      .config("spark.ui.port", "0")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  test("Spark 4.2 classpath exposes Jetty 12 ee10 servlet classes") {
    noException should be thrownBy {
      Class.forName("org.sparkproject.jetty.ee10.servlet.ServletContextHandler")
      Class.forName("org.sparkproject.jetty.ee10.servlet.DefaultServlet")
      Class.forName("org.sparkproject.jetty.ee10.servlet.ServletHolder")
    }
  }

  test("DataFlint static UI handler serves index.html over Jetty 12 ee10") {
    val ui = spark.sparkContext.ui.getOrElse(
      fail("Spark UI was not started; cannot verify static handler")
    )
    val url = s"${ui.webUrl.stripSuffix("/")}/dataflint/index.html"
    val connection = URI.create(url).toURL.openConnection().asInstanceOf[HttpURLConnection]
    try {
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(10000)
      connection.setReadTimeout(10000)
      connection.getResponseCode shouldBe 200
    } finally {
      connection.disconnect()
    }
  }
}
