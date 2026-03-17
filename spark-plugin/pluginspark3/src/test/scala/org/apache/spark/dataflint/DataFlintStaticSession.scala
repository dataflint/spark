package org.apache.spark.dataflint

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession

/**
 * Holds the active SparkSession so Python integration tests can access it via Py4J.
 *
 * Usage in Scala test:
 *   DataFlintStaticSession.set(spark)
 *   PythonRunner.main(Array(scriptPath, ""))
 *
 * Usage in Python script (after PythonRunner sets PYSPARK_GATEWAY_PORT):
 *   gateway = pyspark.java_gateway.launch_gateway()
 *   static  = gateway.jvm.org.apache.spark.dataflint.DataFlintStaticSession
 *   sc      = pyspark.SparkContext(gateway=gateway, jsc=static.javaSparkContext())
 *   spark   = SparkSession(sc, jsparkSession=static.session())
 */
object DataFlintStaticSession {
  @volatile private var _session: SparkSession = _

  def set(session: SparkSession): Unit = { _session = session }
  def clear(): Unit                    = { _session = null   }

  // Scala 2 generates static forwarders — accessible as:
  //   gateway.jvm.org.apache.spark.dataflint.DataFlintStaticSession.session()
  def session: SparkSession             = _session
  def javaSparkContext: JavaSparkContext = new JavaSparkContext(_session.sparkContext)
}