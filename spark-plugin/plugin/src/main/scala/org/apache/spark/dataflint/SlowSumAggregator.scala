package org.apache.spark.dataflint

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

/**
 * A test UDAF that sums Doubles but sleeps a configurable amount per row.
 * Used in the pyspark examples to demonstrate JVM-side slow operations
 * (vs. Python UDFs which sleep in a Python subprocess).
 *
 * Sleep duration is configurable via `spark.dataflint.test.slowSumSleepMs`
 * (default 1ms per row).
 */
class SlowSumAggregator extends Aggregator[Double, Double, Double] {

  private lazy val sleepMs: Long = {
    try {
      val sc = org.apache.spark.SparkContext.getActive
      sc.map(_.conf.getLong("spark.dataflint.test.slowSumSleepMs", 1L)).getOrElse(1L)
    } catch {
      case _: Throwable => 1L
    }
  }

  override def zero: Double = 0.0

  override def reduce(buffer: Double, value: Double): Double = {
    if (sleepMs > 0) {
      try { Thread.sleep(sleepMs) } catch { case _: InterruptedException => }
    }
    buffer + value
  }

  override def merge(b1: Double, b2: Double): Double = b1 + b2

  override def finish(reduction: Double): Double = reduction

  override def bufferEncoder: Encoder[Double] = Encoders.scalaDouble

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}