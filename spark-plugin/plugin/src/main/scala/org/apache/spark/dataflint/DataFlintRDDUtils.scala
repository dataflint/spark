package org.apache.spark.dataflint

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric

import java.util.concurrent.TimeUnit.NANOSECONDS

object DataFlintRDDUtils {
  def withDurationMetric(rdd: RDD[InternalRow], durationMetric: SQLMetric): RDD[InternalRow] =
    rdd.mapPartitions { iter =>
      val startTime = System.nanoTime()
      var done = false
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val r = iter.hasNext
          if (!r && !done) {
            durationMetric += NANOSECONDS.toMillis(System.nanoTime() - startTime)
            done = true
          }
          r
        }
        override def next(): InternalRow = iter.next()
      }
    }
}