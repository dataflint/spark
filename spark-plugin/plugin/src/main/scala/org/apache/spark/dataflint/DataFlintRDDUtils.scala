package org.apache.spark.dataflint

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

object DataFlintRDDUtils {
  def withDurationMetric(rdd: RDD[InternalRow], durationMetric: SQLMetric): RDD[InternalRow] =
    rdd.mapPartitions { iter =>
      val startTime = System.nanoTime()
      var done = false
      new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          val r = iter.hasNext
          if (!r && !done) {
            durationMetric += ((System.nanoTime() - startTime)/(1000 * 1000))
            done = true
          }
          r
        }
        override def next(): InternalRow = iter.next()
      }
    }

  def withDurationMetricColumnar(rdd: RDD[ColumnarBatch], durationMetric: SQLMetric): RDD[ColumnarBatch] =
    rdd.mapPartitions { iter =>
      val startTime = System.nanoTime()
      var done = false
      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = {
          val r = iter.hasNext
          if (!r && !done) {
            durationMetric += ((System.nanoTime() - startTime) / (1000 * 1000))
            done = true
          }
          r
        }
        override def next(): ColumnarBatch = iter.next()
      }
    }
}