package org.apache.spark.dataflint

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

object DataFlintRDDUtils {
  def withDurationMetric(rdd: RDD[InternalRow], durationMetric: SQLMetric): RDD[InternalRow] =
    new RDD[InternalRow](rdd) {
      override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
        val startTime = System.nanoTime()
        val iter = firstParent[InternalRow].iterator(split, context)
        var done = false
        new Iterator[InternalRow] {
          override def hasNext: Boolean = {
            val r = iter.hasNext
            if (!r && !done) {
              durationMetric += (System.nanoTime() - startTime) / (1000 * 1000)
              done = true
            }
            r
          }
          override def next(): InternalRow = iter.next()
        }
      }
      override protected def getPartitions: Array[Partition] = firstParent.partitions
    }

  def withDurationMetricColumnar(rdd: RDD[ColumnarBatch], durationMetric: SQLMetric): RDD[ColumnarBatch] =
    new RDD[ColumnarBatch](rdd) {
      override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
        val startTime = System.nanoTime()
        val iter = firstParent[ColumnarBatch].iterator(split, context)
        var done = false
        new Iterator[ColumnarBatch] {
          override def hasNext: Boolean = {
            val r = iter.hasNext
            if (!r && !done) {
              durationMetric += (System.nanoTime() - startTime) / (1000 * 1000)
              done = true
            }
            r
          }
          override def next(): ColumnarBatch = iter.next()
        }
      }
      override protected def getPartitions: Array[Partition] = firstParent.partitions
    }
}