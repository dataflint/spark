package org.apache.spark.dataflint.listener

import scala.collection.JavaConverters._
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVStore, KVStoreView}


class DataflintStore(val store: KVStore) {
  // mapToSeq copied from KVUtils because it does not exists in spark 3.3
  def mapToSeq[T, B](view: KVStoreView[T])(mapFunc: T => B): Seq[B] = {
    Utils.tryWithResource(view.closeableIterator()) { iter =>
      iter.asScala.map(mapFunc).toList
    }
  }

  def icebergCommits(offset: Int, length: Int): Seq[IcebergCommitInfo] = {
    mapToSeq(store.view(classOf[IcebergCommitWrapper]))(_.info).filter(_.executionId >= offset).take(length).sortBy(_.executionId)
  }

  def databricksAdditionalExecutionInfo(offset: Int, length: Int): Seq[DatabricksAdditionalExecutionInfo] = {
    mapToSeq(store.view(classOf[DatabricksAdditionalExecutionWrapper]))(_.info).filter(_.executionId >= offset).take(length).sortBy(_.executionId)
  }

  def environmentInfo(): Option[DataflintEnvironmentInfo] = {
    mapToSeq(store.view(classOf[DataflintEnvironmentInfoWrapper]))(_.info).headOption
  }

  def rddStorageInfo(): Seq[DataflintRDDStorageInfo] = {
    mapToSeq(store.view(classOf[DataflintRDDStorageInfoWrapper]))(_.info)
  }

}
