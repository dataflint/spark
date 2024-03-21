package org.apache.spark.dataflint.listener

import org.apache.spark.status.{JobDataWrapper, KVUtils}
import org.apache.spark.util.kvstore.KVStore

class DataflintStore(val store: KVStore) {
  def icebergCommits(offset: Int, length: Int): Seq[IcebergCommitInfo] = {
    KVUtils.mapToSeq(store.view(classOf[IcebergCommitWrapper]).skip(offset).max(length))(_.info).sortBy(_.executionId)
  }
}
