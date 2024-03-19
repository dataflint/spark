package org.apache.spark.dataflint.listener

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.status.ElementTrackingStore

class DataflintListener(context: SparkContext) extends SparkListener with Logging {
  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    try {
      event match {
        case icebergCommitEvent: IcebergCommitEvent => {
          logInfo(s"Received IcebergCommitEvent for execution id ${icebergCommitEvent.icebergCommit.executionId} table ${icebergCommitEvent.icebergCommit.tableName}")
          val commitInfo = new IcebergCommitWrapper(icebergCommitEvent.icebergCommit)
            val store = context.statusStore.store.asInstanceOf[ElementTrackingStore]
            store.write(commitInfo)
        }
        case _ => {}
      }
    } catch {
      case e: Exception => logError("Error while processing events in DataflintListener", e)

    }
  }
}
