package org.apache.spark.dataflint.listener

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ElementTrackingStore

class DataflintListener(store: ElementTrackingStore) extends SparkListener with Logging {

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    try {
      event match {
        case icebergCommitEvent: IcebergCommitEvent => {
          val commitInfo = new IcebergCommitWrapper(icebergCommitEvent.icebergCommit)
            store.write(commitInfo)
        }
        case e: DatabricksAdditionalExecutionEvent => {
          val executionInfo = new DatabricksAdditionalExecutionWrapper(e.databricksAdditionalExecutionInfo)
          store.write(executionInfo)
        }
        case e: DataflintEnvironmentInfoEvent => {
          val wrapper = new DataflintEnvironmentInfoWrapper(e.environmentInfo)
          store.write(wrapper)
        }
        case _ => {}
      }
    } catch {
      case e: Exception => logError("Error while processing events in DataflintListener", e)
    }
  }
}
