package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.dataflint.DataflintSparkUILoader
import org.apache.spark.status.AppHistoryServerPlugin
import org.apache.spark.util.{Clock, SystemClock, Utils}

import java.util.ServiceLoader
import scala.collection.JavaConverters._

// This class is not needed any more, as history server loading is now being done via DataFlintListenerHistoryServerPlugin
// Will be removed in the future, but because users already configured it as provider if we remove this method it will cause issues.
class FsDataflintHistoryProvider(conf: SparkConf, clock: Clock) extends FsHistoryProvider(conf, clock) {
  def this(conf: SparkConf) = {
    this(conf, new SystemClock())
  }

  override def getAppUI(appId: String, attemptId: Option[String]): Option[LoadedAppUI] = {
    super.getAppUI(appId, attemptId)
  }
}
