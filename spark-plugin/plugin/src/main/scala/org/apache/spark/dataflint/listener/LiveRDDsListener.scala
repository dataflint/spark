package org.apache.spark.status

import org.apache.spark.dataflint.listener.{DataflintExecutorStorageInfo, DataflintRDDStorageInfo, DataflintRDDStorageInfoWrapper}

import scala.collection.mutable.HashMap
import org.apache.spark.scheduler._
import org.apache.spark.storage._
import org.apache.spark.internal.Logging

private[spark] class LiveRDDsListener(store: ElementTrackingStore) extends SparkListener with Logging {

  private val liveRDDs = new HashMap[Int, LiveRDD]()

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    event.stageInfo.rddInfos.foreach { info =>
      if (info.storageLevel.isValid) {
        liveRDDs.getOrElseUpdate(info.id, new LiveRDD(info, info.storageLevel))
      }
    }
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    liveRDDs.get(event.rddId).foreach { rdd => {
      val dist = rdd.getDistributions().values
      val maxExecutorData = if(dist.isEmpty) None else Some(dist.maxBy(_.memoryUsed).toApi())
      val maxExecutorInfo = maxExecutorData.map(executor => DataflintExecutorStorageInfo(executor.memoryUsed, executor.memoryRemaining, executor.memoryUsed.toDouble / (executor.memoryUsed + executor.memoryRemaining) * 100))
      store.write(new DataflintRDDStorageInfoWrapper(DataflintRDDStorageInfo(rdd.info.id, rdd.memoryUsed, rdd.diskUsed, rdd.info.numPartitions, rdd.info.storageLevel.description, maxExecutorInfo)))
    }}
    liveRDDs.remove(event.rddId)
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    event.blockUpdatedInfo.blockId match {
      case block: RDDBlockId =>
//        val executorId = event.blockUpdatedInfo.blockManagerId.executorId
        val storageLevel = event.blockUpdatedInfo.storageLevel
        val diskDelta = event.blockUpdatedInfo.diskSize * (if (storageLevel.useDisk) 1 else -1)
        val memoryDelta = event.blockUpdatedInfo.memSize * (if (storageLevel.useMemory) 1 else -1)

        liveRDDs.get(block.rddId).foreach { rdd =>
          // UNDO comments when adding partition-level metrics
//          val partition = rdd.partition(block.name)
//          val executors = if (storageLevel.isValid) {
//            val current = partition.executors
//            if (!current.contains(executorId)) current :+ executorId else current
//          } else {
//            partition.executors.filter(_ != executorId)
//          }
//
//          if (executors.nonEmpty) {
//            partition.update(executors,
//              math.max(0, partition.memoryUsed + memoryDelta),
//              math.max(0, partition.diskUsed + diskDelta))
//          } else {
//            rdd.removePartition(block.name)
//          }

          rdd.memoryUsed = math.max(0, rdd.memoryUsed + memoryDelta)
          rdd.diskUsed = math.max(0, rdd.diskUsed + diskDelta)
        }

      case _ => // Ignore other block types
    }
  }
}
