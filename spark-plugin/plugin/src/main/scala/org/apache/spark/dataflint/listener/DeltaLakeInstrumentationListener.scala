package org.apache.spark.dataflint.listener

import io.delta.tables.DeltaTable
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionStart, SparkPlanGraph}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * Listener that instruments Delta Lake table reads to extract and log metadata
 * about partitioning, z-ordering, and liquid clustering configurations.
 */
class DeltaLakeInstrumentationListener(sparkContext: SparkContext) extends SparkListener with Logging {
  logInfo("DeltaLakeInstrumentationListener initialized")
  private lazy val sparkSession: Option[SparkSession] = {
    Try(SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)).toOption.flatten
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    try {
      event match {
        case e: SparkListenerSQLExecutionStart =>
          onSQLExecutionStart(e)
        case _ => // Ignore other events
      }
    } catch {
      case e: Exception =>
        logError("Error while processing events in DeltaLakeInstrumentationListener", e)
    }
  }

  private def onSQLExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    try {
      // Convert to SparkPlanGraph to get the correct node IDs
      val planGraph = SparkPlanGraph(event.sparkPlanInfo)
      
      // Extract table paths and their corresponding node IDs directly from the graph
      val scanNodes = extractDeltaScanNodes(planGraph)
      
      scanNodes.foreach { case (nodeId, tablePath) =>
        extractAndPostDeltaMetadata(tablePath, nodeId, event.executionId)
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to extract Delta Lake metadata for execution ${event.executionId}", e)
    }
  }

  /**
   * Extract Delta Lake scan nodes from the Spark plan graph
   * Returns a sequence of (nodeId, tablePath) tuples
   */
  private def extractDeltaScanNodes(planGraph: SparkPlanGraph): Seq[(Long, String)] = {
    planGraph.allNodes.flatMap { node =>
      // Look for Delta scan patterns in the node name
      if (node.name.contains("Scan") && !node.name.contains("ExistingRDD")) {
        // Extract table path from node description
        // The desc field contains the full scan information
        extractTablePathFromDesc(node.desc).map { tablePath =>
          (node.id, tablePath)
        }
      } else {
        None
      }
    }.toSeq
  }

  /**
   * Extract table path from node description
   */
  private def extractTablePathFromDesc(desc: String): Option[String] = {
    Try {
      // Skip if it's a delta log scan
      if (desc.contains("_delta_log")) {
        return None
      }
      
      // Extract path from various formats:
      // - "InMemoryFileIndex[file:/tmp/table]"
      // - "TahoeBatchFileIndex(1 paths)[file:/tmp/table]"
      // - "TahoeBatchFileIndex(1 paths)[s3://bucket/table]"
      // - "TahoeBatchFileIndex(1 paths)[dbfs:/mnt/table]"
      val bracketPattern = """\[([^\]]+)\]""".r
      val pathFromBracket = bracketPattern.findAllMatchIn(desc).flatMap { m =>
        val content = m.group(1)
        // Check if this bracket content looks like a path (with or without scheme)
        // Keep the full URI including scheme (file://, s3://, dbfs://, etc.)
        if (content.contains(":") || content.startsWith("/")) {
          Some(content)
        } else {
          None
        }
      }.toSeq.headOption
      
      pathFromBracket
    }.toOption.flatten
  }

  /**
   * Extract Delta Lake metadata and post event for a given table path and node ID
   */
  private def extractAndPostDeltaMetadata(tablePath: String, nodeId: Long, executionId: Long): Unit = {
    sparkSession match {
      case Some(spark) =>
        try {
          loadDeltaTable(spark, tablePath) match {
            case Some(deltaTable) =>
              val partitionColumns = getPartitionColumns(deltaTable)
              val clusteringColumns = getClusteringColumns(deltaTable)
              val zorderColumns = getZOrderColumns(spark, tablePath)
              
              // Extract table name from path (last component after /)
              val tableName = tablePath.split("/").lastOption.filter(_.nonEmpty)
              
              // Create and post the event
              val scanInfo = DataflintDeltaLakeScanInfo(
                executionId = executionId,
                nodeId = nodeId,
                tablePath = tablePath,
                tableName = tableName,
                partitionColumns = partitionColumns,
                clusteringColumns = clusteringColumns,
                zorderColumns = zorderColumns
              )
              
              val event = DataflintDeltaLakeScanEvent(scanInfo)
              sparkContext.listenerBus.post(event)
              
              logDebug(s"Posted Delta Lake scan event for execution $executionId, node $nodeId, table: $tablePath")
            case None =>
              logDebug(s"Could not load Delta table for path: $tablePath")
          }
        } catch {
          case e: Exception =>
            logWarning(s"Could not extract Delta metadata for path: $tablePath - ${e.getMessage}", e)
        }
      case None =>
        logDebug("SparkSession not available, cannot extract Delta metadata")
    }
  }

  /**
   * Load Delta table using Delta Lake API
   */
  private def loadDeltaTable(spark: SparkSession, tablePath: String): Option[DeltaTable] = {
    Try {
      DeltaTable.forPath(spark, tablePath)
    }.toOption
  }

  /**
   * Extract partition columns from Delta table
   */
  private def getPartitionColumns(deltaTable: DeltaTable): Seq[String] = {
    Try {
      val detailDF = deltaTable.detail()
      val rows = detailDF.collect()
      
      if (rows.nonEmpty) {
        val row = rows(0)
        
        // Get partition columns field
        Try {
          val partCols = row.getAs[Seq[String]]("partitionColumns")
          partCols
        }.getOrElse(Seq.empty)
      } else {
        Seq.empty
      }
    }.recover {
      case e: Exception =>
        logWarning(s"Failed to extract partition columns: ${e.getMessage}", e)
        Seq.empty
    }.getOrElse(Seq.empty)
  }

  /**
   * Extract liquid clustering columns from Delta table properties
   */
  private def getClusteringColumns(deltaTable: DeltaTable): Seq[String] = {
    Try {
      val detailDF = deltaTable.detail()
      val rows = detailDF.collect()
      
      if (rows.nonEmpty) {
        val row = rows(0)
        
        // Try to get clusteringColumns directly from the row (newer Delta versions)
        val directClustering = Try {
          row.getAs[Seq[String]]("clusteringColumns")
        }.toOption.filter(_.nonEmpty)
        
        if (directClustering.isDefined) {
          return directClustering.get
        }
        
        // Fall back to checking properties map
        val properties = Try {
          row.getAs[Map[String, String]]("properties")
        }.toOption.getOrElse(Map.empty)
        
        // Look for clustering column information in properties
        // Delta Lake stores liquid clustering info in table properties
        val clusteringCols = properties.get("delta.clustering.columns")
          .orElse(properties.get("delta.clusteringColumns"))
          .orElse(properties.get("clusteringColumns"))
        
        clusteringCols.map { cols =>
          // Handle both "col1,col2" and "[col1,col2]" formats
          val cleaned = cols.trim.stripPrefix("[").stripSuffix("]")
          cleaned.split(",").map(_.trim).filter(_.nonEmpty).toSeq
        }.getOrElse(Seq.empty)
      } else {
        Seq.empty
      }
    }.recover {
      case e: Exception =>
        logWarning(s"Failed to extract clustering columns: ${e.getMessage}", e)
        Seq.empty
    }.getOrElse(Seq.empty)
  }

  /**
   * Try to detect Z-Order columns from Delta table history
   */
  private def getZOrderColumns(spark: SparkSession, tablePath: String): Seq[String] = {
    Try {
      // Query Delta history for OPTIMIZE operations with ZORDER
      val historyDF = spark.sql(s"DESCRIBE HISTORY delta.`$tablePath`")
      val rows = historyDF.collect()
      
      // Look for the most recent OPTIMIZE operation with zOrderBy info
      val zOrderOperations = rows.flatMap { row =>
        val result = Try {
          val operation = row.getAs[String]("operation")
          if (operation != null && operation.toUpperCase.contains("OPTIMIZE")) {
            // Try to get operationParameters
            val params = Try {
              row.getAs[Map[String, String]]("operationParameters")
            }.toOption.getOrElse(Map.empty)
            
            params.get("zOrderBy").orElse(params.get("zorder"))
          } else {
            None
          }
        }.toOption
        
        result match {
          case Some(Some(value)) => Some(value)
          case _ => None
        }
      }
      
      if (zOrderOperations.nonEmpty) {
        val zOrderColsStr = zOrderOperations.head
        // Parse the columns string (format: "[col1,col2]" or "col1,col2")
        // Also remove quotes around column names
        val cleaned = zOrderColsStr.trim.stripPrefix("[").stripSuffix("]")
        cleaned.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).filter(_.nonEmpty).toSeq
      } else {
        Seq.empty
      }
    }.recover {
      case e: Exception =>
        logDebug(s"Unable to query Delta history: ${e.getMessage}")
        Seq.empty
    }.getOrElse(Seq.empty)
  }
}
