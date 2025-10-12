package org.apache.spark.dataflint.listener

import io.delta.tables.DeltaTable
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionStart, SparkPlanGraph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.skipping.clustering.{ClusteredTableUtils, ClusteringColumnInfo}
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}

import scala.util.Try

/**
 * Listener that instruments Delta Lake table reads to extract and log metadata
 * about partitioning, z-ordering, and liquid clustering configurations.
 *
 * @param sparkContext                  The Spark context
 * @param collectZindexFields           Whether to collect z-index fields from Delta Lake history (default: true)
 * @param cacheZindexFieldsToProperties Whether to cache z-index fields to table properties (default: true)
 */
class DeltaLakeInstrumentationListener(
                                        sparkContext: SparkContext,
                                        collectZindexFields: Boolean = true,
                                        cacheZindexFieldsToProperties: Boolean = true
                                      ) extends SparkListener with Logging {
  logInfo(s"DeltaLakeInstrumentationListener initialized (collectZindexFields=$collectZindexFields, cacheZindexFieldsToProperties=$cacheZindexFieldsToProperties)")
  private lazy val sparkSession: Option[SparkSession] = {
    Try(SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)).toOption.flatten
  }

  // Cache to track which table paths have been processed
  private val processedTablePaths = scala.collection.mutable.Set[String]()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    val startTime = System.currentTimeMillis()
    try {
      event match {
        case e: SparkListenerSQLExecutionStart =>
          onSQLExecutionStart(e)
        case _ => // Ignore other events
      }
    } catch {
      case e: Exception =>
        logError("Error while processing events in DeltaLakeInstrumentationListener", e)
    } finally {
      val duration = System.currentTimeMillis() - startTime
      logDebug(s"onOtherEvent processing took ${duration}ms")
    }
  }

  private def onSQLExecutionStart(event: SparkListenerSQLExecutionStart): Unit = {
    try {
      if (event.rootExecutionId.isDefined && event.rootExecutionId.get != event.executionId) {
        // This is a nested execution, skip processing the listener to improve performance and to avoid searching zorder fields while running OPTIMIZE zorder
        logDebug(s"DeltaLakeInstrumentationListener - Skipping nested SQL execution ${event.executionId}")
      } else {
        // Convert to SparkPlanGraph to get the correct node IDs
        val planGraph = SparkPlanGraph(event.sparkPlanInfo)

        // Check if this is an OptimizeTableCommand - if so, reset cache
        if (shouldResetCache(planGraph)) {
          logDebug("Detected OptimizeTableCommand - resetting table path cache")
          processedTablePaths.clear()
        }

        // Extract table paths and their corresponding node IDs directly from the graph
        val scanNodes = extractDeltaScanNodes(planGraph)

        scanNodes.foreach { case (nodeId, tablePath, tableNameOrPath) =>
          extractAndPostDeltaMetadata(tablePath, tableNameOrPath, nodeId, event.executionId)
        }
      }
    }
    catch {
      case e: Exception =>
        logWarning(s"Failed to extract Delta Lake metadata for execution ${event.executionId}", e)
    }
  }

  /**
   * Check if the plan graph contains only one node with "Execute OptimizeTableCommand"
   * If so, we should reset the cache as tables may have been optimized
   */
  private def shouldResetCache(planGraph: SparkPlanGraph): Boolean = {
    val allNodes = planGraph.allNodes.toSeq
    allNodes.length == 1 && allNodes.head.name.contains("Execute OptimizeTableCommand")
  }

  /**
   * Extract Delta Lake scan nodes from the Spark plan graph
   * Returns a sequence of (nodeId, tablePath, tableNameOrPath) tuples
   * where tableNameOrPath is the qualified table name if available, otherwise the path
   */
  private def extractDeltaScanNodes(planGraph: SparkPlanGraph): Seq[(Long, String, String)] = {
    planGraph.allNodes.flatMap { node =>
      // Look for Delta scan patterns in the node name
      if (node.name.contains("Scan") && !node.name.contains("ExistingRDD")) {
        // Extract table path and table name from node description
        // The desc field contains the full scan information
        extractTablePathFromDesc(node.desc).map { tablePath =>
          val tableNameOrPath = extractTableNameFromDesc(node.desc).getOrElse(tablePath)
          (node.id, tablePath, tableNameOrPath)
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
        // also check that is not truncated, i.e. ends with ...
        if ((content.contains(":") || content.startsWith("/")) && (!content.endsWith("..."))) {
          Some(content)
        } else {
          None
        }
      }.toSeq.headOption

      pathFromBracket
    }.toOption.flatten
  }

  /**
   * Extract table name from node description
   * Example: "FileScan parquet dataflint_user_simulator.test.zorder_table[category#850,value#851]..."
   * Returns: Some("dataflint_user_simulator.test.zorder_table")
   */
  private def extractTableNameFromDesc(desc: String): Option[String] = {
    Try {
      // Pattern to match table name after scan type and before the column list [
      // Handles formats like:
      // - "FileScan parquet catalog.schema.table[columns]"
      // - "Scan parquet catalog.schema.table[columns]"
      // Table names can contain dots (for catalog.schema.table), alphanumerics, and underscores
      val tableNamePattern = """(?:FileScan|Scan)\s+\w+\s+([\w.]+)\[""".r
      tableNamePattern.findFirstMatchIn(desc).map(_.group(1))
    }.toOption.flatten
  }

  /**
   * Extract Delta Lake metadata and post event for a given table path and node ID
   */
  private def extractAndPostDeltaMetadata(tablePath: String, tableNameOrPath: String, nodeId: Long, executionId: Long): Unit = {
    // Check if we've already processed this table path
    if (processedTablePaths.contains(tablePath)) {
      logDebug(s"Skipping already processed table path: $tablePath")
      return
    }

    val startTime = System.currentTimeMillis()
    sparkSession match {
      case Some(spark) =>
        try {
          val (partitionColumns, clusteringColumns, cachedZorderColumns) = getTableColumnsFromDetail(spark, tableNameOrPath)

          // Determine z-order columns based on cache and configuration
          // Only collect z-order columns if collectZindexFields is enabled
          val zorderColumns = if (!collectZindexFields) {
            Seq.empty
          } else {
            cachedZorderColumns match {
              case Some(cached) =>
                // Cache exists (either with values or empty)
                if (cached.nonEmpty) {
                  logDebug(s"Using cached z-order columns from metadata for table $tablePath: ${cached.mkString(", ")}")
                } else {
                  logDebug(s"Using cached z-order columns from metadata for table $tablePath: none (table has no z-order)")
                }
                cached
              case None =>
                // No cache exists, need to query history
                logDebug(s"No cached z-order columns found for table $tablePath, querying Delta Lake history")
                getZOrderColumns(spark, tableNameOrPath, executionId)
            }
          }

          // Extract table name from path (last component after /)
          val tableName = tablePath.split("/").lastOption.filter(_.nonEmpty)

          // Create and post the event
          val scanInfo = DataflintDeltaLakeScanInfo(
            minExecutionId = executionId,
            tablePath = tablePath,
            tableName = tableName,
            partitionColumns = partitionColumns,
            clusteringColumns = clusteringColumns,
            zorderColumns = zorderColumns
          )

          val event = DataflintDeltaLakeScanEvent(scanInfo)
          sparkContext.listenerBus.post(event)

          // Mark this table path as processed
          processedTablePaths.add(tablePath)

          val duration = System.currentTimeMillis() - startTime
          logDebug(s"Posted Delta Lake scan event for execution $executionId, table: $tablePath (took ${duration}ms)")
        }
        catch {
          case e: Exception =>
            logWarning(s"Could not extract Delta metadata for path: $tablePath - ${e.getMessage}", e)
        }
      case None =>
        logDebug("SparkSession not available, cannot extract Delta metadata")
    }
  }

  def extractLogicalNames(snapshot: Snapshot): Seq[String] = {
    if (ClusteredTableUtils.isSupported(snapshot.protocol)) {
      ClusteredTableUtils.getClusteringColumnsOptional(snapshot).map { clusteringColumns =>
        clusteringColumns.map(ClusteringColumnInfo(snapshot.schema, _).logicalName)
      }.getOrElse(Seq.empty)
    } else {
      Seq.empty
    }
  }

  /**
   * Extract partition, clustering, and z-order columns from Delta table metadata (unified method)
   * Uses DeltaLog.unsafeVolatileSnapshot for better performance (no Spark job required)
   * Returns (partitionColumns, clusteringColumns, cachedZorderColumns)
   * Note: cachedZorderColumns returns Option[Seq[String]] where:
   *   - None means not cached yet (need to query history)
   *   - Some(Seq.empty) means cached as empty (table has no z-order fields)
   *   - Some(Seq(...)) means cached with values
   *
   * @param spark           The SparkSession
   * @param tableNameOrPath The table name (e.g., "catalog.schema.table") or file path
   */
  private def getTableColumnsFromDetail(spark: SparkSession, tableNameOrPath: String): (Seq[String], Seq[String], Option[Seq[String]]) = {
    Try {
      // Use unsafeVolatileSnapshot for better performance (no Spark job needed)
      // assume DeltaLog is already initialized as the scan step must have read the snapshot
      val snapshot = DeltaLog.forTable(spark, tableNameOrPath).unsafeVolatileSnapshot

      // Check if snapshot is null
      if (snapshot == null) {
        logWarning(s"Snapshot is null for table $tableNameOrPath")
        return (Seq.empty, Seq.empty, None)
      }

      val metadata = snapshot.metadata

      // Get partition columns from metadata
      val partitionColumns = metadata.partitionColumns

      // Get properties map from metadata (contains clustering and z-order info)
      val customProperties = metadata.configuration

      val clusteringColumns = extractLogicalNames(snapshot)

      // Check for z-order fields in properties (cached by Dataflint)
      // Return None if property doesn't exist, Some(Seq) if it does (even if empty)
      val cachedZorderColumns = customProperties.get("dataflint.zorderFields").map { cols =>
        // Format: "col1,col2" or empty string
        if (cols.trim.isEmpty) {
          Seq.empty
        } else {
          cols.split(",").map(_.trim).filter(_.nonEmpty).toSeq
        }
      }

      (partitionColumns, clusteringColumns, cachedZorderColumns)
    }.recover {
      case e: Exception =>
        logWarning(s"Failed to extract table columns from snapshot metadata: ${e.getMessage}", e)
        (Seq.empty, Seq.empty, None)
    }.getOrElse((Seq.empty, Seq.empty, None))
  }

  /**
   * Try to detect Z-Order columns from Delta table history
   * Uses DeltaLog.history API directly for better performance (no Spark jobs)
   * Always persist the result as table metadata (even if empty) to avoid re-scanning history
   *
   * @param spark           The SparkSession
   * @param tableNameOrPath The table name (e.g., "catalog.schema.table") or file path
   * @param rootExecutionId The parent execution ID to set as root for nested queries
   */
  private def getZOrderColumns(spark: SparkSession, tableNameOrPath: String, rootExecutionId: Long): Seq[String] = {
    Try {
      // Set the root execution ID so any Spark SQL operations appear as nested executions
      spark.sparkContext.setLocalProperty("spark.sql.execution.root.id", rootExecutionId.toString)
      spark.sparkContext.setJobDescription(s"DataFlint Delta Lake Instrumentation - search operations history to find Z-Order fields for table $tableNameOrPath")

      // Use DeltaLog history API directly (no Spark job needed)
      // Get last 1000 operations, assume if OPTIMIZE was not done recently, table is not z-ordered
      val deltaLog = DeltaLog.forTable(spark, tableNameOrPath)
      val history = deltaLog.history.getHistory(Some(1000))

      spark.sparkContext.setLocalProperty("spark.sql.execution.root.id", null) // Clear local property
      spark.sparkContext.setJobDescription(null) // Clear job description

      // Find the most recent OPTIMIZE operation
      val optimizeOps = history.filter(_.operation == "OPTIMIZE")

      val zOrderColumns = if (optimizeOps.nonEmpty) {
        val mostRecentOptimize = optimizeOps.head // History is already sorted by version desc
        val params = mostRecentOptimize.operationParameters

        params.get("zOrderBy").orElse(params.get("zorder")) match {
          case Some(zOrderColsStr) =>
            // Parse the columns string (format: "[col1,col2]" or "col1,col2")
            // Also remove quotes around column names
            val cleaned = zOrderColsStr.trim.stripPrefix("[").stripSuffix("]")
            cleaned.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).filter(_.nonEmpty).toSeq
          case None =>
            Seq.empty
        }
      } else {
        Seq.empty
      }

      // Cache z-order fields to table metadata (only if caching is enabled)
      // Cache even if empty so we know the table has been checked and has no z-order fields
      if (cacheZindexFieldsToProperties) {
        Try {
          val metadataValue = zOrderColumns.mkString(",")
          if (zOrderColumns.nonEmpty) {
            logDebug(s"Set dataflint.zorderFields metadata for table $tableNameOrPath: $metadataValue")
            spark.sparkContext.setJobDescription(s"DataFlint Delta Lake Instrumentation - add found z-order fields to table properties for table $tableNameOrPath")
          } else {
            logDebug(s"Set dataflint.zorderFields metadata to empty for table $tableNameOrPath (no z-order fields found)")
            spark.sparkContext.setJobDescription(s"DataFlint Delta Lake Instrumentation - add no z-order fields to table properties for table $tableNameOrPath")
          }

          spark.sparkContext.setLocalProperty("spark.sql.execution.root.id", rootExecutionId.toString)
          // Determine if tableNameOrPath is a qualified table name or a path
          val alterTableCmd = if (tableNameOrPath.contains("/") || tableNameOrPath.contains(":\\") || tableNameOrPath.contains("s3://") || tableNameOrPath.contains("dbfs:/")) {
            // It's a path, use delta.`path` syntax
            s"ALTER TABLE delta.`$tableNameOrPath` SET TBLPROPERTIES ('dataflint.zorderFields' = '$metadataValue')"
          } else {
            // It's a qualified table name, use it directly
            s"ALTER TABLE $tableNameOrPath SET TBLPROPERTIES ('dataflint.zorderFields' = '$metadataValue')"
          }
          spark.sql(alterTableCmd)
          spark.sparkContext.setJobDescription(null) // Clear job description
          spark.sparkContext.setLocalProperty("spark.sql.execution.root.id", null) // Clear local property
        }.recover {
          case e: Exception =>
            logWarning(s"Failed to set z-order metadata for table $tableNameOrPath: ${e.getMessage}", e)
        }
      } else {
        logDebug(s"Skipping caching z-order fields to properties for table $tableNameOrPath (cacheZindexFieldsToProperties=false)")
      }

      zOrderColumns
    }.recover {
      case e: Exception =>
        logDebug(s"Unable to query Delta history: ${e.getMessage}")
        Seq.empty
    }.getOrElse(Seq.empty)
  }
}
