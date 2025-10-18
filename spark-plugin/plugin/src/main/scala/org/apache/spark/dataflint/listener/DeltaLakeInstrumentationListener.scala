package org.apache.spark.dataflint.listener

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionStart, SparkPlanGraph}
import org.apache.spark.sql.SparkSession

import scala.util.Try

/**
 * Listener that instruments Delta Lake table reads to extract and log metadata
 * about partitioning, z-ordering, and liquid clustering configurations.
 *
 * Uses reflection to support both open-source Delta Lake and Databricks runtime:
 * - Open-source: org.apache.spark.sql.delta.DeltaLog
 * - Databricks: com.databricks.sql.transaction.tahoe.DeltaLog
 *
 * @param sparkContext                  The Spark context
 * @param collectZindexFields           Whether to collect z-index fields from Delta Lake history
 * @param cacheZindexFieldsToProperties Whether to cache z-index fields to table properties
 * @param historyLimit                  Maximum number of history entries to scan for z-order columns
 * @param isDatabricks                  Whether running on Databricks runtime
 */
class DeltaLakeInstrumentationListener(
                                        sparkContext: SparkContext,
                                        collectZindexFields: Boolean,
                                        cacheZindexFieldsToProperties: Boolean,
                                        historyLimit: Int,
                                        isDatabricks: Boolean
                                      ) extends SparkListener with Logging {
  logInfo(s"DeltaLakeInstrumentationListener initialized (collectZindexFields=$collectZindexFields, cacheZindexFieldsToProperties=$cacheZindexFieldsToProperties, historyLimit=$historyLimit, isDatabricks=$isDatabricks)")
  
  // Reflection utility for Delta Lake operations
  private lazy val reflectionUtils = new DeltaLakeReflectionUtils(isDatabricks)
  
  // Test reflection methods on initialization
  reflectionUtils.testReflection()
  
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
          val duration = System.currentTimeMillis() - startTime
          logInfo(s"DeltaLakeInstrumentationListener - processing for SQL id ${e.executionId} took ${duration}ms")
        case _ => // Ignore other events
      }
    } catch {
      case e: Throwable =>
        logWarning("Error while processing events in DeltaLakeInstrumentationListener", e)
    } finally {
      val duration = System.currentTimeMillis() - startTime
      logInfo(s"DeltaLakeInstrumentationListener - processing took ${duration}ms")
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
      case e: Throwable =>
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
        DeltaTablePathParser.extractTablePathFromDesc(node.desc).map { tablePath =>
          val tableNameOrPath = DeltaTablePathParser.extractTableNameFromDesc(node.desc).getOrElse(tablePath)
          (node.id, tablePath, tableNameOrPath)
        }
      } else {
        None
      }
    }.toSeq
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

    logInfo(s"DeltaLakeInstrumentationListener - Extracting metadata for table: $tablePath (tableNameOrPath: $tableNameOrPath)")
    
    val startTime = System.currentTimeMillis()
    sparkSession match {
      case Some(spark) =>
        try {
          val (partitionColumns, clusteringColumns, clusteringColumnsPhysicalNames, cachedZorderColumns) = getTableColumnsFromDetail(spark, tableNameOrPath)

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

          // Use the qualified table name if available, otherwise extract from path
          // tableNameOrPath already contains the best table name (qualified name from plan description)
          val tableName = Some(tableNameOrPath)

          // Create and post the event
          val scanInfo = DataflintDeltaLakeScanInfo(
            minExecutionId = executionId,
            tablePath = tablePath,
            tableName = tableName,
            partitionColumns = partitionColumns,
            clusteringColumns = clusteringColumns,
            clusteringColumnsPhysicalNames = clusteringColumnsPhysicalNames,
            zorderColumns = zorderColumns
          )

          logInfo(s"DeltaLakeInstrumentationListener - Posting event for $tablePath: partitionColumns=[${partitionColumns.mkString(",")}], clusteringColumns=[${clusteringColumns.mkString(",")}], clusteringColumnsPhysicalNames=[${clusteringColumnsPhysicalNames.mkString(",")}], zorderColumns=[${zorderColumns.mkString(",")}]")

          val event = DataflintDeltaLakeScanEvent(scanInfo)
          sparkContext.listenerBus.post(event)

          // Mark this table path as processed
          processedTablePaths.add(tablePath)

          val duration = System.currentTimeMillis() - startTime
          logDebug(s"Posted Delta Lake scan event for execution $executionId, table: $tablePath (took ${duration}ms)")
        }
        catch {
          case e: Throwable =>
            logWarning(s"Could not extract Delta metadata for path: $tablePath", e)
        }
      case None =>
        logWarning("SparkSession not available, cannot extract Delta metadata")
    }
  }

  /**
   * Get partition, clustering, and cached z-order columns from table metadata.
   * Returns (partitionColumns, clusteringColumns, clusteringColumnsPhysicalNames, cachedZorderColumns)
   */
  private def getTableColumnsFromDetail(spark: SparkSession, tableNameOrPath: String): (Seq[String], Seq[String], Seq[String], Option[Seq[String]]) = {
    Try {
      logInfo(s"Getting table columns for $tableNameOrPath")
      reflectionUtils.getDeltaLog(spark, tableNameOrPath) match {
        case Some(deltaLog) =>
          logInfo(s"Got DeltaLog for $tableNameOrPath")
          reflectionUtils.callUnsafeVolatileSnapshot(deltaLog) match {
            case Some(snapshot) =>
              logInfo(s"Got snapshot for $tableNameOrPath")
              reflectionUtils.callMetadata(snapshot) match {
                case Some(metadata) =>
                  logInfo(s"Got metadata for $tableNameOrPath")
                  val partitionColumns = reflectionUtils.callPartitionColumns(metadata).getOrElse(Seq.empty)
                  logInfo(s"Table $tableNameOrPath - partition columns: ${partitionColumns.mkString(", ")}")
                  
                  val (clusteringColumns, clusteringColumnsPhysicalNames) = extractClusteringColumns(snapshot)
                  logInfo(s"Table $tableNameOrPath - clustering columns: ${clusteringColumns.mkString(", ")}")
                  logInfo(s"Table $tableNameOrPath - clustering columns physical names: ${clusteringColumnsPhysicalNames.mkString(", ")}")
                  
                  // Get cached z-order fields from properties
                  val cachedZorderColumns = reflectionUtils.callConfiguration(metadata).flatMap { customProperties =>
                    logInfo(s"Table $tableNameOrPath - checking for cached z-order fields in properties")
                    customProperties.get("dataflint.zorderFields").map { cols =>
                      if (cols.trim.isEmpty) {
                        logInfo(s"Table $tableNameOrPath - found empty cached z-order fields")
                        Seq.empty
                      } else {
                        val parsed = cols.split(",").map(_.trim).filter(_.nonEmpty).toSeq
                        logInfo(s"Table $tableNameOrPath - found cached z-order fields: ${parsed.mkString(", ")}")
                        parsed
                      }
                    }
                  }
                  
                  if (cachedZorderColumns.isEmpty) {
                    logInfo(s"Table $tableNameOrPath - no cached z-order fields found in properties")
                  }
                  
                  (partitionColumns, clusteringColumns, clusteringColumnsPhysicalNames, cachedZorderColumns)
                case None =>
                  logWarning(s"Failed to get metadata for table $tableNameOrPath")
                  (Seq.empty, Seq.empty, Seq.empty, None)
              }
            case None =>
              logWarning(s"Snapshot is null for table $tableNameOrPath")
              (Seq.empty, Seq.empty, Seq.empty, None)
          }
        case None =>
          logWarning(s"DeltaLog class not available, cannot extract table columns for $tableNameOrPath")
          (Seq.empty, Seq.empty, Seq.empty, None)
      }
    }.recover {
      case e: Throwable =>
        logWarning(s"Failed to extract table columns from snapshot metadata for $tableNameOrPath: ${e.getMessage}", e)
        (Seq.empty, Seq.empty, Seq.empty, None)
    }.getOrElse((Seq.empty, Seq.empty, Seq.empty, None))
  }

  /**
   * Extract clustering columns from Delta snapshot.
   * Returns (logicalNames, physicalNames)
   */
  private def extractClusteringColumns(snapshot: Any): (Seq[String], Seq[String]) = {
    Try {
      logInfo("Attempting to extract clustering columns from snapshot")
      reflectionUtils.callClusteringMetadataFromSnapshot(snapshot) match {
        case Some(clusteringMetadataDomain) =>
          logInfo("Found clustering metadata domain")
          reflectionUtils.callClusteringColumns(clusteringMetadataDomain) match {
            case Some(clusteringColumns) if clusteringColumns.nonEmpty =>
              logInfo(s"Found ${clusteringColumns.size} physical clustering column(s): ${clusteringColumns.map(_.mkString(".")).mkString(", ")}")
              // Table has clustering columns, need schema to convert physical to logical names
              reflectionUtils.callSchema(snapshot) match {
                case Some(schema) =>
                  logInfo("Got schema for clustering column conversion")
                  val logicalNames = clusteringColumns.flatMap { physicalName =>
                    Try {
                      reflectionUtils.callClusteringColumnInfoApply(schema, physicalName).flatMap { columnInfo =>
                        reflectionUtils.callLogicalName(columnInfo)
                      }
                    }.recover {
                      case e: Throwable =>
                        logWarning(s"Failed to convert clustering physical name ${physicalName.mkString(".")} to logical name: ${e.getMessage}", e)
                        None
                    }.getOrElse(None)
                  }
                  // Convert physical names (Seq[Seq[String]]) to dot-separated strings
                  val physicalNames = clusteringColumns.map(_.mkString("."))
                  logInfo(s"Converted to ${logicalNames.size} logical clustering column(s): ${logicalNames.mkString(", ")}")
                  (logicalNames, physicalNames)
                case None =>
                  logWarning("Failed to get schema from snapshot for clustering column conversion (table has clustering but schema not available)")
                  (Seq.empty, Seq.empty)
              }
            case Some(cols) =>
              // Clustering metadata exists but no columns defined (empty list)
              logDebug(s"Clustering metadata found but column list is empty (size=${cols.size})")
              (Seq.empty, Seq.empty)
            case None =>
              // Could not get clustering columns from metadata domain
              logDebug("Could not extract clustering columns from clustering metadata domain (callClusteringColumns returned None)")
              (Seq.empty, Seq.empty)
          }
        case None =>
          logDebug("No clustering metadata domain found for this table (table doesn't support liquid clustering or clustering not configured)")
          (Seq.empty, Seq.empty)
      }
    }.recover {
      case e: Throwable =>
        logWarning(s"Failed to extract clustering columns: ${e.getMessage}", e)
        (Seq.empty, Seq.empty)
    }.getOrElse((Seq.empty, Seq.empty))
  }

  /**
   * Try to detect Z-Order columns from Delta table history
   * Uses DeltaLog.history API directly via reflection for better performance (no Spark jobs)
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
      
      val zOrderColumns = getZOrderColumnsFromHistory(spark, tableNameOrPath, historyLimit)

      spark.sparkContext.setLocalProperty("spark.sql.execution.root.id", null) // Clear local property
      spark.sparkContext.setJobDescription(null) // Clear job description

      // Cache z-order fields to table metadata (only if caching is enabled)
      // Cache even if empty so we know the table has been checked and has no z-order fields
      if (cacheZindexFieldsToProperties) {
        Try {
          val metadataValue = zOrderColumns.mkString(",")
          if (zOrderColumns.nonEmpty) {
            logInfo(s"Set dataflint.zorderFields metadata for table $tableNameOrPath: $metadataValue")
            spark.sparkContext.setJobDescription(s"DataFlint Delta Lake Instrumentation - add found z-order fields to table properties for table $tableNameOrPath")
          } else {
            logInfo(s"Set dataflint.zorderFields metadata to empty for table $tableNameOrPath (no z-order fields found)")
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
          case e: Throwable =>
            logWarning(s"Failed to set z-order metadata for table $tableNameOrPath: ${e.getMessage}", e)
        }
      } else {
        logDebug(s"Skipping caching z-order fields to properties for table $tableNameOrPath (cacheZindexFieldsToProperties=false)")
      }

      zOrderColumns
    }.recover {
      case e: Throwable =>
        logDebug(s"Unable to query Delta history: ${e.getMessage}")
        Seq.empty
    }.getOrElse(Seq.empty)
  }

  /**
   * Get z-order columns from Delta table history.
   * Returns the z-order columns from the most recent OPTIMIZE operation.
   */
  private def getZOrderColumnsFromHistory(spark: SparkSession, tableNameOrPath: String, limit: Int): Seq[String] = {
    Try {
      logInfo(s"Searching Delta history for z-order columns for table $tableNameOrPath (limit=$limit)")
      reflectionUtils.getDeltaLog(spark, tableNameOrPath) match {
        case Some(deltaLog) =>
          logInfo(s"Got DeltaLog for history search: $tableNameOrPath")
          reflectionUtils.callHistory(deltaLog) match {
            case Some(history) =>
              logInfo(s"Got history object for $tableNameOrPath")
              reflectionUtils.callGetHistory(history, Some(limit)) match {
                case Some(historySeq) =>
                  logInfo(s"Retrieved ${historySeq.size} history entries for $tableNameOrPath")
                  // Find the most recent OPTIMIZE operation
                  val optimizeOps = historySeq.filter { commit =>
                    reflectionUtils.callOperation(commit).contains("OPTIMIZE")
                  }
                  
                  logInfo(s"Found ${optimizeOps.size} OPTIMIZE operations in history for $tableNameOrPath")
                  
                  if (optimizeOps.nonEmpty) {
                    val mostRecentOptimize = optimizeOps.head
                    reflectionUtils.callOperationParameters(mostRecentOptimize) match {
                      case Some(params) =>
                        logInfo(s"Got operation parameters for most recent OPTIMIZE: ${params.keys.mkString(", ")}")
                        params.get("zOrderBy").orElse(params.get("zorder")) match {
                          case Some(zOrderColsStr) =>
                            val cleaned = zOrderColsStr.trim.stripPrefix("[").stripSuffix("]")
                            val result = cleaned.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).filter(_.nonEmpty).toSeq
                            logInfo(s"Table $tableNameOrPath - found z-order columns from history: ${result.mkString(", ")}")
                            result
                          case None =>
                            logInfo(s"Table $tableNameOrPath - OPTIMIZE operation found but no zOrderBy/zorder parameter (regular OPTIMIZE without z-order)")
                            Seq.empty
                        }
                      case None =>
                        logWarning(s"Table $tableNameOrPath - failed to get operation parameters from OPTIMIZE commit")
                        Seq.empty
                    }
                  } else {
                    logInfo(s"Table $tableNameOrPath - no OPTIMIZE operations found in last $limit history entries (table may never have been optimized with z-order)")
                    Seq.empty
                  }
                case None =>
                  logWarning(s"Table $tableNameOrPath - failed to get history sequence from history object")
                  Seq.empty
              }
            case None =>
              logWarning(s"Table $tableNameOrPath - failed to get history object from DeltaLog")
              Seq.empty
          }
        case None =>
          logWarning(s"Table $tableNameOrPath - DeltaLog class not available, cannot get z-order columns from history")
          Seq.empty
      }
    }.recover {
      case e: Throwable =>
        logWarning(s"Table $tableNameOrPath - unable to query Delta history: ${e.getMessage}", e)
        Seq.empty
    }.getOrElse(Seq.empty)
  }
}
