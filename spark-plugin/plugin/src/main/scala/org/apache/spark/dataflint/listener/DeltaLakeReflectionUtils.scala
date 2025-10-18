package org.apache.spark.dataflint.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Utility class for Delta Lake reflection operations.
 * Provides cached reflection methods and utility wrappers to support both open-source
 * Delta Lake and Databricks runtime without compile-time dependencies.
 *
 * @param isDatabricks Whether running on Databricks runtime (default: false)
 */
class DeltaLakeReflectionUtils(isDatabricks: Boolean = false) extends Logging {
  
  // ========== Class Caching ==========
  
  private lazy val deltaLogClass: Option[Class[_]] = try {
    val className = if (isDatabricks) {
      "com.databricks.sql.transaction.tahoe.DeltaLog"
    } else {
      "org.apache.spark.sql.delta.DeltaLog"
    }
    Some(Class.forName(className))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to load DeltaLog class: ${e.getMessage}", e)
      None
  }
  
  private lazy val snapshotClass: Option[Class[_]] = try {
    val className = if (isDatabricks) {
      "com.databricks.sql.transaction.tahoe.Snapshot"
    } else {
      "org.apache.spark.sql.delta.Snapshot"
    }
    Some(Class.forName(className))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to load Snapshot class: ${e.getMessage}", e)
      None
  }
  
  private lazy val deltaTableClass: Option[Class[_]] = if (isDatabricks) {
    try {
      Some(Class.forName("io.delta.tables.DeltaTable"))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to load DeltaTable class: ${e.getMessage}", e)
        None
    }
  } else {
    None
  }
  
  private lazy val structTypeClass: Option[Class[_]] = try {
    Some(Class.forName("org.apache.spark.sql.types.StructType"))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to load StructType class: ${e.getMessage}", e)
      None
  }
  
  // ========== Module Caching ==========
  
  private lazy val clusteringMetadataDomainModule: Option[Any] = try {
    val className = if (isDatabricks) {
      "com.databricks.sql.transaction.tahoe.clustering.ClusteringMetadataDomain$"
    } else {
      "org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain$"
    }
    val clazz = Class.forName(className)
    val moduleField = clazz.getField("MODULE$")
    Some(moduleField.get(null))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to load ClusteringMetadataDomain module: ${e.getMessage}", e)
      None
  }
  
  private lazy val clusteringColumnInfoModule: Option[Any] = try {
    val className = if (isDatabricks) {
      "com.databricks.sql.io.skipping.liquid.ClusteringColumnInfo$"
    } else {
      "org.apache.spark.sql.delta.skipping.clustering.ClusteringColumnInfo$"
    }
    val clazz = Class.forName(className)
    val moduleField = clazz.getField("MODULE$")
    Some(moduleField.get(null))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to load ClusteringColumnInfo module: ${e.getMessage}", e)
      None
  }
  
  // ========== Method Caching ==========
  
  private lazy val deltaTableForNameMethod: Option[java.lang.reflect.Method] = if (isDatabricks) {
    try {
      deltaTableClass.map(c => c.getMethod("forName", classOf[SparkSession], classOf[String]))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to get DeltaTable.forName method: ${e.getMessage}", e)
        None
    }
  } else {
    None
  }
  
  private lazy val deltaLogForTableMethod: Option[java.lang.reflect.Method] = if (!isDatabricks) {
    try {
      deltaLogClass.map(c => c.getMethod("forTable", classOf[SparkSession], classOf[String]))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to get DeltaLog.forTable method: ${e.getMessage}", e)
        None
    }
  } else {
    None
  }
  
  private lazy val deltaTableDeltaLogMethod: Option[java.lang.reflect.Method] = if (isDatabricks) {
    try {
      deltaTableClass.map(c => c.getMethod("deltaLog"))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to get DeltaTable.deltaLog method: ${e.getMessage}", e)
        None
    }
  } else {
    None
  }
  
  private lazy val clusteringMetadataFromSnapshotMethod: Option[java.lang.reflect.Method] = try {
    (clusteringMetadataDomainModule, snapshotClass) match {
      case (Some(module), Some(sc)) => Some(module.getClass.getMethod("fromSnapshot", sc))
      case _ => None
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get ClusteringMetadataDomain.fromSnapshot method: ${e.getMessage}", e)
      None
  }
  
  private lazy val unsafeVolatileSnapshotMethod: Option[java.lang.reflect.Method] = try {
    deltaLogClass.map(c => c.getMethod("unsafeVolatileSnapshot"))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get DeltaLog.unsafeVolatileSnapshot method: ${e.getMessage}", e)
      None
  }
  
  private lazy val snapshotMetadataMethod: Option[java.lang.reflect.Method] = try {
    snapshotClass.map(c => c.getMethod("metadata"))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get Snapshot.metadata method: ${e.getMessage}", e)
      None
  }
  
  private lazy val snapshotSchemaMethod: Option[java.lang.reflect.Method] = try {
    snapshotClass.map(c => c.getMethod("schema"))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get Snapshot.schema method: ${e.getMessage}", e)
      None
  }
  
  private lazy val metadataPartitionColumnsMethod: Option[java.lang.reflect.Method] = try {
    snapshotClass.map { sc =>
      val metadataClass = sc.getMethod("metadata").getReturnType
      metadataClass.getMethod("partitionColumns")
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get Metadata.partitionColumns method: ${e.getMessage}", e)
      None
  }
  
  private lazy val metadataConfigurationMethod: Option[java.lang.reflect.Method] = try {
    snapshotClass.map { sc =>
      val metadataClass = sc.getMethod("metadata").getReturnType
      metadataClass.getMethod("configuration")
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get Metadata.configuration method: ${e.getMessage}", e)
      None
  }
  
  private lazy val clusteringMetadataClusteringColumnsMethod: Option[java.lang.reflect.Method] = try {
    (clusteringMetadataDomainModule, clusteringMetadataFromSnapshotMethod) match {
      case (Some(module), Some(fromSnapshotMethod)) =>
        // Get the ClusteringMetadataDomain class by removing the '$' suffix from module class name
        val moduleClassName = module.getClass.getName.stripSuffix("$")
        val clusteringMetadataDomainClass: Option[Class[_]] = try {
          Some(Class.forName(moduleClassName))
        } catch {
          case _: Throwable => None
        }
        clusteringMetadataDomainClass.map(clazz => clazz.getMethod("clusteringColumns"))
      case _ => None
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get ClusteringMetadataDomain.clusteringColumns method: ${e.getMessage}", e)
      None
  }
  
  private lazy val clusteringColumnInfoApplyMethod: Option[java.lang.reflect.Method] = try {
    (clusteringColumnInfoModule, structTypeClass) match {
      case (Some(module), Some(structType)) =>
        Some(module.getClass.getMethod("apply", structType, classOf[Seq[_]]))
      case _ => None
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get ClusteringColumnInfo.apply method: ${e.getMessage}", e)
      None
  }
  
  private lazy val clusteringColumnInfoLogicalNameMethod: Option[java.lang.reflect.Method] = try {
    clusteringColumnInfoModule.flatMap { module =>
      // Get the return type of apply method to find ClusteringColumnInfo class
      val applyMethod = module.getClass.getMethods.find(_.getName == "apply")
      applyMethod.map { m =>
        val returnType = m.getReturnType
        returnType.getMethod("logicalName")
      }
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get ClusteringColumnInfo.logicalName method: ${e.getMessage}", e)
      None
  }
  
  private lazy val deltaLogHistoryMethod: Option[java.lang.reflect.Method] = try {
    deltaLogClass.map(c => c.getMethod("history"))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get DeltaLog.history method: ${e.getMessage}", e)
      None
  }
  
  private lazy val historyGetHistoryMethod: Option[java.lang.reflect.Method] = try {
    deltaLogClass.map { dlc =>
      val historyClass = dlc.getMethod("history").getReturnType
      historyClass.getMethod("getHistory", classOf[Option[_]])
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get DeltaHistory.getHistory method: ${e.getMessage}", e)
      None
  }
  
  private lazy val commitClass: Option[Class[_]] = try {
    deltaLogClass.flatMap { dlc =>
      // Get the commit class from history.getHistory return type
      val historyClass = dlc.getMethod("history").getReturnType
      val getHistoryMethod = historyClass.getMethod("getHistory", classOf[Option[_]])
      // The return type is Seq[Commit], we need to find the element type
      // We can do this by looking at the actual history implementation
      // Try multiple possible class names - DeltaHistory is the actual class returned
      val possibleClassNames = if (isDatabricks) {
        Seq(
          "com.databricks.sql.transaction.tahoe.DeltaHistory",
          "com.databricks.sql.transaction.tahoe.commands.history.Commit",
          "com.databricks.sql.transaction.tahoe.history.Commit"
        )
      } else {
        Seq(
          "org.apache.spark.sql.delta.DeltaHistory",
          "org.apache.spark.sql.delta.commands.history.Commit",
          "org.apache.spark.sql.delta.actions.CommitInfo"
        )
      }
      
      possibleClassNames.iterator.map { className =>
        try {
          Some(Class.forName(className))
        } catch {
          case _: ClassNotFoundException => None
        }
      }.collectFirst { case Some(clazz) => clazz }
    }
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to load Commit class: ${e.getMessage}", e)
      None
  }
  
  private lazy val commitOperationMethod: Option[java.lang.reflect.Method] = try {
    commitClass.map(clazz => clazz.getMethod("operation"))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get Commit.operation method: ${e.getMessage}", e)
      None
  }
  
  private lazy val commitOperationParametersMethod: Option[java.lang.reflect.Method] = try {
    commitClass.map(clazz => clazz.getMethod("operationParameters"))
  } catch {
    case e: Throwable =>
      logWarning(s"Failed to get Commit.operationParameters method: ${e.getMessage}", e)
      None
  }
  
  // ========== Utility Methods ==========
  
  /**
   * Get DeltaLog instance using reflection.
   * For Databricks: Uses DeltaTable.forName(spark, tableNameOrPath).deltaLog
   * For open-source: Uses DeltaLog.forTable(spark, tableNameOrPath)
   */
  def getDeltaLog(spark: SparkSession, tableNameOrPath: String): Option[Any] = {
    try {
      if (isDatabricks) {
        deltaTableForNameMethod match {
          case Some(forNameMethod) =>
            val deltaTable = forNameMethod.invoke(null, spark, tableNameOrPath)
            deltaTableDeltaLogMethod match {
              case Some(deltaLogMethod) =>
                Some(deltaLogMethod.invoke(deltaTable))
              case None =>
                logWarning(s"DeltaTable.deltaLog method not available for Databricks")
                None
            }
          case None =>
            logWarning(s"DeltaTable.forName method not available for Databricks")
            None
        }
      } else {
        deltaLogForTableMethod match {
          case Some(forTableMethod) =>
            Some(forTableMethod.invoke(null, spark, tableNameOrPath))
          case None =>
            logWarning(s"DeltaLog.forTable method not available")
            None
        }
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to get DeltaLog for table $tableNameOrPath: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call deltaLog.unsafeVolatileSnapshot()
   */
  def callUnsafeVolatileSnapshot(deltaLog: Any): Option[Any] = {
    try {
      unsafeVolatileSnapshotMethod.map(method => method.invoke(deltaLog))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call unsafeVolatileSnapshot: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call snapshot.metadata()
   */
  def callMetadata(snapshot: Any): Option[Any] = {
    try {
      snapshotMetadataMethod.map(method => method.invoke(snapshot))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call metadata: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call snapshot.schema()
   */
  def callSchema(snapshot: Any): Option[Any] = {
    try {
      snapshotSchemaMethod.map(method => method.invoke(snapshot))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call schema: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call metadata.partitionColumns()
   */
  def callPartitionColumns(metadata: Any): Option[Seq[String]] = {
    try {
      metadataPartitionColumnsMethod.map(method => method.invoke(metadata).asInstanceOf[Seq[String]])
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call partitionColumns: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call metadata.configuration()
   */
  def callConfiguration(metadata: Any): Option[scala.collection.Map[String, String]] = {
    try {
      metadataConfigurationMethod.map(method => method.invoke(metadata).asInstanceOf[scala.collection.Map[String, String]])
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call configuration: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call ClusteringMetadataDomain.fromSnapshot(snapshot)
   */
  def callClusteringMetadataFromSnapshot(snapshot: Any): Option[Any] = {
    try {
      (clusteringMetadataDomainModule, clusteringMetadataFromSnapshotMethod) match {
        case (Some(module), Some(method)) =>
          method.invoke(module, snapshot.asInstanceOf[Object]).asInstanceOf[Option[Any]]
        case _ =>
          logWarning("ClusteringMetadataDomain or fromSnapshot method not available (normal for Delta Lake < 3.1.0)")
          None
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call clusteringMetadataFromSnapshot: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call clusteringMetadataDomain.clusteringColumns()
   */
  def callClusteringColumns(clusteringMetadataDomain: Any): Option[Seq[Seq[String]]] = {
    try {
      clusteringMetadataClusteringColumnsMethod.map(method => method.invoke(clusteringMetadataDomain).asInstanceOf[Seq[Seq[String]]])
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call clusteringColumns: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call ClusteringColumnInfo.apply(schema, physicalName)
   */
  def callClusteringColumnInfoApply(schema: Any, physicalName: Seq[String]): Option[Any] = {
    try {
      (clusteringColumnInfoModule, clusteringColumnInfoApplyMethod) match {
        case (Some(module), Some(method)) =>
          Some(method.invoke(module, schema.asInstanceOf[Object], physicalName.asInstanceOf[Object]))
        case _ => None
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call clusteringColumnInfoApply: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call clusteringColumnInfo.logicalName()
   */
  def callLogicalName(clusteringColumnInfo: Any): Option[String] = {
    try {
      clusteringColumnInfoLogicalNameMethod.map(method => method.invoke(clusteringColumnInfo).asInstanceOf[String])
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call logicalName: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call deltaLog.history()
   */
  def callHistory(deltaLog: Any): Option[Any] = {
    try {
      deltaLogHistoryMethod.map(method => method.invoke(deltaLog))
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call history: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call history.getHistory(limit)
   */
  def callGetHistory(history: Any, limit: Option[Int]): Option[Seq[Any]] = {
    try {
      historyGetHistoryMethod.map(method => method.invoke(history, limit).asInstanceOf[Seq[Any]])
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call getHistory: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call commit.operation()
   */
  def callOperation(commit: Any): Option[String] = {
    try {
      commitOperationMethod.map { method =>
        // Check if the commit object is the right type for this method
        val commitClassName = commit.getClass.getName
        val expectedClassName = method.getDeclaringClass.getName
        if (!method.getDeclaringClass.isInstance(commit)) {
          logWarning(s"Commit object type mismatch: expected $expectedClassName but got $commitClassName")
          // Try to find the operation method on the actual commit class
          val actualMethod = commit.getClass.getMethod("operation")
          actualMethod.invoke(commit).asInstanceOf[String]
        } else {
          method.invoke(commit).asInstanceOf[String]
        }
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call operation on commit of type ${commit.getClass.getName}: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Call commit.operationParameters()
   */
  def callOperationParameters(commit: Any): Option[scala.collection.Map[String, String]] = {
    try {
      commitOperationParametersMethod.map { method =>
        // Check if the commit object is the right type for this method
        if (!method.getDeclaringClass.isInstance(commit)) {
          logWarning(s"Commit object type mismatch for operationParameters: expected ${method.getDeclaringClass.getName} but got ${commit.getClass.getName}")
          // Try to find the operationParameters method on the actual commit class
          val actualMethod = commit.getClass.getMethod("operationParameters")
          actualMethod.invoke(commit).asInstanceOf[scala.collection.Map[String, String]]
        } else {
          method.invoke(commit).asInstanceOf[scala.collection.Map[String, String]]
        }
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Failed to call operationParameters on commit of type ${commit.getClass.getName}: ${e.getMessage}", e)
        None
    }
  }
  
  /**
   * Test all reflection methods to ensure they were successfully loaded.
   * Logs warnings for any methods that failed to load.
   * 
   * @return true if all required methods were loaded successfully, false otherwise
   */
  def testReflection(): Boolean = {
    logInfo("Testing Delta Lake reflection methods...")
    
    var allSuccess = true
    
    // Test classes
    if (deltaLogClass.isEmpty) {
      logWarning("DeltaLog class not loaded")
      allSuccess = false
    }
    if (snapshotClass.isEmpty) {
      logWarning("Snapshot class not loaded")
      allSuccess = false
    }
    if (structTypeClass.isEmpty) {
      logWarning("StructType class not loaded")
      allSuccess = false
    }
    if (isDatabricks && deltaTableClass.isEmpty) {
      logWarning("DeltaTable class not loaded (Databricks mode)")
      allSuccess = false
    }
    if (commitClass.isEmpty) {
      logWarning("DeltaHistory/Commit class not loaded")
      allSuccess = false
    }
    
    // Test modules (optional for older Delta versions)
    if (clusteringMetadataDomainModule.isEmpty) {
      logInfo("ClusteringMetadataDomain module not loaded (normal for Delta Lake < 3.1.0)")
    }
    if (clusteringColumnInfoModule.isEmpty) {
      logInfo("ClusteringColumnInfo module not loaded (normal for Delta Lake < 3.1.0)")
    }
    
    // Test core methods
    if (isDatabricks) {
      if (deltaTableForNameMethod.isEmpty) {
        logWarning("DeltaTable.forName method not loaded")
        allSuccess = false
      }
      if (deltaTableDeltaLogMethod.isEmpty) {
        logWarning("DeltaTable.deltaLog method not loaded")
        allSuccess = false
      }
    } else {
      if (deltaLogForTableMethod.isEmpty) {
        logWarning("DeltaLog.forTable method not loaded")
        allSuccess = false
      }
    }
    
    if (unsafeVolatileSnapshotMethod.isEmpty) {
      logWarning("DeltaLog.unsafeVolatileSnapshot method not loaded")
      allSuccess = false
    }
    if (snapshotMetadataMethod.isEmpty) {
      logWarning("Snapshot.metadata method not loaded")
      allSuccess = false
    }
    if (snapshotSchemaMethod.isEmpty) {
      logWarning("Snapshot.schema method not loaded")
      allSuccess = false
    }
    if (metadataPartitionColumnsMethod.isEmpty) {
      logWarning("Metadata.partitionColumns method not loaded")
      allSuccess = false
    }
    if (metadataConfigurationMethod.isEmpty) {
      logWarning("Metadata.configuration method not loaded")
      allSuccess = false
    }
    
    // Test clustering methods (optional)
    if (clusteringMetadataFromSnapshotMethod.isEmpty) {
      logInfo("ClusteringMetadataDomain.fromSnapshot method not loaded (normal for Delta Lake < 3.1.0)")
    }
    if (clusteringMetadataClusteringColumnsMethod.isEmpty) {
      logInfo("ClusteringMetadataDomain.clusteringColumns method not loaded (normal for Delta Lake < 3.1.0)")
    }
    if (clusteringColumnInfoApplyMethod.isEmpty) {
      logInfo("ClusteringColumnInfo.apply method not loaded (normal for Delta Lake < 3.1.0)")
    }
    if (clusteringColumnInfoLogicalNameMethod.isEmpty) {
      logInfo("ClusteringColumnInfo.logicalName method not loaded (normal for Delta Lake < 3.1.0)")
    }
    
    // Test history methods
    if (deltaLogHistoryMethod.isEmpty) {
      logWarning("DeltaLog.history method not loaded")
      allSuccess = false
    }
    if (historyGetHistoryMethod.isEmpty) {
      logWarning("DeltaHistory.getHistory method not loaded")
      allSuccess = false
    }
    if (commitOperationMethod.isEmpty) {
      logWarning("DeltaHistory/Commit.operation method not loaded")
      allSuccess = false
    }
    if (commitOperationParametersMethod.isEmpty) {
      logWarning("DeltaHistory/Commit.operationParameters method not loaded")
      allSuccess = false
    }
    
    if (allSuccess) {
      logInfo("All required Delta Lake reflection methods loaded successfully")
    } else {
      logWarning("Some required Delta Lake reflection methods failed to load")
    }
    
    allSuccess
  }
}
