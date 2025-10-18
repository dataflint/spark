package org.apache.spark.dataflint.listener

import scala.util.Try

/**
 * Utility object for parsing Delta table paths and names from Spark plan descriptions.
 */
object DeltaTablePathParser {
  
  /**
   * Extract table path from node description.
   * 
   * Looks for paths in brackets like:
   * - "InMemoryFileIndex[file:/tmp/table]"
   * - "TahoeBatchFileIndex(1 paths)[file:/tmp/table]"
   * - "PreparedDeltaFileIndex(1 paths)[s3://bucket/path]"
   * 
   * @param desc The plan description string
   * @return Some(path) if found, None otherwise
   */
  def extractTablePathFromDesc(desc: String): Option[String] = {
    Try {
      // Skip if it's a delta log scan
      if (desc.contains("_delta_log")) {
        return None
      }

      // Extract path from various formats
      // Look for patterns like: Location: PreparedDeltaFileIndex(1 paths)[s3://...]
      // or TahoeBatchFileIndex(1 paths)[file:/tmp/table]
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
   * Extract table name from node description.
   * 
   * Examples:
   * - "FileScan parquet dataflint_user_simulator.test.zorder_table[category#850,value#851]..."
   *   Returns: Some("dataflint_user_simulator.test.zorder_table")
   * - "Scan parquet my_catalog.my_schema.my_table[id#123]"
   *   Returns: Some("my_catalog.my_schema.my_table")
   * 
   * The table name appears after the scan type (e.g., "parquet") and before the column list [...]
   * 
   * @param desc The plan description string
   * @return Some(tableName) if found, None otherwise
   */
  def extractTableNameFromDesc(desc: String): Option[String] = {
    Try {
      // Pattern to match table name after scan type and before the column list [
      // Handles formats like:
      // - "FileScan parquet catalog.schema.table[columns]"
      // - "Scan parquet catalog.schema.table[columns]"
      // Table names can contain dots (for catalog.schema.ta]ble), alphanumerics, and underscores
      val tableNamePattern = """(?:FileScan|Scan)\s+\w+\s+([\w.]+)\[""".r
      tableNamePattern.findFirstMatchIn(desc).map(_.group(1))
    }.toOption.flatten
  }

  /**
   * Extract table name from path by taking the last component.
   * 
   * Examples:
   * - "s3://bucket/path/to/table" -> Some("table")
   * - "file:/tmp/my_table" -> Some("my_table")
   * - "/local/path/table_name" -> Some("table_name")
   * - "s3://bucket/unity-catalog/.../88e720e8-f105-4e21-86f1-80331268c834" -> Some("88e720e8-f105-4e21-86f1-80331268c834")
   * - "/path/to/table/" -> None (trailing slash)
   * 
   * @param path The table path
   * @return Some(tableName) if the path is non-empty and has a last component, None otherwise
   */
  def extractTableNameFromPath(path: String): Option[String] = {
    // Remove trailing slash if present, then split and get last component
    val normalizedPath = path.stripSuffix("/")
    if (normalizedPath.isEmpty) {
      None
    } else {
      normalizedPath.split("/").lastOption.filter(_.nonEmpty)
    }
  }
}

