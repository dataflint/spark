package org.apache.spark.dataflint.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import scala.util.Try

/**
 * Walks an analyzed [[LogicalPlan]] and pulls out read/write/save targets from
 * the typed nodes (not from `simpleString` descriptions), so paths and table
 * identifiers are captured at their full length regardless of
 * SQLConf.maxToStringFields truncation.
 */
object DataflintIOExtractor extends Logging {

  /** Extract all read+write targets from an analyzed logical plan. */
  def extract(plan: LogicalPlan): Seq[DataflintIOTarget] = {
    val out = collection.mutable.ArrayBuffer.empty[DataflintIOTarget]
    Try {
      plan.foreach { node =>
        Try(extractFromNode(node)).toOption.flatten.foreach(out += _)
      }
    }.recover {
      case t: Throwable => logWarning("DataflintIOExtractor: failed to walk plan", t)
    }
    out.toSeq
  }

  private def extractFromNode(node: LogicalPlan): Option[DataflintIOTarget] = node match {

    // --- READS ---------------------------------------------------------------
    case lr: LogicalRelation =>
      val (paths, format) = lr.relation match {
        case h: HadoopFsRelation =>
          (h.location.rootPaths.map(_.toString), h.fileFormat.getClass.getSimpleName)
        case other =>
          (Seq.empty[String], other.getClass.getSimpleName)
      }
      val tableName = lr.catalogTable.map(_.identifier.unquotedString)
      Some(DataflintIOTarget(
        operation = "read",
        format = format,
        paths = paths,
        tableName = tableName,
        saveMode = None,
        partitionColumns = lr.catalogTable.map(_.partitionColumnNames).getOrElse(Seq.empty),
        options = Map.empty
      ))

    case d: DataSourceV2Relation =>
      val tableName = d.identifier.map(_.toString).orElse(Option(d.table).map(_.name))
      val format = Option(d.table).map(_.getClass.getSimpleName).getOrElse("DataSourceV2")
      val opts = safeOptionsMap(d.options.asCaseSensitiveMap())
      Some(DataflintIOTarget(
        operation = "read",
        format = format,
        paths = optionalPath(opts),
        tableName = tableName,
        saveMode = None,
        partitionColumns = Seq.empty,
        options = opts
      ))

    // --- WRITES (V1) ---------------------------------------------------------
    case c: InsertIntoHadoopFsRelationCommand =>
      Some(DataflintIOTarget(
        operation = "write",
        format = c.fileFormat.getClass.getSimpleName,
        paths = Seq(c.outputPath.toString),
        tableName = c.catalogTable.map(_.identifier.unquotedString),
        saveMode = Some(c.mode.toString),
        partitionColumns = c.partitionColumns.map(_.name),
        options = c.options
      ))

    case c: SaveIntoDataSourceCommand =>
      val tableName = c.options.get("dbtable").orElse(c.options.get("table"))
      Some(DataflintIOTarget(
        operation = "write",
        format = c.dataSource.getClass.getSimpleName,
        paths = c.options.get("path").toSeq,
        tableName = tableName,
        saveMode = Some(c.mode.toString),
        partitionColumns = Seq.empty,
        options = c.options
      ))

    case c: CreateDataSourceTableAsSelectCommand =>
      Some(DataflintIOTarget(
        operation = "write",
        format = c.table.provider.getOrElse(""),
        paths = c.table.storage.locationUri.map(_.toString).toSeq,
        tableName = Some(c.table.identifier.unquotedString),
        saveMode = Some(c.mode.toString),
        partitionColumns = c.table.partitionColumnNames,
        options = c.table.storage.properties
      ))

    // --- WRITES (V2: AppendData / OverwriteByExpression / OverwritePartitionsDynamic / ReplaceData) ---
    case w: V2WriteCommand =>
      val named = Try(w.table).toOption
      val tableName = named.flatMap(t => Option(t.name))
      val format = named.map(_.getClass.getSimpleName).getOrElse("V2Write")
      Some(DataflintIOTarget(
        operation = "write",
        format = format,
        paths = Seq.empty,
        tableName = tableName,
        saveMode = Some(w.getClass.getSimpleName),
        partitionColumns = Seq.empty,
        options = Map.empty
      ))

    case _ => None
  }

  /** Defensive conversion of a Spark CaseInsensitiveStringMap to a Scala Map. */
  private def safeOptionsMap(m: java.util.Map[String, String]): Map[String, String] = {
    Try {
      val it = m.entrySet().iterator()
      val b = Map.newBuilder[String, String]
      while (it.hasNext) { val e = it.next(); b += (e.getKey -> e.getValue) }
      b.result()
    }.getOrElse(Map.empty)
  }

  private def optionalPath(opts: Map[String, String]): Seq[String] =
    opts.get("path").orElse(opts.get("paths")).toSeq
}
