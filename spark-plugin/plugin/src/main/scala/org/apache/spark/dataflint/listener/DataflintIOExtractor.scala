package org.apache.spark.dataflint.listener

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V2WriteCommand}
import org.apache.spark.sql.execution.{FileSourceScanExec, QueryExecution, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableAsSelectCommand, DataWritingCommandExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InsertIntoHadoopFsRelationCommand, LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation}

import scala.util.Try

/**
 * Pulls IO targets (reads/writes/saves) out of a QueryExecution by reading
 * typed plan fields — not regex over `simpleString` — so paths and identifiers
 * are captured at full length regardless of SQLConf.maxToStringFields truncation.
 *
 * Primary walk is over `qe.executedPlan` with a pre-order index assigned to each
 * node; that index matches the nodeId Spark's [[org.apache.spark.sql.execution.ui.SparkPlanGraph]]
 * assigns, so consumers can correlate each target back to the SparkUI SQL tab.
 * For [[DataWritingCommandExec]] we delegate to the logical-plan matcher on the
 * wrapped `cmd` and overlay the physical nodeId.
 */
object DataflintIOExtractor extends Logging {

  /** Walk the physical plan, attaching the pre-order nodeId to every captured target. */
  def extract(qe: QueryExecution): Seq[DataflintIOTarget] = {
    val out = collection.mutable.ArrayBuffer.empty[DataflintIOTarget]
    Try {
      var idx = 0
      qe.executedPlan.foreach { node =>
        Try(extractFromPhysical(node, idx)).toOption.flatten.foreach(out += _)
        idx += 1
      }
    }.recover {
      case t: Throwable => logWarning("DataflintIOExtractor: failed to walk plan", t)
    }
    out.toSeq
  }

  private def extractFromPhysical(node: SparkPlan, nodeId: Int): Option[DataflintIOTarget] = node match {

    // --- PHYSICAL READS ------------------------------------------------------
    case fsse: FileSourceScanExec =>
      Some(DataflintIOTarget(
        nodeId = nodeId,
        operation = "read",
        format = fsse.relation.fileFormat.getClass.getSimpleName,
        paths = fsse.relation.location.rootPaths.map(_.toString),
        tableName = fsse.tableIdentifier.map(_.unquotedString),
        saveMode = None,
        partitionColumns = fsse.relation.partitionSchema.fieldNames.toSeq,
        options = fsse.relation.options
      ))

    case bse: BatchScanExec =>
      val tableName = Option(bse.table).map(_.name)
      Some(DataflintIOTarget(
        nodeId = nodeId,
        operation = "read",
        format = Option(bse.table).map(_.getClass.getSimpleName).getOrElse("DataSourceV2"),
        paths = Seq.empty,
        tableName = tableName,
        saveMode = None,
        partitionColumns = Seq.empty,
        options = Map.empty
      ))

    case rdse: RowDataSourceScanExec =>
      Some(DataflintIOTarget(
        nodeId = nodeId,
        operation = "read",
        format = Try(rdse.relation.getClass.getSimpleName).getOrElse("RowDataSource"),
        paths = Seq.empty,
        tableName = Try(rdse.tableIdentifier.map(_.unquotedString)).getOrElse(None),
        saveMode = None,
        partitionColumns = Seq.empty,
        options = Map.empty
      ))

    // --- PHYSICAL WRITES (V1) — delegate to logical matcher, overlay nodeId --
    case dwce: DataWritingCommandExec =>
      extractFromLogical(dwce.cmd).map(_.copy(nodeId = nodeId))

    case _ => None
  }

  /**
   * Logical-plan matcher for the commands [[DataWritingCommandExec]] wraps.
   * Returns nodeId = -1; the physical-plan caller overlays the real id.
   */
  private def extractFromLogical(node: LogicalPlan): Option[DataflintIOTarget] = node match {

    case lr: LogicalRelation =>
      val (paths, format) = lr.relation match {
        case h: HadoopFsRelation =>
          (h.location.rootPaths.map(_.toString), h.fileFormat.getClass.getSimpleName)
        case other =>
          (Seq.empty[String], other.getClass.getSimpleName)
      }
      Some(DataflintIOTarget(
        nodeId = -1,
        operation = "read",
        format = format,
        paths = paths,
        tableName = lr.catalogTable.map(_.identifier.unquotedString),
        saveMode = None,
        partitionColumns = lr.catalogTable.map(_.partitionColumnNames).getOrElse(Seq.empty),
        options = Map.empty
      ))

    case d: DataSourceV2Relation =>
      val opts = safeOptionsMap(d.options.asCaseSensitiveMap())
      Some(DataflintIOTarget(
        nodeId = -1,
        operation = "read",
        format = Option(d.table).map(_.getClass.getSimpleName).getOrElse("DataSourceV2"),
        paths = optionalPath(opts),
        tableName = d.identifier.map(_.toString).orElse(Option(d.table).map(_.name)),
        saveMode = None,
        partitionColumns = Seq.empty,
        options = opts
      ))

    case c: InsertIntoHadoopFsRelationCommand =>
      Some(DataflintIOTarget(
        nodeId = -1,
        operation = "write",
        format = c.fileFormat.getClass.getSimpleName,
        paths = Seq(c.outputPath.toString),
        tableName = c.catalogTable.map(_.identifier.unquotedString),
        saveMode = Some(c.mode.toString),
        partitionColumns = c.partitionColumns.map(_.name),
        options = c.options
      ))

    case c: SaveIntoDataSourceCommand =>
      Some(DataflintIOTarget(
        nodeId = -1,
        operation = "write",
        format = c.dataSource.getClass.getSimpleName,
        paths = c.options.get("path").toSeq,
        tableName = c.options.get("dbtable").orElse(c.options.get("table")),
        saveMode = Some(c.mode.toString),
        partitionColumns = Seq.empty,
        options = c.options
      ))

    case c: CreateDataSourceTableAsSelectCommand =>
      Some(DataflintIOTarget(
        nodeId = -1,
        operation = "write",
        format = c.table.provider.getOrElse(""),
        paths = c.table.storage.locationUri.map(_.toString).toSeq,
        tableName = Some(c.table.identifier.unquotedString),
        saveMode = Some(c.mode.toString),
        partitionColumns = c.table.partitionColumnNames,
        options = c.table.storage.properties
      ))

    case w: V2WriteCommand =>
      val named = Try(w.table).toOption
      Some(DataflintIOTarget(
        nodeId = -1,
        operation = "write",
        format = named.map(_.getClass.getSimpleName).getOrElse("V2Write"),
        paths = Seq.empty,
        tableName = named.flatMap(t => Option(t.name)),
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
