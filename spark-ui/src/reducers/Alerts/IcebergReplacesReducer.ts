import { Alerts, SparkSQLStore } from "../../interfaces/AppStore";
import { calculatePercentage, humanFileSize } from "../../utils/FormatUtils";

const REPLACED_MOST_OF_TABLE_PERCENTAGE_THRESHOLD = 60;
const REPLACED_MORE_FILES_THAN_RECORDS_PERCENTAGE_THRESHOLD = 30;

const BYTE_WRITTEN_TOO_SMALL_THREASHOLD = 3 * 1024 * 1024;
const FILES_WRITTEN_TOO_HIGH_THREASHOLD = 100;

export function reduceIcebergReplaces(sql: SparkSQLStore, alerts: Alerts) {
  sql.sqls.forEach((sql) => {
    sql.nodes.forEach((node) => {
      if (node.icebergCommit === undefined) {
        return;
      }
      const metrics = node.icebergCommit.metrics;

      if (metrics.addedFilesSizeInBytes && metrics.addedDataFiles) {
        const avgFileSize =
          metrics.addedFilesSizeInBytes / metrics.addedDataFiles;
        const avgFileSizeString = humanFileSize(avgFileSize);
        if (
          avgFileSize < BYTE_WRITTEN_TOO_SMALL_THREASHOLD &&
          metrics.addedDataFiles > FILES_WRITTEN_TOO_HIGH_THREASHOLD
        ) {
          alerts.push({
            id: `writeSmallFiles_${sql.id}_${node.nodeId}_${avgFileSizeString}`,
            name: `writeSmallFiles`,
            title: `Writing Small Files To Iceberg Table`,
            location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
            message: `The average file size is ${avgFileSizeString}, which is too small and can cause performance issues`,
            suggestion: `
          1. Configure iceberg table distribution mode (write.distribution-mode), see https://iceberg.apache.org/docs/latest/spark-writes/#writing-distribution-modes
          2. Write your job in bigger batches so you will write mode data per-batch
          3. If your table is partitioned, consider coosing a partitioning columns with lower cardinality
          `,
            type: "warning",
            source: {
              type: "sql",
              sqlId: sql.id,
              sqlNodeId: node.nodeId,
            },
          });
        }
      }
      if (node.nodeName === "ReplaceData") {
        const metrics = node.icebergCommit.metrics;
        const previousSnapshotTotal =
          metrics.totalRecords - metrics.addedRecords + metrics.removedRecords;
        const tableChangedPercentage = calculatePercentage(
          metrics.removedDataFiles,
          metrics.totalDataFiles,
        );
        const recordsChangedPercentage =
          metrics.removedRecords === metrics.totalRecords
            ? calculatePercentage(metrics.removedRecords, metrics.totalRecords)
            : calculatePercentage(
                Math.abs(metrics.addedRecords - metrics.removedRecords),
                metrics.totalRecords,
              );
        const isMergeChangedLessThanExisting =
          metrics.addedRecords + metrics.removedRecords <=
          previousSnapshotTotal * 2; // if 2X the records was added/removed, it might mean the merge is justified because we don't know how many were kept
        if (
          tableChangedPercentage >
            REPLACED_MORE_FILES_THAN_RECORDS_PERCENTAGE_THRESHOLD &&
          recordsChangedPercentage <
            REPLACED_MORE_FILES_THAN_RECORDS_PERCENTAGE_THRESHOLD &&
          isMergeChangedLessThanExisting
        ) {
          alerts.push({
            id: `inefficientIcebergReplaceTable_${sql.id}_${node.nodeId}`,
            name: "inefficientIcebergReplaceTable",
            title: "Inefficient Replace Of Data In Iceberg Table",
            location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
            message: `${tableChangedPercentage.toFixed(1)}% of table ${
              node.icebergCommit.tableName
            } files were replaced, while only ${recordsChangedPercentage.toFixed(
              1,
            )}% of records were changed`,
            suggestion: `
    1. Switch write mode merge-on-read mode, so instead of re-writing the entire file, only the changed records will be written
    2. Partition the table in such a way that usage of update/merge/delete operation to update only the required partitions
                        `,
            type: "warning",
            source: {
              type: "sql",
              sqlId: sql.id,
              sqlNodeId: node.nodeId,
            },
          });
        } else if (
          tableChangedPercentage > REPLACED_MOST_OF_TABLE_PERCENTAGE_THRESHOLD
        ) {
          alerts.push({
            id: `replacedMostOfIcebergTable_${sql.id}_${node.nodeId}`,
            name: "replacedMostOfIcebergTable",
            title: "Replaced Most Of Iceberg Table",
            location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
            message: `${tableChangedPercentage.toFixed(1)}% of table ${
              node.icebergCommit.tableName
            } files were replaced, which is potential mis-use of iceberg`,
            suggestion: `
    1. Partition the table in such a way that usage of update/merge/delete operation to update as little files as possible
                        `,
            type: "warning",
            source: {
              type: "sql",
              sqlId: sql.id,
              sqlNodeId: node.nodeId,
            },
          });
        }
      }
    });
  });
}
