import { Alerts, SparkSQLStore } from "../../interfaces/AppStore";
import { humanFileSize, parseBytesString } from "../../utils/FormatUtils";

const BYTE_READ_TOO_SMALL_THRESHOLD = 3 * 1024 * 1024;
const FILES_READ_TOO_HIGH_THREASHOLD = 100;

const BYTE_WRITTEN_TOO_SMALL_THREASHOLD = 3 * 1024 * 1024;
const FILES_WRITTEN_TOO_HIGH_THREASHOLD = 100;

const IDEAL_FILE_SIZE = 128 * 1024 * 1024;

const BROADCAST_SIZE_THRESHOLD = 1 * 1024 * 1024 * 1024;

export function reduceSQLInputOutputAlerts(sql: SparkSQLStore, alerts: Alerts) {
  sql.sqls.forEach((sql) => {
    sql.nodes.forEach((node) => {
      const filesReadMetric = parseFloat(
        node.metrics
          .find((metric) => metric.name === "files read")
          ?.value?.replaceAll(",", "") ?? "0",
      );
      const bytesReadMetric = parseBytesString(
        node.metrics.find((metric) => metric.name === "bytes read")?.value ??
        "0",
      );

      if (node.type === "input" && filesReadMetric && bytesReadMetric) {
        const avgFileSize = bytesReadMetric / filesReadMetric;
        const avgFileSizeString = humanFileSize(avgFileSize);
        if (
          avgFileSize < BYTE_READ_TOO_SMALL_THRESHOLD &&
          filesReadMetric > FILES_READ_TOO_HIGH_THREASHOLD
        ) {
          alerts.push({
            id: `readSmallFiles_${sql.id}_${node.nodeId}_${avgFileSizeString}`,
            name: "readSmallFiles",
            title: "Reading Small Files",
            location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
            message: `The average file size is ${avgFileSizeString}, which is too small and can cause performance issues`,
            suggestion: `
    1. Ask the data owner to increase the file size
    2. Decrease number of executors, so each executor will read more small files and reduce the resource overhead
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

      const fileWrittenMetric = parseFloat(
        node.metrics
          .find((metric) => metric.name === "files written")
          ?.value?.replaceAll(",", "") ?? "0",
      );
      const bytesWrittenMetric = parseBytesString(
        node.metrics.find((metric) => metric.name === "bytes written")?.value ??
        "0",
      );

      if (
        node.type === "output" &&
        node.parsedPlan?.type === "WriteToHDFS" &&
        fileWrittenMetric &&
        bytesWrittenMetric
      ) {
        const avgFileSize = bytesWrittenMetric / fileWrittenMetric;
        const avgFileSizeString = humanFileSize(avgFileSize);
        if (
          avgFileSize < BYTE_WRITTEN_TOO_SMALL_THREASHOLD &&
          fileWrittenMetric > FILES_WRITTEN_TOO_HIGH_THREASHOLD
        ) {
          const isPartitioned =
            node.parsedPlan.plan.partitionKeys !== undefined;
          if (
            isPartitioned &&
            node.parsedPlan.plan.partitionKeys !== undefined
          ) {
            const partitionsWritten = parseFloat(
              node.metrics
                .find((metric) => metric.name === "partitions written")
                ?.value?.replaceAll(",", "") ?? "0",
            );
            const expectedAvgFileSize =
              partitionsWritten !== 0
                ? humanFileSize(bytesWrittenMetric / partitionsWritten)
                : 0;
            const filesPerPartition =
              partitionsWritten !== 0
                ? (fileWrittenMetric / partitionsWritten).toFixed(1)
                : 0;
            const partitionKeysText = node.parsedPlan.plan.partitionKeys
              .map((key) => `"${key}"`)
              .join(",");
            const partitionKeysStringParam = node.parsedPlan.plan.partitionKeys
              .map((key) => `"${key}"`)
              .join(",");
            alerts.push({
              id: `writeSmallFiles_${sql.id}_${node.nodeId}_${avgFileSizeString}`,
              name: `writeSmallFilesPartitioned`,
              title: `Writing Small Partitioned Files`,
              location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
              message: `The avg files written for each partition is ${filesPerPartition}, and the avg file size is ${avgFileSizeString}, which is too small and can cause performance issues`,
              suggestion: `
        1. Do a repartition by your partition key before writing the data, by running .repartition(${partitionKeysStringParam}) before writing the dataframe. Avg file size after this change should be ${expectedAvgFileSize}
        2. Choose different partition key${partitionKeysText.length === 1 ? "" : "s"
                } instead of ${partitionKeysText} with lower cardinality`,
              type: "warning",
              source: {
                type: "sql",
                sqlId: sql.id,
                sqlNodeId: node.nodeId,
              },
            });
          } else {
            const idealFileCount = Math.ceil(
              bytesWrittenMetric / IDEAL_FILE_SIZE,
            );
            const fileSizeAfterRepartition = humanFileSize(
              bytesWrittenMetric / idealFileCount,
            );
            alerts.push({
              id: `writeSmallFiles_${sql.id}_${node.nodeId}_${avgFileSizeString}`,
              name: `writeSmallUnpartitionedFiles`,
              title: `Writing Small Unpartitioned Files`,
              location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
              message: `The average file size is ${avgFileSizeString}, which is too small and can cause performance issues`,
              suggestion: `
        1. Do a repartition before writing the data, by running .repartition(${idealFileCount}). After repartition avg file size should be ${fileSizeAfterRepartition}`,
              type: "warning",
              source: {
                type: "sql",
                sqlId: sql.id,
                sqlNodeId: node.nodeId,
              },
            });
          }
        }
      }
    });
  });
}