import { Alerts, SparkSQLStore } from "../../interfaces/AppStore";
import { humanFileSize } from "../../utils/FormatUtils";

const BYTE_READ_TOO_SMALL_THRESHOLD = 3 * 1024 * 1024;
const FILES_READ_TOO_HIGH_THREASHOLD = 100;

const BYTE_WRITTEN_TOO_SMALL_THREASHOLD = 3 * 1024 * 1024;
const FILES_WRITTEN_TOO_HIGH_THREASHOLD = 100;

export function reduceSQLInputOutputAlerts(sql: SparkSQLStore, alerts: Alerts) {
  sql.sqls.forEach((sql) => {
    sql.nodes.forEach((node) => {
      const filesReadMetric = parseFloat(
        node.metrics.find((metric) => metric.name === "files read")?.value ??
          "0",
      );
      const bytesReadMetric = parseFloat(
        node.metrics.find((metric) => metric.name === "bytes read")?.value ??
          "0",
      );

      if (node.type === "input" && filesReadMetric && bytesReadMetric) {
        const avgFileSize = filesReadMetric / bytesReadMetric;
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
        node.metrics.find((metric) => metric.name === "files written")?.value ??
          "0",
      );
      const bytesWrittenMetric = parseFloat(
        node.metrics.find((metric) => metric.name === "bytes written")?.value ??
          "0",
      );

      if (node.type === "output" && fileWrittenMetric && bytesWrittenMetric) {
        const avgFileSize = fileWrittenMetric / bytesWrittenMetric;
        const avgFileSizeString = humanFileSize(avgFileSize);
        if (
          avgFileSize < BYTE_WRITTEN_TOO_SMALL_THREASHOLD &&
          fileWrittenMetric > FILES_WRITTEN_TOO_HIGH_THREASHOLD
        ) {
          alerts.push({
            id: `writeSmallFiles_${sql.id}_${node.nodeId}_${avgFileSizeString}`,
            name: "writeSmallFiles",
            title: "Writing Small Files",
            location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
            message: `The average file size is ${avgFileSizeString}, which is too small and can cause performance issues`,
            suggestion: `
    1. Choose another partition key which has lower cardinality
    2. Do a repartition by your partition key before writing the data
    3. Manually decrease the number of partitions by calling '.repartition' on your dataframe`,
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
