import { Alerts, SparkSQLStore } from "../../interfaces/AppStore";
import { humanFileSize, parseBytesString } from "../../utils/FormatUtils";

const BROADCAST_SIZE_THRESHOLD = 1 * 1024 * 1024 * 1024;

export function reduceBroadcastTooLargeAlert(sql: SparkSQLStore, alerts: Alerts) {
    sql.sqls.forEach((sql) => {
        sql.nodes.forEach((node) => {
            if (node.nodeName === "BroadcastExchange" || (node.nodeName === "Exchange" && node.parsedPlan?.type === "Exchange" && node.parsedPlan?.plan.isBroadcast)) {
                const broadcastSizeMetric = parseBytesString(
                    node.metrics.find((metric) => metric.name === "data size")?.value ?? "0",
                );

                if (broadcastSizeMetric > BROADCAST_SIZE_THRESHOLD) {
                    const broadcastSizeString = humanFileSize(broadcastSizeMetric);
                    alerts.push({
                        id: `largeBroadcast_${sql.id}_${node.nodeId}_${broadcastSizeString}`,
                        name: "largeBroadcast",
                        title: "Large data Broadcast",
                        location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
                        message: `The data broadcast size is ${broadcastSizeString}, which exceeds the 1GB threshold and can cause performance issues`,
                        suggestion: `
    1. spark.sql.autoBroadcastJoinThreshold config might be set to a large number which is not optimal
    2. The broadcast hint is applied on a large dataframe which is not optimal`,
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