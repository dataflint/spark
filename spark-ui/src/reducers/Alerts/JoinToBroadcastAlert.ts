import { Alerts, SparkSQLStore } from "../../interfaces/AppStore";
import { humanFileSize } from "../../utils/FormatUtils";
import { findLastInputNodeSize, findTwoNodes, generateGraph } from "../PlanGraphUtils";

export function reduceJoinToBroadcastAlert(sql: SparkSQLStore, alerts: Alerts) {
    sql.sqls.forEach((sql) => {
        const graph = generateGraph(sql.edges, sql.nodes);
        sql.nodes.forEach((node) => {
            if (node.nodeName === "SortMergeJoin") {
                const inputNodes = findTwoNodes(node, graph, sql.nodes);
                if (!inputNodes) {
                    return null;
                }
                const inputSizeBytes = inputNodes.map(inputNode => {
                    const inputSize = findLastInputNodeSize(inputNode, graph, sql.nodes);
                    if (inputSize === null) {
                        return null;
                    }
                    return inputSize
                }).filter(size => size !== null);
                if (inputSizeBytes.length !== 2) {
                    return null;
                }

                const input1 = inputSizeBytes[0];
                const input2 = inputSizeBytes[1]

                if (input1 === null || input2 === null) {
                    return null;
                }

                const smallTable = Math.min(input1, input2);
                const largeTable = Math.max(input1, input2);

                if (
                    // if small table is less than 10MiB (shouldn't happen unless AQE is disabled)
                    (smallTable < (10 * 1024 * 1024)) ||
                    // if small table is less than 100MiB and large table is more than 10GiB
                    (smallTable < (100 * 1024 * 1024) && largeTable > (10 * 1024 * 1024 * 1024)) ||
                    // if small table is less than 1GiB and large table is more than 300GiB
                    (smallTable < (1000 * 1024 * 1024) && largeTable > (300 * 1024 * 1024 * 1024)) ||
                    // if small table is less than 5GiB and large table is more than 1TiB
                    (smallTable < (5 * 1024 * 1024 * 1024) && largeTable > (1024 * 1024 * 1024 * 1024))
                ) {
                    alerts.push({
                        id: `broadcastSmallTable_${sql.id}_${node.nodeId}_${smallTable}_${largeTable}`,
                        name: "broadcastSmallTable",
                        title: "Broadcast small table in Sort Merge join",
                        location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
                        message: `Broadcast the smaller table (${humanFileSize(smallTable)}) to avoid shuffling the larger table (${humanFileSize(largeTable)})`,
                        suggestion: `
    1. Use broadcast(small_table_df) hint on the smaller table
    2. Set spark.sql.autoBroadcastJoinThreshold to a value larger than ${humanFileSize(smallTable)}`,
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