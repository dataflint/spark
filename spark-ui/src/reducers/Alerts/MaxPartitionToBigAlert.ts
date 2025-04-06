import {
    Alerts,
    SparkSQLStore,
    SparkStagesStore,
} from "../../interfaces/AppStore";
import { humanFileSize } from "../../utils/FormatUtils";

// 5GB threshold in bytes
const MAX_PARTITION_SIZE_THRESHOLD = 5 * 1024 * 1024 * 1024;

export function reduceMaxPartitionToBigAlert(
    sql: SparkSQLStore,
    stages: SparkStagesStore,
    alerts: Alerts,
) {
    sql.sqls.forEach((sql) => {
        sql.nodes.forEach((node) => {
            const stageInfo = node.stage;
            if (stageInfo === undefined) {
                return;
            }

            // Handle both one-stage and exchange (shuffle) operations
            if (stageInfo.type === "onestage") {
                // For single stage operations
                checkStageForLargePartitions(sql, node, stageInfo.stageId, stages, alerts);
            } else if (stageInfo.type === "exchange") {
                // For shuffle operations, check the write stage
                if (stageInfo.writeStage !== -1) {
                    checkStageForLargePartitions(sql, node, stageInfo.writeStage, stages, alerts);
                }
                if (stageInfo.readStage !== -1) {
                    checkStageForLargePartitions(sql, node, stageInfo.readStage, stages, alerts);
                }
            }
        });
    });
}

function checkStageForLargePartitions(
    sql: any,
    node: any,
    stageId: number,
    stages: SparkStagesStore,
    alerts: Alerts
) {
    const stageData = stages.find(
        (stage) => stage.stageId === stageId,
    );

    if (stageData !== undefined) {
        // Check shuffle write distribution (output) first
        let maxPartitionSize = 0;
        let dataType = "";

        // If no shuffle write, check output distribution
        if (stageData.outputDistribution && stageData.outputDistribution.length > 10 && stageData.outputDistribution[10] > MAX_PARTITION_SIZE_THRESHOLD) {
            maxPartitionSize = stageData.outputDistribution[10];
            dataType = "output";
        }
        // If no output either, check input distribution
        else if (stageData.inputDistribution && stageData.inputDistribution.length > 10 && stageData.inputDistribution[10] > MAX_PARTITION_SIZE_THRESHOLD) {
            maxPartitionSize = stageData.inputDistribution[10];
            dataType = "input";
        }
        else if (stageData.shuffleWriteDistribution && stageData.shuffleWriteDistribution.length > 10 && stageData.shuffleWriteDistribution[10] > MAX_PARTITION_SIZE_THRESHOLD) {
            // The last element (index 10) is the maximum value
            maxPartitionSize = stageData.shuffleWriteDistribution[10];
            dataType = "shuffle write";
        } else if (stageData.shuffleReadDistribution && stageData.shuffleReadDistribution.length > 10 && stageData.shuffleReadDistribution[10] > MAX_PARTITION_SIZE_THRESHOLD) {
            // The last element (index 10) is the maximum value
            maxPartitionSize = stageData.shuffleReadDistribution[10];
            dataType = "shuffle read";
        }

        // If the maximum partition size exceeds our threshold, add an alert
        if (maxPartitionSize !== 0) {
            const maxPartitionSizeFormatted = humanFileSize(maxPartitionSize);
            const stageTypeText = node.stage?.type === "exchange" ? "shuffle" : "stage";

            alerts.push({
                id: `maxPartitionToBig_${sql.id}_${node.nodeId}_${stageId}`,
                name: "maxPartitionToBig",
                title: "Large Partition Size",
                location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}" (${stageTypeText} ${stageId})`,
                message: `Maximum ${dataType} partition size in stage ${stageId} is ${maxPartitionSizeFormatted}, which is too big and can cause performance issues or OOM errors`,
                suggestion: `
  1. Increase the number of partitions to reduce the size of each partition
  2. Use more specific partitioning keys to distribute data more evenly
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
} 