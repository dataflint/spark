import {
    Alerts,
    SparkSQLStore,
} from "../../interfaces/AppStore";
import { StageMap } from "../../interfaces/StageMap";
import { humanFileSize } from "../../utils/FormatUtils";

// 5GB threshold in bytes
const MAX_PARTITION_SIZE_THRESHOLD = 5 * 1024 * 1024 * 1024;

export function reduceMaxPartitionToBigAlert(
    sql: SparkSQLStore,
    stageMap: StageMap,
    alerts: Alerts,
) {
    // Track which stages we've already created alerts for to avoid duplicates
    const alertedStages = new Set<string>();

    for (const sqlItem of sql.sqls) {
        for (const node of sqlItem.nodes) {
            const stageInfo = node.stage;
            if (stageInfo === undefined) {
                continue;
            }

            // Handle both one-stage and exchange (shuffle) operations
            if (stageInfo.type === "onestage") {
                // For single stage operations
                checkStageForLargePartitions(sqlItem, stageInfo.stageId, stageMap, alerts, alertedStages);
            } else if (stageInfo.type === "exchange") {
                // For shuffle operations, check the write stage
                if (stageInfo.writeStage !== -1) {
                    checkStageForLargePartitions(sqlItem, stageInfo.writeStage, stageMap, alerts, alertedStages);
                }
                if (stageInfo.readStage !== -1) {
                    checkStageForLargePartitions(sqlItem, stageInfo.readStage, stageMap, alerts, alertedStages);
                }
            }
        }
    }
}

function checkStageForLargePartitions(
    sqlItem: any,
    stageId: number,
    stageMap: StageMap,
    alerts: Alerts,
    alertedStages: Set<string>
) {
    const alertKey = `${sqlItem.id}_${stageId}`;

    // Skip if we've already created an alert for this stage in this SQL
    if (alertedStages.has(alertKey)) {
        return;
    }

    // O(1) lookup instead of O(n) find
    const stageData = stageMap.get(stageId);

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
            alertedStages.add(alertKey);

            const maxPartitionSizeFormatted = humanFileSize(maxPartitionSize);

            alerts.push({
                id: `maxPartitionToBig_${sqlItem.id}_stage_${stageId}`,
                name: "maxPartitionToBig",
                title: "Large Partition Size",
                location: `In: SQL query "${sqlItem.description}" (id: ${sqlItem.id}), Stage ${stageId}`,
                message: `Maximum ${dataType} partition size in stage ${stageId} is ${maxPartitionSizeFormatted}, which is too big and can cause performance issues or OOM errors`,
                suggestion: `
  1. Increase the number of partitions to reduce the size of each partition
  2. Use more specific partitioning keys to distribute data more evenly
`,
                type: "warning",
                source: {
                    type: "stage",
                    sqlId: sqlItem.id,
                    stageId: stageId,
                },
            });
        }
    }
}
