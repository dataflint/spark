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
    // Track which stages we've already created alerts for to avoid duplicates
    const alertedStages = new Set<string>();

    sql.sqls.forEach((sql) => {
        sql.nodes.forEach((node) => {
            const stageInfo = node.stage;
            if (stageInfo === undefined) {
                return;
            }

            // Handle both one-stage and exchange (shuffle) operations
            if (stageInfo.type === "onestage") {
                // For single stage operations
                checkStageForLargePartitions(sql, stageInfo.stageId, stages, alerts, alertedStages);
            } else if (stageInfo.type === "exchange") {
                // For shuffle operations, check the write stage
                if (stageInfo.writeStage !== -1) {
                    checkStageForLargePartitions(sql, stageInfo.writeStage, stages, alerts, alertedStages);
                }
                if (stageInfo.readStage !== -1) {
                    checkStageForLargePartitions(sql, stageInfo.readStage, stages, alerts, alertedStages);
                }
            }
        });
    });
}

function checkStageForLargePartitions(
    sql: any,
    stageId: number,
    stages: SparkStagesStore,
    alerts: Alerts,
    alertedStages: Set<string>
) {
    const alertKey = `${sql.id}_${stageId}`;
    
    // Skip if we've already created an alert for this stage in this SQL
    if (alertedStages.has(alertKey)) {
        return;
    }

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
            alertedStages.add(alertKey);
            
            const maxPartitionSizeFormatted = humanFileSize(maxPartitionSize);

            alerts.push({
                id: `maxPartitionToBig_${sql.id}_stage_${stageId}`,
                name: "maxPartitionToBig",
                title: "Large Partition Size",
                location: `In: SQL query "${sql.description}" (id: ${sql.id}), Stage ${stageId}`,
                message: `Maximum ${dataType} partition size in stage ${stageId} is ${maxPartitionSizeFormatted}, which is too big and can cause performance issues or OOM errors`,
                suggestion: `
  1. Increase the number of partitions to reduce the size of each partition
  2. Use more specific partitioning keys to distribute data more evenly
`,
                type: "warning",
                source: {
                    type: "stage",
                    sqlId: sql.id,
                    stageId: stageId,
                },
            });
        }
    }
}
