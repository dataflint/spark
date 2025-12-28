import { duration } from "moment";
import {
  Alerts,
  SparkSQLStore,
  SparkStagesStore,
} from "../../interfaces/AppStore";
import { humanizeTimeDiff } from "../../utils/FormatUtils";

export function reducePartitionSkewAlert(
  sql: SparkSQLStore,
  stages: SparkStagesStore,
  alerts: Alerts,
) {
  // Track which stages we've already created alerts for to avoid duplicates
  const alertedStages = new Set<string>();

  sql.sqls.forEach((sql) => {
    sql.nodes.forEach((node) => {
      const stageInfo = node.stage;
      if (stageInfo === undefined || stageInfo.type !== "onestage") {
        return;
      }
      
      const stageId = stageInfo.stageId;
      const alertKey = `${sql.id}_${stageId}`;
      
      // Skip if we've already created an alert for this stage in this SQL
      if (alertedStages.has(alertKey)) {
        return;
      }
      
      const stageData = stages.find(
        (stage) => stage.stageId === stageId,
      );

      if (stageData?.hasPartitionSkew === true) {
        alertedStages.add(alertKey);
        
        const maxTaskDurationTxt =
          stageData.maxTaskDuration === undefined
            ? ""
            : humanizeTimeDiff(duration(stageData.maxTaskDuration));
        const medianTaskDurationTxt =
          stageData.mediumTaskDuration === undefined
            ? ""
            : humanizeTimeDiff(duration(stageData.mediumTaskDuration));
        const skewRatio =
          stageData.maxTaskDuration === 0
            ? 0
            : (stageData.maxTaskDuration ?? 0) /
              (stageData.mediumTaskDuration ?? 1);

        alerts.push({
          id: `partitionSkew_${sql.id}_stage_${stageId}`,
          name: "partitionSkew",
          title: "Partition Skew",
          location: `In: SQL query "${sql.description}" (id: ${sql.id}), Stage ${stageId}`,
          message: `Partition skew ratio of ${skewRatio.toFixed(
            1,
          )}X, median task duration is ${medianTaskDurationTxt} and max task duration is ${maxTaskDurationTxt}`,
          suggestion: `
  1. Fix the partition skew, by changing the repartition your data differently
  2. Do not fix the partition skew, and instead decrease number of executors/cores, so you will have less resource waste
`,
          type: "warning",
          source: {
            type: "stage",
            sqlId: sql.id,
            stageId: stageId,
          },
        });
      }
    });
  });
}
