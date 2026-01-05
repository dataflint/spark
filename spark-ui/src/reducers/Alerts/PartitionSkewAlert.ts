import { duration } from "moment";
import {
  Alerts,
  SparkSQLStore,
} from "../../interfaces/AppStore";
import { StageMap } from "../../interfaces/StageMap";
import { humanizeTimeDiff } from "../../utils/FormatUtils";

export function reducePartitionSkewAlert(
  sql: SparkSQLStore,
  stageMap: StageMap,
  alerts: Alerts,
) {
  // Track which stages we've already created alerts for to avoid duplicates
  const alertedStages = new Set<string>();

  for (const sqlItem of sql.sqls) {
    for (const node of sqlItem.nodes) {
      const stageInfo = node.stage;
      if (stageInfo === undefined || stageInfo.type !== "onestage") {
        continue;
      }

      const stageId = stageInfo.stageId;
      const alertKey = `${sqlItem.id}_${stageId}`;

      // Skip if we've already created an alert for this stage in this SQL
      if (alertedStages.has(alertKey)) {
        continue;
      }

      // O(1) lookup instead of O(n) find
      const stageData = stageMap.get(stageId);

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
          id: `partitionSkew_${sqlItem.id}_stage_${stageId}`,
          name: "partitionSkew",
          title: "Partition Skew",
          location: `In: SQL query "${sqlItem.description}" (id: ${sqlItem.id}), Stage ${stageId}`,
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
            sqlId: sqlItem.id,
            stageId: stageId,
          },
        });
      }
    }
  }
}
