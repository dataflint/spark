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
  sql.sqls.forEach((sql) => {
    sql.nodes.forEach((node) => {
      const stageInfo = node.stage;
      if (stageInfo === undefined || stageInfo.type !== "onestage") {
        return;
      }
      const stageData = stages.find(
        (stage) => stage.stageId === stageInfo.stageId,
      );

      if (stageData?.hasPartitionSkew === true) {
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
          id: `partitionSkew_${sql.id}_${node.nodeId}`,
          name: "partitionSkew",
          title: "Partition Skew",
          location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
          message: `Partition skew ratio of ${skewRatio.toFixed(
            1,
          )}X, median task duration is ${medianTaskDurationTxt} and max task duration is ${maxTaskDurationTxt}`,
          suggestion: `
  1. Fix the partition skew, by changing the repartition your data differently
  2. Do not fix the partition skew, and instead decrease number of executors/cores, so you will have less resource waste
`,
          type: "warning",
          source: {
            type: "sql",
            sqlId: sql.id,
            sqlNodeId: node.nodeId,
          },
        });
      }
    });
  });
}
