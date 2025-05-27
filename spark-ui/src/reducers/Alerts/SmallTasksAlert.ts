import { duration } from "moment";
import {
  Alerts,
  SparkSQLStore,
  SparkStagesStore,
} from "../../interfaces/AppStore";
import { humanizeTimeDiff } from "../../utils/FormatUtils";

const LARGE_TASKS_NUM_THRESHOLD = 5000;
const MEDIAN_TASK_TIME_THRESHOLD_MS = 500;
const TASKS_RECOMMENDED_DECREASE_RATIO = 10;

export function reduceSmallTasksAlert(
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

      if (stageData !== undefined &&
        stageData.numTasks > LARGE_TASKS_NUM_THRESHOLD &&
        stageData.mediumTaskDuration !== undefined &&
        stageData.mediumTaskDuration < MEDIAN_TASK_TIME_THRESHOLD_MS) {
        const medianTaskDurationTxt =
          stageData.mediumTaskDuration === undefined
            ? ""
            : humanizeTimeDiff(duration(stageData.mediumTaskDuration));
        const recommendedTaskNum = Math.ceil(stageData.numTasks / TASKS_RECOMMENDED_DECREASE_RATIO);

        alerts.push({
          id: `SmallTasks_${sql.id}_${node.nodeId}`,
          name: "smallTasks",
          title: "Large Number Of Small Tasks",
          location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
          message: `${stageData.numTasks} tasks with median task duration of ${medianTaskDurationTxt}, which causes large scheduling overhead for Spark`,
          suggestion: `
  1. Repartition to less tasks, so you will have less overhead, by running .repartition(${recommendedTaskNum})
  2. Instead of repartition, you can run .coallese(${recommendedTaskNum}) to decrease the number of tasks without shuffling on the expense of less parallelism
  3. If you need to hash-partition, call repartition like this: .repartition(${recommendedTaskNum}, "hash_key1", "hash_key2")
`,
          shortSuggestion: `.repartition(${recommendedTaskNum}) before this transformation`,
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
