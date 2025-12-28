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

      if (stageData !== undefined &&
        stageData.numTasks > LARGE_TASKS_NUM_THRESHOLD &&
        stageData.mediumTaskDuration !== undefined &&
        stageData.mediumTaskDuration < MEDIAN_TASK_TIME_THRESHOLD_MS) {

        alertedStages.add(alertKey);

        const medianTaskDurationTxt =
          stageData.mediumTaskDuration === undefined
            ? ""
            : humanizeTimeDiff(duration(stageData.mediumTaskDuration));
        const recommendedTaskNum = Math.ceil(stageData.numTasks / TASKS_RECOMMENDED_DECREASE_RATIO);

        alerts.push({
          id: `SmallTasks_${sql.id}_stage_${stageId}`,
          name: "smallTasks",
          title: "Large Number Of Small Tasks",
          location: `In: SQL query "${sql.description}" (id: ${sql.id}), Stage ${stageId}`,
          message: `${stageData.numTasks} tasks with median task duration of ${medianTaskDurationTxt}, which causes large scheduling overhead for Spark`,
          suggestion: `
  1. Repartition to less tasks, so you will have less overhead, by running .repartition(${recommendedTaskNum})
  2. Instead of repartition, you can run .coallese(${recommendedTaskNum}) to decrease the number of tasks without shuffling on the expense of less parallelism
  3. If you need to hash-partition, call repartition like this: .repartition(${recommendedTaskNum}, "hash_key1", "hash_key2")
`,
          shortSuggestion: `.repartition(${recommendedTaskNum}) before this transformation`,
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
