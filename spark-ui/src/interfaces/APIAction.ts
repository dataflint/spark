import { Attempt } from "./SparkApplications";
import { SparkConfiguration } from "./SparkConfiguration";
import { SparkExecutors } from "./SparkExecutors";
import { SparkJobs } from "./SparkJobs";
import { SQLPlans } from "./SQLPlan";
import { SparkSQLs } from "./SparkSQLs";
import { SparkStages } from "./SparkStages";
import { NodesMetrics } from "./SqlMetrics";

export type ApiAction =
  | {
      type: "setInitial";
      config: SparkConfiguration;
      appId: string;
      attempt: Attempt;
      epocCurrentTime: number;
    }
  | { type: "setStages"; value: SparkStages }
  | { type: "setSparkExecutors"; value: SparkExecutors }
  | { type: "setSQLMetrics"; value: NodesMetrics; sqlId: string }
  | { type: "setSparkJobs"; value: SparkJobs }
  | { type: "setSQL"; sqls: SparkSQLs; plans: SQLPlans }
  | { type: "updateConnection"; isConnected: boolean }
  | { type: "updateDuration"; epocCurrentTime: number }
  | { type: "calculateSqlQueryLevelMetrics" };
