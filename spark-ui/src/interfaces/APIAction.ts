import { SparkConfiguration } from "./SparkConfiguration";
import { SparkExecutors } from "./SparkExecutors";
import { SparkJobs } from "./SparkJobs";
import { SparkSQLs } from "./SparkSQLs";
import { SparkStages } from "./SparkStages";
import { NodesMetrics } from "./SqlMetrics";

export type ApiAction =
    { type: 'setInitial', config: SparkConfiguration, appId: string, sparkVersion: string } |
    { type: 'setStatus', value: SparkStages } |
    { type: 'setSparkExecutors', value: SparkExecutors } |
    { type: 'setSQMetrics', value: NodesMetrics, sqlId: string } |
    { type: 'setSparkJobs', value: SparkJobs } |
    { type: 'setSQL', value: SparkSQLs };
