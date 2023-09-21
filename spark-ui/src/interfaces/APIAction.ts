import { Attempt } from "./SparkApplications";
import { SparkConfiguration } from "./SparkConfiguration";
import { SparkExecutors } from "./SparkExecutors";
import { SparkJobs } from "./SparkJobs";
import { SparkSQLs } from "./SparkSQLs";
import { SparkStages } from "./SparkStages";
import { NodesMetrics } from "./SqlMetrics";

export type ApiAction =
    { type: 'setInitial', config: SparkConfiguration, appId: string, attempt: Attempt, epocCurrentTime: number } |
    { type: 'setStages', value: SparkStages } |
    { type: 'setSparkExecutors', value: SparkExecutors, epocCurrentTime: number } |
    { type: 'setSQMetrics', value: NodesMetrics, sqlId: string } |
    { type: 'setSparkJobs', value: SparkJobs } |
    { type: 'setSQL', value: SparkSQLs } | 
    { type: 'updateConnection', isConnected: boolean } | 
    { type: 'updateDuration', epocCurrentTime: number };