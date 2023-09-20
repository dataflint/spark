import { SparkConfiguration } from "./SparkConfiguration";
import { SparkSQLs } from "./SparkSQLs";
import { SparkStages } from "./SparkStages";
import { NodesMetrics } from "./SqlMetrics";

export type ApiAction =
    { type: 'setInitial', config: SparkConfiguration, appId: string, sparkVersion: string } |
    { type: 'setStatus', value: SparkStages } |
    { type: 'setSQMetrics', value: NodesMetrics, sqlId: string } |
    { type: 'setSQL', value: SparkSQLs };
