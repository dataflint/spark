import { SparkConfiguration } from "./SparkConfiguration";
import { SparkSQLs } from "./SparkSQLs";
import { SparkStages } from "./SparkStages";

export type ApiAction =
    { type: 'setInitial', config: SparkConfiguration, appId: string, sparkVersion: string } |
    { type: 'setStatus', value: SparkStages } |
    { type: 'setSQL', value: SparkSQLs };
