import {
  Alerts,
  AlertsStore,
  ConfigStore,
  SparkSQLStore,
  SparkStagesStore,
  StatusStore,
  SparkExecutorsStore,
} from "../interfaces/AppStore";
import { reduceIcebergReplaces } from "./Alerts/IcebergReplacesReducer";
import { reduceLongFilterConditions } from "./Alerts/LongFilterConditions";
import { reduceMemoryAlerts } from "./Alerts/MemoryAlertsReducer";
import { reduceSQLInputOutputAlerts } from "./Alerts/MemorySQLInputOutputAlerts";
import { reducePartitionSkewAlert } from "./Alerts/PartitionSkewAlert";
import { reduceSmallTasksAlert } from "./Alerts/SmallTasksAlert";
import { reduceWastedCoresAlerts } from "./Alerts/WastedCoresAlertsReducer";

export function reduceAlerts(
  sqlStore: SparkSQLStore,
  statusStore: StatusStore,
  stageStore: SparkStagesStore,
  config: ConfigStore,
  executors: SparkExecutorsStore,
  environmentInfo: any 
): AlertsStore {
  const alerts: Alerts = [];
  reduceMemoryAlerts(statusStore, config, environmentInfo, executors ,alerts);
  reduceWastedCoresAlerts(statusStore, config, alerts);
  reduceSQLInputOutputAlerts(sqlStore, alerts);
  reducePartitionSkewAlert(sqlStore, stageStore, alerts);
  reduceSmallTasksAlert(sqlStore, stageStore, alerts);
  reduceIcebergReplaces(sqlStore, alerts);
  reduceLongFilterConditions(sqlStore, alerts);
  return {
    alerts: alerts,
  };
}
