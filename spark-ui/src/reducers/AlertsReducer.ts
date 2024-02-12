import {
  Alerts,
  AlertsStore,
  ConfigStore,
  SparkSQLStore,
  SparkStagesStore,
  StatusStore,
} from "../interfaces/AppStore";
import { reduceMemoryAlerts } from "./Alerts/MemoryAlertsReducer";
import { reduceSQLInputOutputAlerts } from "./Alerts/MemorySQLInputOutputAlerts";
import { reducePartitionSkewAlert } from "./Alerts/PartitionSkewAlert";
import { reduceWastedCoresAlerts } from "./Alerts/WastedCoresAlertsReducer";

export function reduceAlerts(
  sqlStore: SparkSQLStore,
  statusStore: StatusStore,
  stageStore: SparkStagesStore,
  config: ConfigStore,
): AlertsStore {
  const alerts: Alerts = [];
  reduceMemoryAlerts(statusStore, config, alerts);
  reduceWastedCoresAlerts(statusStore, config, alerts);
  reduceSQLInputOutputAlerts(sqlStore, alerts);
  reducePartitionSkewAlert(sqlStore, stageStore, alerts);
  return {
    alerts: alerts,
  };
}
