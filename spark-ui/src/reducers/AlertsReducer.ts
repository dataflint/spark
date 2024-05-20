import {
  Alerts,
  AlertsStore,
  ConfigStore,
  SparkSQLStore,
  SparkStagesStore,
  StatusStore,
} from "../interfaces/AppStore";
import { reduceIcebergReplaces } from "./Alerts/IcebergReplacesReducer";
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
): AlertsStore {
  const alerts: Alerts = [];
  reduceMemoryAlerts(statusStore, config, alerts);
  reduceWastedCoresAlerts(statusStore, config, alerts);
  reduceSQLInputOutputAlerts(sqlStore, alerts);
  reducePartitionSkewAlert(sqlStore, stageStore, alerts);
  reduceSmallTasksAlert(sqlStore, stageStore, alerts);
  reduceIcebergReplaces(sqlStore, alerts);
  return {
    alerts: alerts,
  };
}
