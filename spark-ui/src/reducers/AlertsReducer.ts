import {
  Alerts,
  AlertsStore,
  ConfigStore,
  SparkSQLStore,
  StatusStore,
} from "../interfaces/AppStore";
import { reduceMemoryAlerts } from "./Alerts/MemoryAlertsReducer";
import { reduceSQLInputOutputAlerts } from "./Alerts/MemorySQLInputOutputAlerts";
import { reduceWastedCoresAlerts } from "./Alerts/WastedCoresAlertsReducer";

export function reduceAlers(
  sqlStore: SparkSQLStore,
  statusStore: StatusStore,
  config: ConfigStore,
): AlertsStore {
  const alerts: Alerts = [];
  reduceMemoryAlerts(statusStore, config, alerts);
  reduceWastedCoresAlerts(statusStore, config, alerts);
  reduceSQLInputOutputAlerts(sqlStore, alerts);
  return {
    alerts: alerts,
  };
}
