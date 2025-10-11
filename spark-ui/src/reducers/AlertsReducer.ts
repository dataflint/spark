import {
  Alerts,
  AlertsStore,
  ConfigStore,
  SparkExecutorsStore,
  SparkSQLStore,
  SparkStagesStore,
  StatusStore,
} from "../interfaces/AppStore";
import { parseAlertDisabledConfig } from "../utils/ConfigParser";
import { reduceBroadcastTooLargeAlert } from "./Alerts/BroadcastTooLargeAlert";
import { reduceFullScanAlert } from "./Alerts/FullScanAlert";
import { reduceIcebergReplaces } from "./Alerts/IcebergReplacesReducer";
import { reduceJoinToBroadcastAlert } from "./Alerts/JoinToBroadcastAlert";
import { reduceLargeCrossJoinScanAlert } from "./Alerts/LargeCrossJoinScanAlert";
import { reduceLongFilterConditions } from "./Alerts/LongFilterConditions";
import { reduceMaxPartitionToBigAlert } from "./Alerts/MaxPartitionToBigAlert";
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
  reduceMemoryAlerts(statusStore, config, environmentInfo, executors, alerts);
  reduceWastedCoresAlerts(statusStore, config, alerts);
  reduceSQLInputOutputAlerts(sqlStore, alerts);
  reducePartitionSkewAlert(sqlStore, stageStore, alerts);
  reduceSmallTasksAlert(sqlStore, stageStore, alerts);
  reduceIcebergReplaces(sqlStore, alerts);
  reduceLongFilterConditions(sqlStore, alerts);
  reduceBroadcastTooLargeAlert(sqlStore, alerts);
  reduceJoinToBroadcastAlert(sqlStore, alerts);
  reduceLargeCrossJoinScanAlert(sqlStore, alerts);
  reduceMaxPartitionToBigAlert(sqlStore, stageStore, alerts);
  reduceFullScanAlert(sqlStore, alerts);
  const disabledAlerts = parseAlertDisabledConfig(config.alertDisabled);
  const filteredAlerts = alerts.filter(alert => !disabledAlerts.has(alert.name));
  return {
    alerts: filteredAlerts,
  };
}
