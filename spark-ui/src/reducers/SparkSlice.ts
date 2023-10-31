import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { AppStore, StatusStore } from "../interfaces/AppStore";
import { Attempt } from "../interfaces/SparkApplications";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkExecutors } from "../interfaces/SparkExecutors";
import { SparkJobs } from "../interfaces/SparkJobs";
import { SparkSQLs } from "../interfaces/SparkSQLs";
import { SparkStages } from "../interfaces/SparkStages";
import { NodesMetrics } from "../interfaces/SqlMetrics";
import { SQLPlans } from "../interfaces/SQLPlan";
import { reduceAlers } from './AlertsReducer';
import { extractConfig, extractRunMetadata } from "./ConfigReducer";
import { calculateSparkExecutorsStore } from "./ExecutorsReducer";
import {
  calculateJobsStore,
  calculateSqlQueryLevelMetricsReducer,
  calculateStagesStore,
} from "./MetricsReducer";
import { calculateSqlStore, updateSqlNodeMetrics } from "./SqlReducer";
import {
  calculateDuration,
  calculateSparkExecutorsStatus,
  calculateSqlIdleTime,
  calculateStageStatus,
} from "./StatusReducer";

export const initialState: AppStore = {
  isConnected: false,
  isInitialized: false,
  runMetadata: undefined,
  status: undefined,
  config: undefined,
  sql: undefined,
  jobs: undefined,
  stages: undefined,
  executors: undefined,
  alerts: undefined,
};

const sparkSlice = createSlice({
  name: "sparkApi",
  initialState,
  reducers: {
    setInitial: (
      state,
      action: PayloadAction<{
        config: SparkConfiguration;
        appId: string;
        attempt: Attempt;
        epocCurrentTime: number;
      }>,
    ) => {
      const [appName, config] = extractConfig(action.payload.config);
      const runMetadata = extractRunMetadata(
        appName,
        action.payload.appId,
        action.payload.attempt,
      );
      const duration = calculateDuration(
        runMetadata,
        action.payload.epocCurrentTime,
      );
      const newStatus: StatusStore = {
        duration: duration,
        sqlIdleTime: 0,
        stages: undefined,
        executors: undefined,
      };

      state.isConnected = true;
      state.isInitialized = true;
      state.runMetadata = runMetadata;
      state.config = config;
      state.status = newStatus;
      state.alerts = { alerts: [] }
      state.sql = undefined;
      state.stages = undefined;
      state.jobs = undefined;
      state.executors = undefined;
    },
    setStages: (state, action: PayloadAction<{ value: SparkStages }>) => {
      if (state.status === undefined) {
        return;
      }
      const stageStatus = calculateStageStatus(
        state.status?.stages,
        action.payload.value,
      );
      if (stageStatus !== state.status?.stages) {
        const stageStore = calculateStagesStore(
          state.stages,
          action.payload.value,
        );
        state.status.stages = stageStatus;
        state.stages = stageStore;
      }
    },
    setSparkExecutors: (
      state,
      action: PayloadAction<{ value: SparkExecutors }>,
    ) => {
      if (
        state.runMetadata === undefined ||
        state.status === undefined ||
        state.config === undefined
      ) {
        return;
      }

      const currentEndDate =
        state.runMetadata.startTime + state.status.duration!;
      const executorsStore = calculateSparkExecutorsStore(
        state.executors,
        action.payload.value,
        currentEndDate,
        state.config.executorMemoryBytes,
      );
      const executorsStatus = calculateSparkExecutorsStatus(executorsStore);

      state.status = { ...state.status, executors: executorsStatus };
      state.executors = executorsStore;
    },
    setSQLMetrics: (
      state,
      action: PayloadAction<{ sqlId: string; value: NodesMetrics }>,
    ) => {
      if (state.status === undefined) {
        return;
      }
      if (state.jobs && state.sql && state.executors && state.stages) {
        const sqlWithNodeMetrics = updateSqlNodeMetrics(
          state.sql,
          action.payload.sqlId,
          action.payload.value,
        );
        state.sql = calculateSqlQueryLevelMetricsReducer(
          sqlWithNodeMetrics,
          state.status,
          state.jobs,
          state.stages,
          state.executors,
        );
      }
    },
    updateDuration: (
      state,
      action: PayloadAction<{ epocCurrentTime: number }>,
    ) => {
      if (state.status === undefined) {
        return;
      }
      state.status.duration = calculateDuration(
        state.runMetadata!,
        action.payload.epocCurrentTime,
      );
    },
    updateConnection: (
      state,
      action: PayloadAction<{ isConnected: boolean }>,
    ) => {
      if (state.isConnected !== action.payload.isConnected) {
        state.isConnected = action.payload.isConnected;
      }
    },
    setSparkJobs: (state, action: PayloadAction<{ value: SparkJobs }>) => {
      if (state.stages) {
        state.jobs = calculateJobsStore(
          state.jobs,
          state.stages,
          action.payload.value,
        );
      }
    },
    setSQL: (
      state,
      action: PayloadAction<{ sqls: SparkSQLs; plans: SQLPlans }>,
    ) => {
      if (state.status === undefined) {
        return;
      }

      const sqlStore = calculateSqlStore(
        state.sql,
        action.payload.sqls,
        action.payload.plans,
      );
      if (state.jobs && state.executors && state.stages) {
        state.sql = calculateSqlQueryLevelMetricsReducer(
          sqlStore,
          state.status,
          state.jobs,
          state.stages,
          state.executors,
        );
      } else {
        state.sql = sqlStore;
      }
    },
    onCycleEnd: (state) => {
      if (state.status === undefined) {
        return;
      }

      state.status.sqlIdleTime = Math.max(0, calculateSqlIdleTime(state.sql!, state.status!, state.runMetadata!));

      if (state.jobs && state.sql && state.executors && state.stages) {
        state.sql = calculateSqlQueryLevelMetricsReducer(
          state.sql,
          state.status,
          state.jobs,
          state.stages,
          state.executors,
        );
      }
      state.alerts = reduceAlers(state.sql!, state.status!, state.config!);
    },
  },
});

// Export the action creators and the reducer
export const {
  setInitial,
  setStages,
  setSparkExecutors,
  setSQLMetrics,
  updateDuration,
  updateConnection,
  setSparkJobs,
  setSQL,
  onCycleEnd,
} = sparkSlice.actions;

export default sparkSlice.reducer;
