import { ApiAction } from "../interfaces/APIAction";
import {
  AppStore,
  RunMetadataStore,
  SparkExecutorsStatus,
  StagesSummeryStore,
  StatusStore,
} from "../interfaces/AppStore";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkStages } from "../interfaces/SparkStages";
import { humanFileSize, timeStrToEpocTime } from "../utils/FormatUtils";
import isEqual from "lodash/isEqual";
import { calculateSqlStore, updateSqlNodeMetrics } from "./SqlReducer";
import { SparkExecutor, SparkExecutors } from "../interfaces/SparkExecutors";
import { Attempt } from "../interfaces/SparkApplications";
import moment from "moment";
import { extractConfig, extractRunMetadata } from "./ConfigReducer";
import {
  calculateDuration,
  calculateSparkExecutorsStatus,
  calculateStageStatus,
} from "./StatusReducer";
import {
  calculateJobsStore,
  calculateStagesStore,
  calculateSqlQueryLevelMetricsReducer,
} from "./MetricsReducer";
import { calculateSparkExecutorsStore } from "./ExecutorsReducer";
import { PayloadAction, createSlice } from "@reduxjs/toolkit";
import { SparkSQLs } from "../interfaces/SparkSQLs";
import { SparkJobs } from "../interfaces/SparkJobs";
import { SQLPlans } from "../interfaces/SQLPlan";
import { NodesMetrics } from "../interfaces/SqlMetrics";

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
        stages: undefined,
        executors: undefined,
      };

      state.isConnected = true;
      state.isInitialized = true;
      state.runMetadata = runMetadata;
      state.config = config;
      state.status = newStatus;
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
      if (state.runMetadata === undefined || state.status === undefined) {
        return;
      }

      const currentEndDate =
        state.runMetadata.startTime + state.status.duration!;
      const executorsStore = calculateSparkExecutorsStore(
        state.executors,
        action.payload.value,
        currentEndDate,
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
      if (state.jobs && state.sql && state.executors) {
        const sqlWithNodeMetrics = updateSqlNodeMetrics(
          state.sql,
          action.payload.sqlId,
          action.payload.value,
        );
        state.sql = calculateSqlQueryLevelMetricsReducer(
          sqlWithNodeMetrics,
          state.status,
          state.jobs,
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
      state.status = {
        ...state.status,
        duration: calculateDuration(
          state.runMetadata!,
          action.payload.epocCurrentTime,
        ),
      };
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
      if (state.jobs && state.executors) {
        state.sql = calculateSqlQueryLevelMetricsReducer(
          sqlStore,
          state.status,
          state.jobs,
          state.executors,
        );
      } else {
        state.sql = sqlStore;
      }
    },
    calculateSqlQueryLevelMetrics: (state) => {
      if (state.status === undefined) {
        return;
      }

      if (state.jobs && state.sql && state.executors) {
        state.sql = calculateSqlQueryLevelMetricsReducer(
          state.sql,
          state.status,
          state.jobs,
          state.executors,
        );
      }
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
  calculateSqlQueryLevelMetrics,
} = sparkSlice.actions;

export default sparkSlice.reducer;
