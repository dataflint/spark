import { ApiAction } from "../interfaces/APIAction";
import { AppStore, RunMetadataStore, SparkExecutorsStatus, StagesSummeryStore, StatusStore } from '../interfaces/AppStore';
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkStages } from "../interfaces/SparkStages";
import { humanFileSize, timeStrToEpocTime } from '../utils/FormatUtils';
import isEqual from 'lodash/isEqual';
import { calculateSqlStore, updateSqlNodeMetrics } from "./SqlReducer";
import { SparkExecutor, SparkExecutors } from "../interfaces/SparkExecutors";
import { Attempt } from '../interfaces/SparkApplications';
import moment from 'moment'
import { extractConfig, extractRunMetadata } from "./ConfigReducer";
import { calculateDuration, calculateSparkExecutorsStatus, calculateStageStatus } from "./StatusReducer";
import { calculateJobsStore, calculateStagesStore, calculateSqlQueryLevelMetrics } from "./MetricsReducer";
import { calculateSparkExecutorsStore } from "./ExecutorsReducer";


export const initialState: AppStore = {
    isConnected: false,
    isInitialized: false,
    runMetadata: undefined,
    status: undefined,
    config: undefined,
    sql: undefined,
    jobs: undefined,
    stages: undefined,
    executors: undefined
  };

export function sparkApiReducer(store: AppStore, action: ApiAction): AppStore {
    if(action.type === 'setInitial') {
        const [appName, config] = extractConfig(action.config)
        const runMetadata = extractRunMetadata(appName, action.appId, action.attempt);
        const duration = calculateDuration(runMetadata, action.epocCurrentTime);
        const newStatus: StatusStore = { duration: duration, stages: undefined, executors: undefined };
        const newStore: AppStore = { 
            isConnected: true, 
            isInitialized: true, 
            runMetadata: runMetadata, 
            config: config, 
            status: newStatus, 
            sql: undefined,
            stages: undefined,
            jobs: undefined,
            executors: undefined
        };
        return newStore
    }
    if (!store.isInitialized) {
        // Shouldn't happen as store should be initialized when we get updated metrics
        return store;
    }

    switch (action.type) {
        case 'setSQL':
            const sqlStore = calculateSqlStore(store.sql, action.value);
            if(store.jobs === undefined || store.executors === undefined) {
                // shouldn't happen as we should have jobs and executors before we have sql
                return { ...store, sql: sqlStore };
            }
            return { ...store, sql: calculateSqlQueryLevelMetrics(sqlStore, store.status, store.jobs, store.executors) };
        case 'setStages':
            const stageStatus = calculateStageStatus(store.status.stages, action.value);
            if (stageStatus === store.status.stages) {
                return store;
            } else {
                const stageStore = calculateStagesStore(store.stages, action.value)
                return { ...store, status: { ...store.status, stages: stageStatus }, stages: stageStore };
            }
        case 'setSparkExecutors':
            const currentEndDate = store.runMetadata.startTime + store.status.duration;
            const executorsStore = calculateSparkExecutorsStore(store.executors, action.value, currentEndDate);
            const executorsStatus = calculateSparkExecutorsStatus(executorsStore);
            return { ...store, status: { ...store.status, executors: executorsStatus }, executors: executorsStore };
        case 'setSQLMetrics':
            if (store.jobs === undefined || store.sql === undefined || store.executors === undefined) {
                // Shouldn't happen as store should be initialized when we get updated metrics
                return store;
            }
            const sqlWithNodeMetrics = updateSqlNodeMetrics(store.sql, action.sqlId, action.value)
            const sqlWithMetrics = calculateSqlQueryLevelMetrics(sqlWithNodeMetrics, store.status, store.jobs, store.executors);
            return { ...store, sql: sqlWithMetrics };
        case 'updateDuration':
            return { ...store, status: { ...store.status, duration: calculateDuration(store.runMetadata, action.epocCurrentTime) } };
        case 'updateConnection':
            if (store.isConnected === action.isConnected)
                return store;

            return { ...store, isConnected: action.isConnected }
        case 'setSparkJobs':
            if (store.stages === undefined) {
                return store;
            }

            const jobsStore = calculateJobsStore(store.jobs, store.stages, action.value);
            return { ...store, jobs: jobsStore };
        case 'calculateSqlQueryLevelMetrics':
            if (store.jobs === undefined || store.sql === undefined || store.executors === undefined) {
                // Shouldn't happen as store should be initialized when we get updated metrics
                return store;
            }

            const updatedStore = calculateSqlQueryLevelMetrics(store.sql, store.status, store.jobs, store.executors);
            return {...store, sql: updatedStore}
        default:
            // this shouldn't happen as we suppose to handle all actions
            return store;
    }
}
