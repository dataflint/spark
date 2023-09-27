import { ApiAction } from "../interfaces/APIAction";
import { AppStore, RunMetadataStore, SparkExecutorsStatus, StagesSummeryStore, StatusStore } from '../interfaces/AppStore';
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkStages } from "../interfaces/SparkStages";
import { humanFileSize } from "../utils/FormatUtils";
import isEqual from 'lodash/isEqual';
import { calculateSqlStore, updateSqlNodeMetrics } from "./SqlReducer";
import { SparkExecutor, SparkExecutors } from "../interfaces/SparkExecutors";
import { Attempt } from '../interfaces/SparkApplications';
import moment from 'moment'
import { extractConfig, extractRunMetadata } from "./ConfigReducer";
import { calculateDuration, calculateSparkExecutorsStatus, calculateStageStatus } from "./StatusReducer";
import { calculateJobsStore, calculateStagesStore, calculateSqlQueryLevelMetrics } from "./MetricsReducer";


export const initialState: AppStore = {
    isConnected: false,
    isInitialized: false,
    runMetadata: undefined,
    status: undefined,
    config: undefined,
    sql: undefined,
    jobs: undefined,
    stages: undefined
  };

export function sparkApiReducer(store: AppStore, action: ApiAction): AppStore {
    if(action.type === 'setInitial') {
        const [appName, config] = extractConfig(action.config)
        const runMetadata = extractRunMetadata(appName, action.appId, action.attempt);
        const duration = calculateDuration(runMetadata, action.epocCurrentTime);
        const newStatus: StatusStore = { duration, stages: undefined, executors: undefined };
        const newStore: AppStore = { 
            isConnected: true, 
            isInitialized: true, 
            runMetadata: runMetadata, 
            config: config, 
            status: newStatus, 
            sql: undefined,
            stages: undefined,
            jobs: undefined
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
            if (sqlStore === store.sql) {
                return store;
            } else {
                if(store.jobs === undefined) {
                    // shouldn't happen as we should have jobs before we have sql
                    return { ...store, sql: sqlStore };
                }
                const sqlWithMetrics = calculateSqlQueryLevelMetrics(sqlStore, store.jobs);
                return { ...store, sql: sqlWithMetrics };
            }
        case 'setStages':
            const stageStatus = calculateStageStatus(store.status.stages, action.value);
            const stageStore = calculateStagesStore(store.stages, action.value)
            if (stageStatus === store.status.stages) {
                return store;
            } else {
                return { ...store, status: { ...store.status, stages: stageStatus }, stages: stageStore };
            }
        case 'setSparkExecutors':
            const executorsStatus = calculateSparkExecutorsStatus(store.status.executors, store.status.stages?.totalTaskTimeMs, action.value);
            if (executorsStatus === store.status.executors) {
                return store;
            } else {
                return { ...store, status: { ...store.status, executors: executorsStatus } };
            }
        case 'setSQMetrics':
            if (store.sql === undefined || store.jobs === undefined) {
                // Shouldn't happen as store should be initialized when we get updated metrics
                return store;
            }
            const sqlWithNodeMetrics = updateSqlNodeMetrics(store.sql, action.sqlId, action.value)
            const sqlWithMetrics = calculateSqlQueryLevelMetrics(sqlWithNodeMetrics, store.jobs);
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
        default:
            // this shouldn't happen as we suppose to handle all actions
            return store;
    }
}
