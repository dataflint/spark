import { number } from "yargs";
import { ApiAction } from "../interfaces/APIAction";
import { SparkApplication, SparkApplications } from "../interfaces/SparkApplications";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkExecutors } from "../interfaces/SparkExecutors";
import { SparkJobs } from "../interfaces/SparkJobs";
import { SparkSQLs, SqlStatus } from '../interfaces/SparkSQLs';
import { SparkStages } from "../interfaces/SparkStages";
import { NodesMetrics } from '../interfaces/SqlMetrics';

const POLL_TIME = 1000
const SQL_QUERY_LENGTH = 100

class SparkAPI {
    basePath: string
    initialized: boolean = false;
    isConnected: boolean = false;
    appId: string = "";
    apiPath: string;
    applicationsPath: string;
    dispatch: React.Dispatch<ApiAction>;
    lastCompletedSqlId: number = -1;
    pollingStopped: boolean = false;
    historyServerMode: boolean = false;

    private get applicationPath(): string {
        return `${this.apiPath}/applications/${this.appId}`
    }

    private get environmentPath(): string {
        return `${this.applicationPath}/environment`
    }

    private get stagesPath(): string {
        return `${this.applicationPath}/stages`
    }

    private getSqlMetricsPath(sqlId: string): string {
        return `${this.applicationPath}/devtool/sql/${sqlId}`
    }

    private buildSqlPath(offset: number): string {
        return `${this.applicationPath}/sql?offset=${offset}&length=${SQL_QUERY_LENGTH}`
    }

    private get executorsPath(): string {
        return `${this.applicationPath}/allexecutors`
    }

    private get jobsPath(): string {
        return `${this.applicationPath}/jobs`
    }

    private resetState(): void {
        this.lastCompletedSqlId = -1;
        this.appId = "";
    }

    constructor(basePath: string, dispatch: React.Dispatch<ApiAction>, historyServerMode: boolean = false) {
        this.basePath = basePath;
        this.apiPath = `${basePath}/api/v1`;
        this.applicationsPath = `${this.apiPath}/applications`;
        this.dispatch = dispatch
        this.historyServerMode = historyServerMode;
    }

    start(): () => void {
        this.fetchData();
        return () => this.pollingStopped = true;
    }

    private getCurrentApp(appData: SparkApplications): SparkApplication {
        if (this.historyServerMode) {
            const urlSegments = window.location.href.split('/');
            try {
                const historyIndex = urlSegments.findIndex(segment => segment === 'history');
                const appId = urlSegments[historyIndex + 1];
                const app = appData.find((app) => app.id === appId);
                if (!app) {
                    throw new Error();
                }

                return app;
            }
            catch {
                throw new Error("Invalid app id");
            }
        }

        return appData[0];
    }

    async queryData(path: string): Promise<any> {
        try {
            const requestContent = await fetch(path)
            return await requestContent.json();
        } catch (e) {
            console.log(`request to path: ${path} failed, error: ${e}`);
            throw(e);
        }
    }

    async fetchData(): Promise<void> {
        try {
            if (document.hidden || this.pollingStopped) {
                // skip fetching when tab is not in focus
                // TODO: skip also the interval when tab is not in focus
                return;
            }
            if (!this.initialized || !this.isConnected) {
                this.resetState(); // In case of disconnection
                console.log('asking for applications')
                const appData: SparkApplications = await this.queryData(this.applicationsPath);
                console.log('app response:', appData);
            
                const currentApplication = this.getCurrentApp(appData);
                this.appId = currentApplication.id;
                const currentAttempt = currentApplication.attempts[currentApplication.attempts.length - 1];

                const sparkConfiguration: SparkConfiguration = await this.queryData(this.environmentPath);
                this.initialized = true; // should happen after fetching app and env succesfully
                this.isConnected = true;
                this.dispatch({ type: 'setInitial', config: sparkConfiguration, appId: this.appId, attempt: currentAttempt, epocCurrentTime: Date.now() });
            } else {
                this.dispatch({ type: 'updateDuration', epocCurrentTime: Date.now() });
            }

            const sparkStages: SparkStages =  await this.queryData(this.stagesPath);
            this.dispatch({ type: 'setStages', value: sparkStages });

            const sparkExecutors: SparkExecutors = await this.queryData(this.executorsPath);
            this.dispatch({ type: 'setSparkExecutors', value: sparkExecutors });

            const sparkJobs: SparkJobs = await this.queryData(this.jobsPath);
            this.dispatch({ type: 'setSparkJobs', value: sparkJobs });

            const sparkSQLs: SparkSQLs = await this.queryData(this.buildSqlPath(this.lastCompletedSqlId + 1));
            if (sparkSQLs.length !== 0) {
                this.dispatch({ type: 'setSQL', value: sparkSQLs });

                const finishedSqls = sparkSQLs.filter(sql => sql.status === SqlStatus.Completed || sql.status === SqlStatus.Failed);

                if (finishedSqls.length > 0) {
                    this.lastCompletedSqlId = Math.max(...finishedSqls.map(sql => parseInt(sql.id)));
                }

                const runningSqlIds = sparkSQLs.filter(sql => sql.status === SqlStatus.Running).map(sql => sql.id)
                if (runningSqlIds.length !== 0) {
                    const sqlId = runningSqlIds[0];
                    const nodesMetrics: NodesMetrics = await this.queryData(this.getSqlMetricsPath(sqlId));
                    this.dispatch({ type: 'setSQLMetrics', value: nodesMetrics, sqlId: sqlId });
                }
            }
            this.dispatch({ type: 'calculateSqlQueryLevelMetrics' });
        } catch (e) {
            this.isConnected = false;
            this.dispatch({ type: 'updateConnection', isConnected: false });
            console.log(e);
        }
        finally {
            if (!this.pollingStopped && !this.historyServerMode)
                setTimeout(this.fetchData.bind(this), POLL_TIME);
        }
    }
}

export default SparkAPI;