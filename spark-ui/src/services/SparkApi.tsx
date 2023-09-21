import { number } from "yargs";
import { ApiAction } from "../interfaces/APIAction";
import { SparkApplications } from "../interfaces/SparkApplications";
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
    appId: string = "";
    apiPath: string;
    applicationsPath: string;
    dispatch: React.Dispatch<ApiAction>;
    lastCompletedSqlId: number = -1;
    pollingStopped: boolean = false;

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

    constructor(basePath: string, dispatch: React.Dispatch<ApiAction>) {
        this.basePath = basePath;
        this.apiPath = `${basePath}/api/v1`;
        this.applicationsPath = `${this.apiPath}/applications`;
        this.dispatch = dispatch
    }

    start(): () => void {
        this.fetchData();
        return () => this.pollingStopped = true;
    }



    async fetchData(): Promise<void> {
        if (document.hidden || this.pollingStopped) {
            // skip fetching when tab is not in focus
            // TODO: skip also the interval when tab is not in focus
            return;
        }
        try {
            if (!this.initialized) {
                const appData: SparkApplications = await (await fetch(this.applicationsPath)).json();
                const currentApplication = appData[0];
                this.appId = currentApplication.id;
                const currentAttempt = currentApplication.attempts[currentApplication.attempts.length - 1];

                const sparkConfiguration: SparkConfiguration = await (await fetch(this.environmentPath)).json();
                this.initialized = true; // should happen after fetching app and env succesfully
                this.dispatch({type: 'setInitial', config: sparkConfiguration, appId: this.appId, attempt: currentAttempt, epocCurrentTime: Date.now() });
            }

            const sparkStages: SparkStages = await (await fetch(this.stagesPath)).json();
            this.dispatch({ type: 'setStages', value: sparkStages });

            const sparkExecutors: SparkExecutors = await (await fetch(this.executorsPath)).json();
            this.dispatch({ type: 'setSparkExecutors', value: sparkExecutors, epocCurrentTime: Date.now() });

            const sparkJobs: SparkJobs = await (await fetch(this.jobsPath)).json();
            this.dispatch({ type: 'setSparkJobs', value: sparkJobs });

            const sparkSQLs: SparkSQLs = await (await fetch(this.buildSqlPath(this.lastCompletedSqlId + 1))).json();
            if (sparkSQLs.length !== 0) {
                this.dispatch({ type: 'setSQL', value: sparkSQLs });

                const finishedSqls = sparkSQLs.filter(sql => sql.status === SqlStatus.Completed || sql.status === SqlStatus.Failed);

                if (finishedSqls.length > 0) {
                    this.lastCompletedSqlId = Math.max(...finishedSqls.map(sql => parseInt(sql.id)));
                }

                const runningSqlIds = sparkSQLs.filter(sql => sql.status === SqlStatus.Running).map(sql => sql.id)
                if (runningSqlIds.length !== 0) {
                    const sqlId = runningSqlIds[0];
                    const nodesMetrics: NodesMetrics = await (await fetch(this.getSqlMetricsPath(sqlId))).json();
                    this.dispatch({ type: 'setSQMetrics', value: nodesMetrics, sqlId: sqlId });
                }
            }

            this.dispatch({ type: 'updateDuration', epocCurrentTime: Date.now() });
        } catch (e) {
            console.log(e);
        }
        finally {
            if (!this.pollingStopped)
                setTimeout(this.fetchData.bind(this), POLL_TIME);
        }
    }
}

export default SparkAPI;