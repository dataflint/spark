import { ApplicationInfo } from "../interfaces/ApplicationInfo";
import { MixpanelEvents } from "../interfaces/Mixpanel";
import { SparkConfiguration } from "../interfaces/SparkConfiguration";
import { SparkExecutors } from "../interfaces/SparkExecutors";
import { SparkJobs } from "../interfaces/SparkJobs";
import { SparkSQLs, SqlStatus } from "../interfaces/SparkSQLs";
import { SparkStages } from "../interfaces/SparkStages";
import { NodesMetrics } from "../interfaces/SqlMetrics";
import { SQLPlans } from "../interfaces/SQLPlan";
import { StagesRdd } from "../interfaces/StagesRdd";
import {
  onCycleEnd,
  setInitial,
  setSparkExecutors,
  setSparkJobs,
  setSQL,
  setSQLMetrics,
  setStages,
  updateConnection,
  updateDuration,
} from "../reducers/SparkSlice";
import { AppDispatch } from "../Store";
import { IS_HISTORY_SERVER_MODE } from "../utils/UrlConsts";
import { isDataFlintSaaSUI, isYarnMode } from "../utils/UrlUtils";
import { MixpanelService } from "./MixpanelService";

const POLL_TIME = 1000;
const SQL_QUERY_LENGTH = 1000;

class SparkAPI {
  basePath: string;
  baseCurrentPage: string;
  initialized: boolean = false;
  isConnected: boolean = false;
  appId: string = "";
  attemptId: string | undefined = undefined;
  apiPath: string;
  applicationsPath: string;
  dispatch: AppDispatch;
  lastCompletedSqlId: number = -1;
  pollingStopped: boolean = false;
  historyServerMode: boolean = false;

  private get applicationPath(): string {
    return `${this.apiPath}/applications/${this.appId}` + (this.attemptId !== undefined ? `/${this.attemptId}` : "");
  }

  private get environmentPath(): string {
    return `${this.applicationPath}/environment`;
  }

  private get stagesPath(): string {
    return `${this.applicationPath}/stages?withSummaries=true`;
  }

  private getSqlMetricsPath(sqlId: string): string {
    return `${this.baseCurrentPage}/sqlmetrics/json/?executionId=${sqlId}`;
  }

  private buildSqlPath(offset: number): string {
    return `${this.applicationPath}/sql?offset=${offset}&length=${SQL_QUERY_LENGTH}&planDescription=false`;
  }

  private buildSqlPlanPath(offset: number): string {
    return `${this.baseCurrentPage}/sqlplan/json/?offset=${offset}&length=${SQL_QUERY_LENGTH}`;
  }

  private buildStageRdd(): string {
    return `${this.baseCurrentPage}/stagesrdd/json/`;
  }
  private applicationinfoPath(): string {
    return `${this.baseCurrentPage}/applicationinfo/json/`;
  }

  private get executorsPath(): string {
    return `${this.applicationPath}/allexecutors`;
  }

  private get jobsPath(): string {
    return `${this.applicationPath}/jobs`;
  }

  private resetState(): void {
    this.lastCompletedSqlId = -1;
    this.appId = "";
  }

  constructor(
    basePath: string,
    baseCurrentPage: string,
    dispatch: AppDispatch,
    historyServerMode: boolean = false,
  ) {
    this.basePath = basePath;
    this.baseCurrentPage = baseCurrentPage;
    this.apiPath = `${basePath}/api/v1`;
    this.applicationsPath = `${this.apiPath}/applications`;
    this.dispatch = dispatch;
    this.historyServerMode = historyServerMode;
  }

  start(): () => void {
    this.fetchData();
    return () => (this.pollingStopped = true);
  }

  private getPlatform(config: SparkConfiguration): string {
    if (isDataFlintSaaSUI()) {
      return "dataflint_saas";
    }
    else if (IS_HISTORY_SERVER_MODE) {
      return "history_server";
    }
    const databricksConf = config.sparkProperties.find(
      (conf) =>
        conf.length > 1 &&
        conf[0] === "spark.databricks.clusterUsageTags.cloudProvider",
    );
    if (databricksConf !== undefined) {
      return "databricks";
    }

    const masterConfig = config.sparkProperties.find(
      (conf) => conf.length > 1 && conf[0] === "spark.master",
    );
    if (masterConfig === undefined || masterConfig.length !== 2) {
      return "unknown";
    }

    const sparkMaster = masterConfig[1];
    if (sparkMaster.startsWith("local")) {
      return "local";
    } else if (sparkMaster.startsWith("spark://")) {
      return "standalone";
    } else if (sparkMaster.startsWith("yarn")) {
      return "yarn";
    } else if (sparkMaster.startsWith("k8s://")) {
      return "k8s";
    }

    return "unknown";
  }

  async queryData(path: string): Promise<any> {
    try {
      const requestContent = await fetch(path);
      return await requestContent.json();
    } catch (e) {
      console.log(`request to path: ${path} failed, error: ${e}`);
      throw e;
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
        const appInfo: ApplicationInfo = await this.queryData(
          this.applicationinfoPath(),
        );

        const currentApplication = appInfo.info
        this.appId = isDataFlintSaaSUI() && appInfo.runId ? appInfo.runId : currentApplication.id;
        const currentAttempt =
          currentApplication.attempts[currentApplication.attempts.length - 1];
        this.attemptId = isDataFlintSaaSUI() ? undefined : (currentAttempt?.attemptId !== undefined ? currentAttempt.attemptId : undefined);
        this.attemptId = isYarnMode() ? undefined : this.attemptId;
        const sparkConfiguration: SparkConfiguration = await this.queryData(
          this.environmentPath,
        );
        this.initialized = true; // should happen after fetching app and env succesfully
        this.isConnected = true;

        const masterConfig = sparkConfiguration.sparkProperties.find(
          (conf) => conf.length > 1 && conf[0] === "spark.dataflint.telemetry.enabled",
        );

        if (masterConfig !== undefined && masterConfig[1] === "false") {
          MixpanelService.setMixpanelTelemetryConfigDisabled();
          console.log("skipping mixpanel telemetry, spark.dataflint.telemetry.enabled is set to false")
        } else {
          MixpanelService.InitMixpanel();
          MixpanelService.Track(MixpanelEvents.SparkAppInitilized, {
            sparkVersion: currentAttempt?.appSparkVersion,
            duration: currentAttempt?.duration,
            platform: this.getPlatform(sparkConfiguration),
          });
        }
        this.dispatch(
          setInitial({
            config: sparkConfiguration,
            appId: currentApplication.id,
            attempt: currentAttempt,
            epocCurrentTime: Date.now(),
          }),
        );

      } else {
        this.dispatch(updateDuration({ epocCurrentTime: Date.now() }));
      }

      const stagesRdd: StagesRdd = await this.queryData(this.buildStageRdd());
      const sparkStages: SparkStages = await this.queryData(this.stagesPath);
      this.dispatch(setStages({ value: sparkStages, stagesRdd: stagesRdd }));

      const sparkExecutors: SparkExecutors = await this.queryData(
        this.executorsPath,
      );
      this.dispatch(setSparkExecutors({ value: sparkExecutors }));

      const sparkJobs: SparkJobs = await this.queryData(this.jobsPath);
      this.dispatch(setSparkJobs({ value: sparkJobs }));

      const sparkSQLs: SparkSQLs = await this.queryData(
        this.buildSqlPath(this.lastCompletedSqlId + 1),
      );
      const sparkPlans: SQLPlans = await this.queryData(
        this.buildSqlPlanPath(this.lastCompletedSqlId + 1),
      );

      if (sparkSQLs.length !== 0) {
        this.dispatch(setSQL({ sqls: sparkSQLs, plans: sparkPlans }));

        const finishedSqls = sparkSQLs.filter(
          (sql) =>
            sql.status === SqlStatus.Completed ||
            sql.status === SqlStatus.Failed,
        );

        if (finishedSqls.length > 0) {
          // in cases of SQLs out of order, like id 2 is running and 3 is completed, we will try to ask from id 2 again
          finishedSqls.forEach((sql) => {
            if (parseInt(sql.id) === this.lastCompletedSqlId + 1) {
              this.lastCompletedSqlId += 1;
            }
          });
        }

        const runningSqlIds = sparkSQLs
          .filter((sql) => sql.status === SqlStatus.Running)
          .map((sql) => sql.id);
        if (runningSqlIds.length !== 0) {
          const sqlId = runningSqlIds.slice(-1)[0];
          const nodesMetrics: NodesMetrics = await this.queryData(
            this.getSqlMetricsPath(sqlId),
          );
          this.dispatch(setSQLMetrics({ value: nodesMetrics, sqlId: sqlId }));
        }
      }
      this.dispatch(onCycleEnd());
    } catch (e) {
      console.log(e);
      this.isConnected = false;
      this.dispatch(updateConnection({ isConnected: false }));
    } finally {
      if (!this.pollingStopped && !this.historyServerMode)
        setTimeout(this.fetchData.bind(this), POLL_TIME);
    }
  }
}

export default SparkAPI;
