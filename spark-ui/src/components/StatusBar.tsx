import AccessTimeIcon from "@mui/icons-material/AccessTime";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import DatasetIcon from "@mui/icons-material/Dataset";
import HourglassBottomIcon from "@mui/icons-material/HourglassBottom";
import MemoryIcon from "@mui/icons-material/Memory";
import QueueIcon from "@mui/icons-material/Queue";
import WorkIcon from "@mui/icons-material/Work";
import { Grid, Typography } from "@mui/material";
import { duration } from "moment";
import React, { FC } from "react";
import "reactflow/dist/style.css";
import { useAppSelector } from "../Hooks";
import { SparkExecutorStore } from "../interfaces/AppStore"; // Adjust the import path as needed
import {
  calculatePercentage,
  humanFileSize,
  humanizeTimeDiff,
} from "../utils/FormatUtils";
import InfoBox from "./InfoBox/InfoBox";
import Progress from "./Progress";

const StatusBar: FC = (): JSX.Element => {
  const { status, sql } = useAppSelector((state) => state.spark);
  const alerts = useAppSelector((state) => state.spark.alerts);
  const environmentInfo = useAppSelector(
    (state) => state.spark.environmentInfo,
  );
  const executors = useAppSelector((state) => state.spark.executors);
  const memoryAlert = alerts?.alerts.find(
    (alert) =>
      (alert.source.type === "status" && alert.source.metric === "memory") ||
      (alert.source.type === "status" &&
        alert.source.metric === "driverMemory"),
  );
  const executorMemoryBytesString = useAppSelector(
    (state) => state.spark.config?.executorMemoryBytesString,
  );

  const currentSql =
    sql === undefined ? undefined : sql.sqls[sql.sqls.length - 1];
  const stagesStatus = status?.stages;
  const executorStatus = status?.executors;
  if (stagesStatus === undefined || executorStatus === undefined) {
    return <Progress />;
  }

  const numOfExecutorsText =
    executorStatus.numOfExecutors === 0
      ? "1 (driver)"
      : executorStatus.numOfExecutors.toString();

  const isIdle = stagesStatus.status == "idle";
  const idleTimeText = humanizeTimeDiff(duration(status?.sqlIdleTime), true);
  const queryRuntimeText = humanizeTimeDiff(
    duration(currentSql?.duration),
    true,
  );
  const durationText = humanizeTimeDiff(duration(status?.duration), true);
  const numOfQueries = sql?.sqls.length ?? 0;
  const driverExecutor = executors?.find(
    (exec: SparkExecutorStore) => exec.id === "driver",
  );
  const driverMaxMemory = environmentInfo?.driverXmxBytes;
  const driverMemoryUsage = driverExecutor?.HeapMemoryUsageBytes ?? 0;
  const isDriverMemoryAvailable =
    driverMaxMemory !== undefined && driverMaxMemory > 1;
  const driverMemoryUsagePercentage = isDriverMemoryAvailable
    ? calculatePercentage(driverMemoryUsage, driverMaxMemory)
    : 0;
  const driverMemoryUsageString = humanFileSize(driverMemoryUsage);
  const driverMaxMemoryString = isDriverMemoryAvailable
    ? humanFileSize(driverMaxMemory)
    : "N/A";
  return (
    <div>
      {isIdle ? (
        <Grid
          container
          spacing={3}
          sx={{ mt: 2, mb: 2 }}
          display="flex"
          justifyContent="center"
          alignItems="center"
        >
          <InfoBox
            title={isIdle ? "Idle Time" : "Query Time"}
            text={isIdle ? idleTimeText : queryRuntimeText}
            color="#7e57c2"
            icon={HourglassBottomIcon}
          ></InfoBox>
          <InfoBox
            title="Duration"
            text={durationText}
            color="#a31545"
            icon={AccessTimeIcon}
          ></InfoBox>
          <InfoBox
            title="Memory Usage"
            text={
              status?.executors?.maxExecutorMemoryPercentage.toFixed(2) + "%"
            }
            color="#8e24aa"
            alert={memoryAlert}
            icon={MemoryIcon}
            tooltipContent={
              <React.Fragment>
                <Typography
                  variant="subtitle1"
                  color="inherit"
                  fontWeight="bold"
                >
                  Peak Executor Memory:
                </Typography>
                <Typography
                  variant="body1"
                  color="inherit"
                  textAlign={"center"}
                >
                  {status?.executors?.maxExecutorMemoryBytesString} /{" "}
                  {executorMemoryBytesString} (
                  {status?.executors?.maxExecutorMemoryPercentage.toFixed(2)}%)
                </Typography>

                {isDriverMemoryAvailable ? (
                  <>
                    <Typography
                      variant="subtitle1"
                      color="inherit"
                      fontWeight="bold"
                      style={{ marginTop: "16px" }}
                    >
                      Peak Driver Memory:
                    </Typography>
                    <Typography
                      variant="body1"
                      color="inherit"
                      textAlign={"center"}
                    >
                      {driverMemoryUsageString} / {driverMaxMemoryString} (
                      {driverMemoryUsagePercentage.toFixed(2)}%)
                    </Typography>
                  </>
                ) : (
                  <Typography
                    variant="body2"
                    color="inherit"
                    style={{ marginTop: "16px" }}
                  >
                    Driver memory information is not available for this run.
                  </Typography>
                )}

                <Typography variant="body2" style={{ marginTop: "16px" }}>
                  "Memory Usage" shows the peak memory usage of the most
                  memory-utilized executor.
                </Typography>
                <Typography variant="body2">
                  "Memory" refers to the JVM memory (on-heap).
                </Typography>
              </React.Fragment>
            }
          />
          <InfoBox
            title="Executors"
            text={numOfExecutorsText}
            color="#52b202"
            icon={WorkIcon}
          ></InfoBox>
          <InfoBox
            title="SQL Queries"
            text={numOfQueries.toString()}
            color="#00838f"
            icon={DatasetIcon}
          ></InfoBox>
        </Grid>
      ) : (
        <Grid
          container
          spacing={3}
          sx={{ mt: 2, mb: 2 }}
          display="flex"
          justifyContent="center"
          alignItems="center"
        >
          <InfoBox
            title={isIdle ? "Idle Time" : "Query Time"}
            text={isIdle ? idleTimeText : queryRuntimeText}
            color="#7e57c2"
            icon={HourglassBottomIcon}
          ></InfoBox>
          {currentSql === undefined ||
          currentSql.stageMetrics === undefined ? null : (
            <InfoBox
              title="Query Input"
              text={humanFileSize(currentSql.stageMetrics.inputBytes)}
              color="#26a69a"
              icon={ArrowDownwardIcon}
            ></InfoBox>
          )}
          {currentSql === undefined ||
          currentSql.stageMetrics === undefined ? null : (
            <InfoBox
              title="Query Output"
              text={humanFileSize(currentSql.stageMetrics.outputBytes)}
              color="#ffa726"
              icon={ArrowUpwardIcon}
            ></InfoBox>
          )}
          <InfoBox
            title="Executors"
            text={numOfExecutorsText}
            color="#52b202"
            icon={WorkIcon}
          ></InfoBox>
          <InfoBox
            title="Pending Tasks"
            text={stagesStatus.totalPendingTasks.toString()}
            icon={QueueIcon}
            tooltipContent={
              <React.Fragment>
                <Typography variant="h6" color="inherit">
                  What is Pending Tasks?
                </Typography>
                <Typography variant="subtitle2">
                  Pending Tasks are the total number of tasks ready to be
                  executed on your cluster
                </Typography>
                <Typography variant="subtitle2">
                  For example: if you have 3 active stages, each with 1000
                  non-completed tasks, you have 3000 pending tasks
                </Typography>
              </React.Fragment>
            }
          ></InfoBox>
        </Grid>
      )}
    </div>
  );
};

export default StatusBar;
