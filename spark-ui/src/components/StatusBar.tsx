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
import { humanFileSize, humanizeTimeDiff } from "../utils/FormatUtils";
import InfoBox from "./InfoBox/InfoBox";
import Progress from "./Progress";

const StatusBar: FC = (): JSX.Element => {
  const { status, sql } = useAppSelector((state) => state.spark);
  const alerts = useAppSelector((state) => state.spark.alerts);
  const memoryAlert = alerts?.alerts.find(
    (alert) =>
      alert.source.type === "status" && alert.source.metric === "memory",
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
                <Typography variant="h6" color="inherit" textAlign={"center"}>
                  {status?.executors?.maxExecutorMemoryBytesString} /{" "}
                  {executorMemoryBytesString}
                </Typography>
                <Typography variant="subtitle2">
                  Peak JVM memory usage / total executor JVM memory
                </Typography>
                <Typography variant="subtitle2">
                  "Memory Usage" is the peak memory usage of the most
                  memory-utilized executor.
                </Typography>
                <Typography variant="subtitle2">
                  "Executor Memory" refers to the executor JVM memory (both
                  on-heap and off-heap).
                </Typography>
              </React.Fragment>
            }
          ></InfoBox>
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
