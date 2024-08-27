import AccessTimeIcon from "@mui/icons-material/AccessTime";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import DirectionsRunIcon from "@mui/icons-material/DirectionsRun";
import HubIcon from "@mui/icons-material/Hub";
import MemoryIcon from "@mui/icons-material/Memory";
import ShuffleIcon from "@mui/icons-material/Shuffle";
import StorageIcon from "@mui/icons-material/Storage";
import SyncProblemIcon from "@mui/icons-material/SyncProblem";
import { Grid, Link, Typography } from "@mui/material";
import { duration } from "moment";
import React, { FC } from "react";
import "reactflow/dist/style.css";
import { useAppSelector } from "../Hooks";
import { humanFileSize, humanizeTimeDiff } from "../utils/FormatUtils";
import InfoBox from "./InfoBox/InfoBox";
import Progress from "./Progress";

const SummaryBar: FC = (): JSX.Element => {
  const status = useAppSelector((state) => state.spark.status);
  const alerts = useAppSelector((state) => state.spark.alerts);
  const environmentInfo = useAppSelector(
    (state) => state.spark.environmentInfo,
  );
  const executors = useAppSelector((state) => state.spark.executors);
  const memoryAlert = alerts?.alerts.find(
    (alert) =>
      (alert.source.type === "status" && alert.source.metric === "memory") ||
      (alert.source.type === "status" && alert.source.metric === "driverMemory")  );
  const wastedCoresAlert = alerts?.alerts.find(
    (alert) =>
      alert.source.type === "status" && alert.source.metric === "wastedCores",
  );
  const executorMemoryBytesString = useAppSelector(
    (state) => state.spark.config?.executorMemoryBytesString,
  );

  if (
    status?.executors === undefined ||
    status.stages === undefined ||
    executorMemoryBytesString === undefined
  ) {
    return <Progress />;
  }

  const durationText = humanizeTimeDiff(duration(status.duration));
  const driverExecutor = executors
    ? Object.values(executors).find((exec) => exec.id === "driver")
    : undefined;
  const driverMaxMemory = environmentInfo?.driverXmxBytes ?? 1;
  const driverMemoryUsage = driverExecutor?.memoryUsageBytes ?? 0;
  const driverMemoryUsagePercentage =
    (driverMemoryUsage / driverMaxMemory) * 100;
  const driverMemoryUsageString = humanFileSize(driverMemoryUsage);
  const driverMaxMemoryString = humanFileSize(driverMaxMemory);
  const totalDCUFormated =
    status.executors.totalDCU > 1
      ? status.executors.totalDCU.toFixed(2)
      : status.executors.totalDCU.toFixed(4);
  return (
    <Grid
      container
      spacing={3}
      sx={{ mt: 2, mb: 2 }}
      justifyContent="center"
      alignItems="center"
    >
      <Grid
        container
        item
        spacing={3}
        display="flex"
        justifyContent="center"
        alignItems="center"
      >
        <InfoBox
          title="Duration"
          text={durationText}
          color="#a31545"
          icon={AccessTimeIcon}
        ></InfoBox>
        <InfoBox
          title="DCU"
          text={totalDCUFormated}
          color="#795548"
          icon={HubIcon}
          tooltipContent={
            <React.Fragment>
              <Typography variant="h6" color="inherit">
                DataFlint Compute Units (DCU)
              </Typography>
              <Typography variant="subtitle2">
                Is measurement unit for spark usage, which is a simular concept
                to DBU (DataBricks Unit)
              </Typography>
              <Typography variant="subtitle2">
                It's calculated by: {totalDCUFormated} (DCU) =
              </Typography>
              <Typography variant="subtitle2">
                <b>{status.executors.totalCoreHour.toFixed(2)}</b> (core/hour
                usage) * 0.05 (core/hour ratio) +
              </Typography>
              <Typography variant="subtitle2">
                <b>{status.executors.totalMemoryGibHour.toFixed(2)}</b> (GiB
                memory/hour usage) * 0.005 (GiB memory/hour ratio)
              </Typography>
              <Typography variant="subtitle2">
                For more information see{" "}
                <Link
                  color="inherit"
                  href="https://dataflint.gitbook.io/dataflint-for-spark/advanced/dcu-calculation"
                >
                  documentation
                </Link>
              </Typography>
            </React.Fragment>
          }
        ></InfoBox>
        <InfoBox
          title="Input"
          text={status.executors.totalInputBytes}
          color="#26a69a"
          icon={ArrowDownwardIcon}
        ></InfoBox>
        <InfoBox
          title="Output"
          text={status.stages.totalOutput}
          color="#ffa726"
          icon={ArrowUpwardIcon}
        ></InfoBox>
        <InfoBox
          title="Memory Usage"
          text={status.executors.maxExecutorMemoryPercentage.toFixed(2) + "%"}
          color="#8e24aa"
          icon={MemoryIcon}
          alert={memoryAlert}
          tooltipContent={
            <React.Fragment>
              <Typography variant="subtitle1" color="inherit" fontWeight="bold">
                Peak Executor Memory:
              </Typography>
              <Typography variant="body1" color="inherit" textAlign={"center"}>
                {status.executors.maxExecutorMemoryBytesString} /{" "}
                {executorMemoryBytesString} (
                {status.executors.maxExecutorMemoryPercentage.toFixed(2)}%)
              </Typography>

              <Typography
                variant="subtitle1"
                color="inherit"
                fontWeight="bold"
                style={{ marginTop: "16px" }}
              >
                Peak Driver Memory:
              </Typography>
              <Typography variant="body1" color="inherit" textAlign={"center"}>
                {driverMemoryUsage > 0 ? (
                  <>
                    {driverMemoryUsageString} / {driverMaxMemoryString} (
                    {driverMemoryUsagePercentage.toFixed(2)}%)
                  </>
                ) : (
                  "N/A"
                )}{" "}
              </Typography>

              <Typography variant="body2" style={{ marginTop: "16px" }}>
                "Memory Usage" shows the peak memory usage of the most
                memory-utilized executor.
              </Typography>
              <Typography variant="body2">
                "Memory" refers to the JVM memory (both on-heap and off-heap).
              </Typography>
            </React.Fragment>
          }
        ></InfoBox>
      </Grid>
      <Grid
        container
        item
        spacing={3}
        display="flex"
        justifyContent="center"
        alignItems="center"
      >
        <InfoBox
          title="Shuffle Read"
          text={status.executors.totalShuffleRead}
          color="#827717"
          icon={ShuffleIcon}
        ></InfoBox>
        <InfoBox
          title="Shuffle Write"
          text={status.executors.totalShuffleWrite}
          color="#9e9d24"
          icon={ShuffleIcon}
        ></InfoBox>
        <InfoBox
          title="Spill To Disk"
          text={status.stages.totalDiskSpill}
          color="#e91e63"
          icon={StorageIcon}
          tooltipContent={
            <React.Fragment>
              <Typography variant="subtitle2">
                "Spilling" in spark is when the data does not fit in the
                executor memory and as a result it needs to be written to disk
              </Typography>
            </React.Fragment>
          }
        ></InfoBox>
        <InfoBox
          title="Wasted Cores"
          text={status.executors.wastedCoresRate.toFixed(2) + "%"}
          alert={wastedCoresAlert}
          color="#618833"
          icon={DirectionsRunIcon}
          tooltipContent={
            <React.Fragment>
              <Typography variant="h6" color="inherit">
                What is Wasted Cores Rate?
              </Typography>
              <Typography variant="subtitle2">
                Wasted Cores Rate is the percentage of time that your executors
                cores are idle.
              </Typography>
              <Typography variant="subtitle2">
                Core Idle Time is defined by an executor core without a
                scheduled task.
              </Typography>
              <Typography variant="subtitle2">
                For example: if you have 400 cores and 200 tasks scheduled your
                cores wasted rate is 50%.
              </Typography>
            </React.Fragment>
          }
        ></InfoBox>
        <InfoBox
          title="Task Error Rate"
          text={status.stages.taskErrorRate.toFixed(2) + "%"}
          color="#ff6e40"
          icon={SyncProblemIcon}
          tooltipContent={
            <React.Fragment>
              <Typography variant="h6" color="inherit" textAlign={"center"}>
                {status.stages.totalFailedTasks} / {status.stages.totalTasks}
              </Typography>
              <Typography variant="subtitle2">
                Number of failed tasks / Total number of tasks
              </Typography>
              <Typography variant="subtitle2">
                "Task Error Rate" is the percentage of tasks failed.
              </Typography>
            </React.Fragment>
          }
        ></InfoBox>
      </Grid>
    </Grid>
  );
};

export default SummaryBar;
