import AccessTimeIcon from "@mui/icons-material/AccessTime";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import DirectionsRunIcon from "@mui/icons-material/DirectionsRun";
import HubIcon from "@mui/icons-material/Hub";
import MemoryIcon from "@mui/icons-material/Memory";
import ShuffleIcon from "@mui/icons-material/Shuffle";
import StorageIcon from "@mui/icons-material/Storage";
import SyncProblemIcon from "@mui/icons-material/SyncProblem";
import { Grid, Typography } from "@mui/material";
import { duration } from "moment";
import React, { FC } from "react";
import "reactflow/dist/style.css";
import { useAppSelector } from "../Hooks";
import { humanizeTimeDiff } from "../utils/FormatUtils";
import InfoBox from "./InfoBox/InfoBox";
import Progress from "./Progress";

const SummaryBar: FC = (): JSX.Element => {
  const status = useAppSelector((state) => state.spark.status);
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
          title="Core Hour"
          text={status.executors.totalCoreHour.toFixed(2)}
          color="#795548"
          icon={HubIcon}
          tooltipContent={
            <React.Fragment>
              <Typography variant="h6" color="inherit">
                What is Core Hour?
              </Typography>
              <Typography variant="subtitle2">
                Core Hour is the time where cores were allocated for your app in
                hours unit
              </Typography>
              <Typography variant="subtitle2">
                For example: if you app allocated 6 cores for 30 minutes you
                used 3 Core Hour
              </Typography>
            </React.Fragment>
          }
        ></InfoBox>
        <InfoBox
          title="Input"
          text={status.stages.totalInput}
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
          tooltipContent={
            <React.Fragment>
              <Typography variant="h6" color="inherit" textAlign={"center"}>
                {status.executors.maxExecutorMemoryBytesString} / {executorMemoryBytesString}
              </Typography>
              <Typography variant="subtitle2">
                The peak memory usage of the most memory-utilized executor.
              </Typography>
              <Typography variant="subtitle2">
                "Executor Memory" refers to the executor JVM memory (both on-heap and off-heap).
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
          text={status.stages.totalShuffleRead}
          color="#827717"
          icon={ShuffleIcon}
        ></InfoBox>
        <InfoBox
          title="Shuffle Write"
          text={status.stages.totalShuffleWrite}
          color="#9e9d24"
          icon={ShuffleIcon}
        ></InfoBox>
        <InfoBox
          title="Spill To Disk"
          text={status.stages.totalDiskSpill}
          color="#e91e63"
          icon={StorageIcon}
        ></InfoBox>
        <InfoBox
          title="Activity Rate"
          text={status.executors.activityRate.toFixed(2) + "%"}
          color="#618833"
          icon={DirectionsRunIcon}
          tooltipContent={
            <React.Fragment>
              <Typography variant="h6" color="inherit">
                What is Activity Rate?
              </Typography>
              <Typography variant="subtitle2">
                Activity rate is the percentage of time that your executors
                cores are not idle.
              </Typography>
              <Typography variant="subtitle2">
                Core Idle Time is defined by an executor's core without a
                scheduled task.
              </Typography>
              <Typography variant="subtitle2">
                For example: if you have 400 cores and 200 tasks scheduled your
                activity rate is 50%.
              </Typography>
            </React.Fragment>
          }
        ></InfoBox>
        <InfoBox
          title="Task Error Rate"
          text={status.stages.taskErrorRate.toFixed(2) + "%"}
          color="#ff6e40"
          icon={SyncProblemIcon}
        ></InfoBox>
      </Grid>
    </Grid>
  );
};

export default SummaryBar;
