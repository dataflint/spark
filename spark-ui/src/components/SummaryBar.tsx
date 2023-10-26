import AccessTimeIcon from "@mui/icons-material/AccessTime";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import DirectionsRunIcon from "@mui/icons-material/DirectionsRun";
import MemoryIcon from "@mui/icons-material/Memory";
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

  if (status?.executors === undefined || status.stages === undefined) {
    return <Progress />;
  }

  const durationText = humanizeTimeDiff(duration(status.duration));

  return (
    <Grid
      container
      spacing={3}
      sx={{ mt: 2, mb: 2 }}
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
        icon={MemoryIcon}
        tooltipContent={<React.Fragment>
          <Typography variant="h6" color="inherit">What is Core Hour?</Typography>
          <Typography variant="subtitle2">
            Core Hour is the time where cores were allocated for your app in hours unit
          </Typography>
          <Typography variant="subtitle2">
            For example: if you app allocated 6 cores for 30 minutes you used 3 Core Hour
          </Typography>
        </React.Fragment>}
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
        title="Activity Rate"
        text={status.executors.activityRate.toFixed(2) + "%"}
        color="#618833"
        icon={DirectionsRunIcon}
        tooltipContent={<React.Fragment>
          <Typography variant="h6" color="inherit">What is Activity Rate?</Typography>
          <Typography variant="subtitle2">
            Activity rate is the percentage of time that your executors cores are not idle.
          </Typography>
          <Typography variant="subtitle2">
            Core Idle Time is defined by an executor's core without a scheduled task.
          </Typography>
          <Typography variant="subtitle2">
            For example: if you have 400 cores and 200 tasks scheduled your activity rate is 50%.
          </Typography>
        </React.Fragment>}
      ></InfoBox>
    </Grid>
  );
};

export default SummaryBar;
