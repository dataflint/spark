import AccessTimeIcon from "@mui/icons-material/AccessTime";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import DirectionsRunIcon from "@mui/icons-material/DirectionsRun";
import MemoryIcon from "@mui/icons-material/Memory";
import { Grid } from "@mui/material";
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
      ></InfoBox>
    </Grid>
  );
};

export default SummaryBar;
