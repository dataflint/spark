import ApiIcon from "@mui/icons-material/Api";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import QueueIcon from "@mui/icons-material/Queue";
import WorkIcon from "@mui/icons-material/Work";
import { Grid, Typography } from "@mui/material";
import React, { FC } from "react";
import "reactflow/dist/style.css";
import { useAppSelector } from "../Hooks";
import { humanFileSize } from "../utils/FormatUtils";
import InfoBox from "./InfoBox/InfoBox";
import Progress from "./Progress";

const StatusBar: FC = (): JSX.Element => {
  const { status, sql } = useAppSelector((state) => state.spark);

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
        title="Status"
        text={stagesStatus.status}
        color="#7e57c2"
        icon={ApiIcon}
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
        tooltipContent={<React.Fragment>
          <Typography variant="h6" color="inherit">What is Pending Tasks?</Typography>
          <Typography variant="subtitle2">
            Pending Tasks are the total number of tasks react to be executed on your cluster
          </Typography>
          <Typography variant="subtitle2">
            For example: if you have 3 active stages, each with 1000 non-completed tasks, you have 3000 pending tasks
          </Typography>
        </React.Fragment>}
      ></InfoBox>
    </Grid>
  );
};

export default StatusBar;
