import CheckIcon from "@mui/icons-material/Check";
import PendingIcon from "@mui/icons-material/Pending";
import {
  Box,
  CircularProgress,
  CircularProgressProps,
  Link,
  Tooltip,
  Typography,
} from "@mui/material";
import { duration } from "moment";
import React from "react";
import { useAppSelector } from "../../Hooks";
import {
  SparkStageStore,
  SQLNodeExchangeStageData,
  SQLNodeStageData,
} from "../../interfaces/AppStore";
import { humanFileSize, humanizeTimeDiff } from "../../utils/FormatUtils";
import { BASE_CURRENT_PAGE } from "../../utils/UrlConsts";
import { getBaseAppUrl } from "../../utils/UrlUtils";
import ExceptionIcon from "../ExceptionIcon";

function CircularProgressWithLabel(
  props: CircularProgressProps & { value: number },
) {
  return (
    <Box sx={{ position: "relative", display: "inline-flex" }}>
      <CircularProgress
        color="info"
        style={{ width: "30px", height: "30px" }}
        variant="determinate"
        {...props}
      />
      <Box
        sx={{
          top: 0,
          left: 0,
          bottom: 0,
          right: 0,
          position: "absolute",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <Typography
          variant="caption"
          component="div"
          color="GrayText"
        >{`${Math.round(props.value)}%`}</Typography>
      </Box>
    </Box>
  );
}

const getStatusIcon = (
  status: string,
  failureReason: string | undefined,
  progress: number | undefined,
): JSX.Element => {
  switch (status) {
    case "ACTIVE":
      return <CircularProgressWithLabel value={progress ?? 0} />;
    case "COMPLETE":
      return (
        <CheckIcon color="success" style={{ width: "30px", height: "30px" }} />
      );
    case "FAILED":
      return <ExceptionIcon failureReason={failureReason ?? ""} />;
    case "PENDING":
      return (
        <PendingIcon
          sx={{ color: "#b2a300" }}
          style={{ width: "30px", height: "30px" }}
        />
      );
    default:
      return <div></div>;
  }
};

const linkToStage = (stageId: number) => {
  window.open(
    `${getBaseAppUrl(BASE_CURRENT_PAGE)}/stages/stage/?id=${stageId}&attempt=0`, // TODO: fetch attempts from store config
    "_blank",
  );
};

function MultiStageIconTooltip({
  stage,
}: {
  stage: SQLNodeExchangeStageData;
}): JSX.Element {
  const stages = useAppSelector((state) => state.spark.stages);
  const readStage = stages?.find(
    (currentStage) => stage.readStage === currentStage.stageId,
  );
  const writeStage = stages?.find(
    (currentStage) => stage.writeStage === currentStage.stageId,
  );
  if (readStage === undefined || writeStage === undefined) return <div></div>;

  return (
    <Tooltip
      sx={{ zIndex: 6 }}
      title={
        <React.Fragment>
          <Box sx={{ m: 1 }} display="flex" gap={1}>
            <Link
              color="inherit"
              onClick={(evn) => linkToStage(stage.readStage)}
            >
              Read Stage - {stage.readStage}
            </Link>
            <Link
              color="inherit"
              onClick={(evn) => linkToStage(stage.writeStage)}
            >
              Write Stage - {stage.writeStage}
            </Link>
          </Box>
          <Box>
            <Typography variant="h6">Read Stage</Typography>
            <StageSummary stageData={readStage} />
          </Box>
          <Box>
            <Typography variant="h6">Write Stage</Typography>
            <StageSummary stageData={writeStage} />
          </Box>
        </React.Fragment>
      }
    >
      {getStatusIcon(
        stage.status,
        readStage.failureReason ?? writeStage.failureReason,
        (readStage.stageProgress ?? 0) + (writeStage.stageProgress ?? 0) / 2,
      )}
    </Tooltip>
  );
}

function SingleStageIconTooltip({
  stage,
}: {
  stage: SQLNodeStageData;
}): JSX.Element {
  const stages = useAppSelector((state) => state.spark.stages);
  const stageData = stages?.find(
    (currentStage) =>
      stage.type === "onestage" && stage.stageId === currentStage.stageId,
  );
  if (stageData === undefined) return <div></div>;

  return (
    <Tooltip
      sx={{ zIndex: 6 }}
      title={
        <React.Fragment>
          <Box sx={{ m: 1 }} display="flex" flexDirection="column" gap={1}>
            <Link color="inherit" onClick={(evn) => linkToStage(stage.stageId)}>
              Stage {stage.stageId}
            </Link>
            <StageSummary stageData={stageData} />
            {/* {stageData.durationDistribution !== undefined ? <DurationDistributionChart durationDist={stageData.durationDistribution} /> : undefined} */}
          </Box>
        </React.Fragment>
      }
    >
      {getStatusIcon(
        stage.status,
        stageData?.failureReason,
        stageData?.stageProgress,
      )}
    </Tooltip>
  );
}

function StageSummary({
  stageData,
}: {
  stageData: SparkStageStore;
}): JSX.Element {
  const executors = useAppSelector((state) => state.spark.executors);
  if (executors === undefined || executors.length === 0) return <div></div>;

  const maxTasks = executors[0].maxTasks;

  return (
    <Box>
      <Typography variant="body2">
        <strong>Tasks number: </strong> {stageData.numTasks}
      </Typography>
      <Typography variant="body2">
        <strong>Total executors time:</strong>{" "}
        {humanizeTimeDiff(
          duration(stageData.metrics.executorRunTime / maxTasks),
        )}
      </Typography>
      <Typography variant="body2">
        <strong>Median task duration:</strong>{" "}
        {humanizeTimeDiff(
          duration(
            stageData.durationDistribution !== undefined
              ? stageData.durationDistribution[5]
              : 0,
          ),
        )}
      </Typography>
      {stageData.metrics.inputBytes !== 0 ? (
        <Typography variant="body2">
          <strong>Median task input size:</strong>{" "}
          {humanFileSize(
            stageData.inputDistribution !== undefined
              ? stageData.inputDistribution[5]
              : 0,
          )}
        </Typography>
      ) : undefined}
      {stageData.metrics.outputBytes !== 0 ? (
        <Typography variant="body2">
          <strong>Median task output size:</strong>{" "}
          {humanFileSize(
            stageData.outputDistribution !== undefined
              ? stageData.outputDistribution[5]
              : 0,
          )}
        </Typography>
      ) : undefined}
      {stageData.metrics.shuffleReadBytes !== 0 ? (
        <Typography variant="body2">
          <strong>Median task shuffle read size:</strong>{" "}
          {humanFileSize(
            stageData.shuffleReadDistribution !== undefined
              ? stageData.shuffleReadDistribution[5]
              : 0,
          )}
        </Typography>
      ) : undefined}
      {stageData.metrics.shuffleWriteBytes !== 0 ? (
        <Typography variant="body2">
          <strong>Median task shuffle write size:</strong>{" "}
          {humanFileSize(
            stageData.shuffleWriteDistribution !== undefined
              ? stageData.shuffleWriteDistribution[5]
              : 0,
          )}
        </Typography>
      ) : undefined}
    </Box>
  );
}

export default function StageIconTooltip({
  stage,
}: {
  stage: SQLNodeStageData | SQLNodeExchangeStageData | undefined;
}): JSX.Element {
  if (stage === undefined) return <div></div>;

  return stage.type === "onestage" ? (
    <SingleStageIconTooltip stage={stage} />
  ) : (
    <MultiStageIconTooltip stage={stage} />
  );
}
