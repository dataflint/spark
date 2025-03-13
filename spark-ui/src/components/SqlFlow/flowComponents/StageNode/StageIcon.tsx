import CheckIcon from "@mui/icons-material/Check";
import PendingIcon from "@mui/icons-material/Pending";
import {
  Box,
  CircularProgress,
  CircularProgressProps,
  Typography
} from "@mui/material";
import React from "react";
import { useAppSelector } from "../../Hooks";
import {
  SQLNodeExchangeStageData,
  SQLNodeStageData
} from "../../interfaces/AppStore";
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
    getStatusIcon(
      stage.status,
      readStage.failureReason ?? writeStage.failureReason,
      (readStage.stageProgress ?? 0) + (writeStage.stageProgress ?? 0) / 2,
    )
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
    getStatusIcon(
      stage.status,
      stageData?.failureReason,
      stageData?.stageProgress,
    ))
}

export default function StageIcon({
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
