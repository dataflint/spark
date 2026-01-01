import CheckIcon from "@mui/icons-material/Check";
import ErrorIcon from "@mui/icons-material/Error";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import PendingIcon from "@mui/icons-material/Pending";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import WarningIcon from "@mui/icons-material/Warning";
import { Alert, AlertTitle, Box, CircularProgress, IconButton, Tooltip, Typography } from "@mui/material";
import { duration } from "moment";
import React, { FC, memo, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { NodeProps } from "reactflow";
import { useAppDispatch, useAppSelector } from "../../Hooks";
import { SparkStageStore } from "../../interfaces/AppStore";
import { setSelectedStage } from "../../reducers/GeneralSlice";
import { humanizeTimeDiff } from "../../utils/FormatUtils";
import { TransperantTooltip } from "../AlertBadge/AlertBadge";
import ExceptionIcon from "../ExceptionIcon";
import styles from "./node-style.module.css";
import { getBucketedColor } from "./PerformanceIndicator";

export const StageGroupNodeName: string = "stageGroupNode";

export interface StageGroupData {
    stageId: number;
    stageInfo?: SparkStageStore;
    status: string;
    stageDuration?: number;
    nodeCount: number;
    attemptCount?: number;
    durationPercentage?: number;
    sqlId?: string;
    alerts?: { id: string; title: string; message: string; type: "warning" | "error"; shortSuggestion?: string }[];
}

interface StageGroupNodeProps extends NodeProps<StageGroupData> { }

// Get status tooltip text
const getStatusTooltip = (status: string): string => {
    switch (status) {
        case "ACTIVE":
            return "Stage is currently running";
        case "COMPLETE":
            return "Stage completed successfully";
        case "COMPLETE_WITH_RETRIES":
            return "Stage completed with task retries";
        case "FAILED":
            return "Stage failed with an error";
        case "PENDING":
            return "Stage is waiting to start";
        default:
            return "Unknown status";
    }
};

// Task progress indicator component
interface TaskProgressProps {
    numTasks: number;
    numCompleteTasks: number;
    numActiveTasks: number;
    numFailedTasks: number;
    status: string;
    compact?: boolean;
}

const TaskProgressIndicator: FC<TaskProgressProps> = ({
    numTasks,
    numCompleteTasks,
    numActiveTasks,
    numFailedTasks,
    status,
    compact = false,
}) => {
    const progress = numTasks > 0 ? (numCompleteTasks / numTasks) * 100 : 0;
    const hasFailedTasks = numFailedTasks > 0;
    const isComplete = status === "COMPLETE" || status === "COMPLETE_WITH_RETRIES";
    const isActive = status === "ACTIVE";
    const isPending = status === "PENDING";

    // Compact version for single-node stages
    if (compact) {
        return (
            <Tooltip
                title={
                    <Box sx={{ textAlign: "center" }}>
                        <Typography sx={{ fontSize: 11, fontWeight: 600 }}>
                            {numCompleteTasks}/{numTasks} tasks
                        </Typography>
                        {hasFailedTasks && (
                            <Typography sx={{ fontSize: 10, color: "#ff5252" }}>
                                {numFailedTasks} failed
                            </Typography>
                        )}
                        {numActiveTasks > 0 && (
                            <Typography sx={{ fontSize: 10, color: "#64b5f6" }}>
                                {numActiveTasks} running
                            </Typography>
                        )}
                    </Box>
                }
                arrow
            >
                <Box
                    sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 0.5,
                        px: 0.8,
                        py: 0.3,
                        borderRadius: 1,
                        backgroundColor: hasFailedTasks
                            ? "rgba(244, 67, 54, 0.15)"
                            : isComplete
                                ? "rgba(76, 175, 80, 0.15)"
                                : isActive
                                    ? "rgba(33, 150, 243, 0.15)"
                                    : "rgba(158, 158, 158, 0.15)",
                        border: `1px solid ${hasFailedTasks
                            ? "rgba(244, 67, 54, 0.4)"
                            : isComplete
                                ? "rgba(76, 175, 80, 0.4)"
                                : isActive
                                    ? "rgba(33, 150, 243, 0.4)"
                                    : "rgba(158, 158, 158, 0.4)"
                            }`,
                    }}
                >
                    {isActive && (
                        <PlayArrowIcon sx={{ fontSize: 12, color: "#64b5f6" }} />
                    )}
                    <Typography
                        sx={{
                            fontSize: 10,
                            fontWeight: 600,
                            color: hasFailedTasks
                                ? "#ff5252"
                                : isComplete
                                    ? "#81c784"
                                    : isActive
                                        ? "#64b5f6"
                                        : "#9e9e9e",
                        }}
                    >
                        {numCompleteTasks}/{numTasks}
                    </Typography>
                    {hasFailedTasks && (
                        <ErrorIcon sx={{ fontSize: 12, color: "#ff5252" }} />
                    )}
                </Box>
            </Tooltip>
        );
    }

    // Full version for multi-node stages - box design
    return (
        <Tooltip
            title={
                <Box sx={{ textAlign: "center", minWidth: 120 }}>
                    <Typography sx={{ fontSize: 12, fontWeight: 600, mb: 0.5 }}>
                        Task Progress
                    </Typography>
                    <Box sx={{ display: "flex", justifyContent: "space-between", gap: 2 }}>
                        <Typography sx={{ fontSize: 11 }}>Completed:</Typography>
                        <Typography sx={{ fontSize: 11, fontWeight: 600, color: "#81c784" }}>
                            {numCompleteTasks}
                        </Typography>
                    </Box>
                    {numActiveTasks > 0 && (
                        <Box sx={{ display: "flex", justifyContent: "space-between", gap: 2 }}>
                            <Typography sx={{ fontSize: 11 }}>Running:</Typography>
                            <Typography sx={{ fontSize: 11, fontWeight: 600, color: "#64b5f6" }}>
                                {numActiveTasks}
                            </Typography>
                        </Box>
                    )}
                    {hasFailedTasks && (
                        <Box sx={{ display: "flex", justifyContent: "space-between", gap: 2 }}>
                            <Typography sx={{ fontSize: 11 }}>Failed:</Typography>
                            <Typography sx={{ fontSize: 11, fontWeight: 600, color: "#ff5252" }}>
                                {numFailedTasks}
                            </Typography>
                        </Box>
                    )}
                    <Box sx={{ display: "flex", justifyContent: "space-between", gap: 2, mt: 0.5, pt: 0.5, borderTop: "1px solid rgba(255,255,255,0.2)" }}>
                        <Typography sx={{ fontSize: 11 }}>Total:</Typography>
                        <Typography sx={{ fontSize: 11, fontWeight: 600 }}>
                            {numTasks}
                        </Typography>
                    </Box>
                </Box>
            }
            arrow
        >
            <Box
                sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 0.75,
                    px: 1,
                    py: 0.4,
                    borderRadius: 1.5,
                    backgroundColor: "rgba(0, 0, 0, 0.3)",
                    border: `1px solid ${hasFailedTasks
                        ? "rgba(244, 67, 54, 0.5)"
                        : isComplete
                            ? "rgba(76, 175, 80, 0.4)"
                            : isActive
                                ? "rgba(33, 150, 243, 0.4)"
                                : "rgba(158, 158, 158, 0.3)"
                        }`,
                }}
            >
                {/* Mini progress bar */}
                <Box
                    sx={{
                        width: 40,
                        height: 4,
                        borderRadius: 2,
                        backgroundColor: "rgba(255, 255, 255, 0.1)",
                        overflow: "hidden",
                        position: "relative",
                    }}
                >
                    {/* Completed portion */}
                    <Box
                        sx={{
                            position: "absolute",
                            left: 0,
                            top: 0,
                            height: "100%",
                            width: `${progress}%`,
                            backgroundColor: hasFailedTasks ? "#ff5252" : "#4caf50",
                            transition: "width 0.3s ease",
                        }}
                    />
                    {/* Active tasks animation */}
                    {isActive && numActiveTasks > 0 && (
                        <Box
                            sx={{
                                position: "absolute",
                                left: `${progress}%`,
                                top: 0,
                                height: "100%",
                                width: `${(numActiveTasks / numTasks) * 100}%`,
                                backgroundColor: "#2196f3",
                                animation: "pulse 1.5s ease-in-out infinite",
                                "@keyframes pulse": {
                                    "0%, 100%": { opacity: 0.6 },
                                    "50%": { opacity: 1 },
                                },
                            }}
                        />
                    )}
                </Box>

                {/* Task count with label */}
                <Typography
                    sx={{
                        fontSize: 11,
                        fontWeight: 600,
                        color: hasFailedTasks
                            ? "#ff5252"
                            : isComplete
                                ? "#81c784"
                                : isActive
                                    ? "#64b5f6"
                                    : "#9e9e9e",
                        whiteSpace: "nowrap",
                    }}
                >
                    Tasks: {numCompleteTasks}/{numTasks}
                </Typography>

                {/* Failed tasks indicator */}
                {hasFailedTasks && (
                    <Box
                        sx={{
                            display: "flex",
                            alignItems: "center",
                            gap: 0.25,
                            color: "#ff5252",
                        }}
                    >
                        <ErrorIcon sx={{ fontSize: 12 }} />
                        <Typography sx={{ fontSize: 10, fontWeight: 700 }}>
                            {numFailedTasks}
                        </Typography>
                    </Box>
                )}
            </Box>
        </Tooltip>
    );
};

// Status icon component for stage header
const StageStatusIcon: FC<{ status: string; stageId: number }> = ({ status, stageId }) => {
    const stages = useAppSelector((state) => state.spark.stages);
    const stageData = stages?.find((s) => s.stageId === stageId);
    const progress = stageData?.stageProgress ?? 0;
    const failureReason = stageData?.failureReason;

    const tooltipText = getStatusTooltip(status);

    switch (status) {
        case "ACTIVE":
            return (
                <Tooltip title={`${tooltipText} (${progress}%)`} arrow>
                    <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center" }}>
                        <CircularProgress
                            color="info"
                            variant="determinate"
                            value={progress}
                            size={20}
                            thickness={5}
                        />
                    </Box>
                </Tooltip>
            );
        case "COMPLETE":
            return (
                <Tooltip title={tooltipText} arrow>
                    <CheckIcon sx={{ color: "#4caf50", fontSize: 20 }} />
                </Tooltip>
            );
        case "COMPLETE_WITH_RETRIES":
            return (
                <Tooltip title={tooltipText} arrow>
                    <CheckIcon sx={{ color: "#ff9800", fontSize: 20 }} />
                </Tooltip>
            );
        case "FAILED":
            return <ExceptionIcon failureReason={failureReason ?? ""} />;
        case "PENDING":
            return (
                <Tooltip title={tooltipText} arrow>
                    <PendingIcon sx={{ color: "#b2a300", fontSize: 20 }} />
                </Tooltip>
            );
        default:
            return null;
    }
};

const StageGroupNodeComponent: FC<StageGroupNodeProps> = ({ data }) => {
    const { stageId, status, stageDuration, stageInfo, attemptCount, durationPercentage, alerts, nodeCount } = data;
    const dispatch = useAppDispatch();
    const [searchParams] = useSearchParams();
    const stages = useAppSelector((state) => state.spark.stages);
    const stageData = stages?.find((s) => s.stageId === stageId);

    // Check if this stage is highlighted via URL param
    const isHighlighted = useMemo(() => {
        const stageIdParam = searchParams.get('stageid') || searchParams.get('stageId');
        if (!stageIdParam) return false;
        const highlightedStageId = parseInt(stageIdParam.trim(), 10);
        return !isNaN(highlightedStageId) && highlightedStageId === stageId;
    }, [searchParams, stageId]);

    // Get resource duration (executorRunTime) from stage metrics
    const resourceDuration = stageData?.metrics?.executorRunTime;

    // Build stage title - show attempts only if > 1
    const stageTitle = attemptCount && attemptCount > 1
        ? `Stage ${stageId} (${attemptCount} attempts)`
        : `Stage ${stageId}`;

    // Handle click to view stage details
    const handleViewStageDetails = () => {
        dispatch(setSelectedStage({
            selectedStage: {
                type: "onestage",
                stageId: stageId,
                status: status,
                stageDuration: stageDuration ?? 0,
                restOfStageDuration: undefined,
            }
        }));
    };

    // Get progress bar color based on percentage
    const progressBarColor = durationPercentage !== undefined
        ? getBucketedColor(durationPercentage)
        : "#78909c";

    // Determine if there are alerts and get the most severe one
    const hasAlerts = alerts && alerts.length > 0;
    const mostSevereAlert = hasAlerts
        ? alerts.find(a => a.type === "error") || alerts[0]
        : null;

    // Check if this is a single-node stage (simplified header)
    const isSingleNodeStage = nodeCount === 1;

    return (
        <Box className={`${styles.stageGroupNode} ${isHighlighted ? styles.stageGroupNodeHighlighted : ''}`}>
            {/* Stage header */}
            <Box className={isSingleNodeStage ? styles.stageGroupHeaderCompact : styles.stageGroupHeader}>
                {/* Progress bar at bottom of header */}
                {durationPercentage !== undefined && (
                    <Box
                        className={styles.stageGroupProgressBar}
                        sx={{
                            width: `${Math.min(durationPercentage, 100)}%`,
                            background: progressBarColor,
                        }}
                    />
                )}

                {/* Left side - Title and Alert Badge */}
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    <Typography className={isSingleNodeStage ? styles.stageGroupTitleCompact : styles.stageGroupTitle}>
                        {stageTitle}
                    </Typography>

                    {/* Alert Badge */}
                    {hasAlerts && mostSevereAlert && (
                        <TransperantTooltip
                            placement="bottom"
                            title={
                                <React.Fragment>
                                    {alerts!.map((alert, index) => (
                                        <Alert
                                            key={index}
                                            severity={alert.type}
                                            icon={alert.type === "warning" ? <WarningIcon /> : <ErrorIcon />}
                                            sx={{ mb: index < alerts!.length - 1 ? 1 : 0 }}
                                        >
                                            <AlertTitle>{alert.title}</AlertTitle>
                                            {alert.message}
                                            {alert.shortSuggestion && (
                                                <>
                                                    <br />
                                                    <b>Recommended Fix:</b>
                                                    <br />
                                                    {alert.shortSuggestion}
                                                </>
                                            )}
                                        </Alert>
                                    ))}
                                </React.Fragment>
                            }
                        >
                            <Box className={styles.stageGroupAlertBadge}>
                                {mostSevereAlert.type === "warning" ? (
                                    <WarningIcon sx={{ color: "#ff9100", fontSize: 18 }} />
                                ) : (
                                    <ErrorIcon sx={{ color: "#bf360c", fontSize: 18 }} />
                                )}
                                {alerts!.length > 1 && (
                                    <Typography sx={{ fontSize: 10, fontWeight: 700, color: "#fff", ml: 0.3 }}>
                                        {alerts!.length}
                                    </Typography>
                                )}
                            </Box>
                        </TransperantTooltip>
                    )}
                </Box>

                {/* Right side - Metrics and controls */}
                <Box className={styles.stageGroupMetrics}>
                    {/* Task progress indicator */}
                    {stageData && (
                        <TaskProgressIndicator
                            numTasks={stageData.numTasks}
                            numCompleteTasks={stageData.completedTasks}
                            numActiveTasks={stageData.activeTasks}
                            numFailedTasks={stageData.failedTasks}
                            status={status}
                            compact={isSingleNodeStage}
                        />
                    )}

                    {/* For single-node stages: show resource time only */}
                    {isSingleNodeStage && resourceDuration !== undefined && resourceDuration > 0 && (
                        <Tooltip title="Total executor CPU time (resource usage)" arrow>
                            <Box
                                className={styles.stageGroupDuration}
                                sx={{
                                    border: `1.5px solid ${progressBarColor}`,
                                    color: progressBarColor,
                                }}
                            >
                                {durationPercentage !== undefined ? `${durationPercentage.toFixed(1)}% - ` : ""}{humanizeTimeDiff(duration(resourceDuration))}
                            </Box>
                        </Tooltip>
                    )}

                    {/* For multi-node stages: show duration */}
                    {!isSingleNodeStage && stageDuration !== undefined && stageDuration > 0 && (
                        <Tooltip title="Stage wall-clock duration (percentage of total SQL duration)" arrow>
                            <Box
                                className={styles.stageGroupDuration}
                                sx={{
                                    border: `1.5px solid ${progressBarColor}`,
                                    color: progressBarColor,
                                }}
                            >
                                Duration: {durationPercentage !== undefined ? `${durationPercentage.toFixed(1)}% - ` : ""}{humanizeTimeDiff(duration(stageDuration))}
                            </Box>
                        </Tooltip>
                    )}

                    {/* Resource Duration with percentage - only for multi-node stages */}
                    {!isSingleNodeStage && resourceDuration !== undefined && resourceDuration > 0 && (
                        <Tooltip title="Total executor CPU time (resource usage)" arrow>
                            <Box
                                className={styles.stageGroupDuration}
                                sx={{
                                    border: `1.5px solid ${progressBarColor}`,
                                    color: progressBarColor,
                                }}
                            >
                                Resource Time: {durationPercentage !== undefined ? `${durationPercentage.toFixed(1)}% - ` : ""}{humanizeTimeDiff(duration(resourceDuration))}
                            </Box>
                        </Tooltip>
                    )}

                    {/* Status Icon */}
                    <Box className={styles.stageGroupStatusIcon}>
                        <StageStatusIcon status={status} stageId={stageId} />
                    </Box>

                    {/* Summary Button - icon only */}
                    <Tooltip title="View stage summary" arrow>
                        <IconButton
                            size="small"
                            onClick={handleViewStageDetails}
                            sx={{
                                backgroundColor: "rgba(25, 118, 210, 0.9)",
                                color: "#fff",
                                width: 24,
                                height: 24,
                                "&:hover": {
                                    backgroundColor: "rgba(25, 118, 210, 1)",
                                },
                            }}
                        >
                            <InfoOutlinedIcon sx={{ fontSize: 16 }} />
                        </IconButton>
                    </Tooltip>
                </Box>
            </Box>
        </Box>
    );
};

// Simple memoization - prevents unnecessary re-renders
const StageGroupNode = memo(StageGroupNodeComponent);

export { StageGroupNode };
