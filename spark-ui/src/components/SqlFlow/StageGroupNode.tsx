import CancelIcon from "@mui/icons-material/Cancel";
import ErrorIcon from "@mui/icons-material/Error";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import WarningIcon from "@mui/icons-material/Warning";
import { Alert, AlertTitle, Box, IconButton, Tooltip, Typography } from "@mui/material";
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

// Task progress indicator component
interface TaskProgressProps {
    numTasks: number;
    numCompleteTasks: number;
    numActiveTasks: number;
    numFailedTasks: number;
    status: string;
    compact?: boolean;
}

// Status colors matching the status icons
const STATUS_COLORS = {
    complete: "#4caf50",           // Green - matches CheckIcon color
    completeWithRetries: "#ff9800", // Orange - matches CheckIcon warning color
    active: "#1976d2",             // Blue - matches CircularProgress color
    failed: "#f44336",             // Red - matches ErrorIcon color
    pending: "#b2a300",            // Yellow - matches PendingIcon color
};

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
    const isComplete = status === "COMPLETE";
    const isCompleteWithRetries = status === "COMPLETE_WITH_RETRIES";
    const isActive = status === "ACTIVE";
    const isFailed = status === "FAILED";

    // Get the appropriate color based on status
    const getStatusColor = () => {
        if (hasFailedTasks || isFailed) return STATUS_COLORS.failed;
        if (isCompleteWithRetries) return STATUS_COLORS.completeWithRetries;
        if (isComplete) return STATUS_COLORS.complete;
        if (isActive) return STATUS_COLORS.active;
        return STATUS_COLORS.pending;
    };

    const statusColor = getStatusColor();

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
                            <Typography sx={{ fontSize: 10, color: STATUS_COLORS.failed }}>
                                {numFailedTasks} failed
                            </Typography>
                        )}
                        {numActiveTasks > 0 && (
                            <Typography sx={{ fontSize: 10, color: STATUS_COLORS.active }}>
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
                        backgroundColor: `${statusColor}22`,
                        border: `1px solid ${statusColor}66`,
                    }}
                >
                    {isActive && (
                        <PlayArrowIcon sx={{ fontSize: 12, color: statusColor }} />
                    )}
                    <Typography
                        sx={{
                            fontSize: 10,
                            fontWeight: 600,
                            color: statusColor,
                        }}
                    >
                        {numCompleteTasks}/{numTasks}
                    </Typography>
                    {hasFailedTasks && (
                        <CancelIcon sx={{ fontSize: 12, color: STATUS_COLORS.failed }} />
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
                        <Typography sx={{ fontSize: 11, fontWeight: 600, color: STATUS_COLORS.complete }}>
                            {numCompleteTasks}
                        </Typography>
                    </Box>
                    {numActiveTasks > 0 && (
                        <Box sx={{ display: "flex", justifyContent: "space-between", gap: 2 }}>
                            <Typography sx={{ fontSize: 11 }}>Running:</Typography>
                            <Typography sx={{ fontSize: 11, fontWeight: 600, color: STATUS_COLORS.active }}>
                                {numActiveTasks}
                            </Typography>
                        </Box>
                    )}
                    {hasFailedTasks && (
                        <Box sx={{ display: "flex", justifyContent: "space-between", gap: 2 }}>
                            <Typography sx={{ fontSize: 11 }}>Failed:</Typography>
                            <Typography sx={{ fontSize: 11, fontWeight: 600, color: STATUS_COLORS.failed }}>
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
                    border: `1px solid ${statusColor}66`,
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
                            backgroundColor: hasFailedTasks ? STATUS_COLORS.failed : isCompleteWithRetries ? STATUS_COLORS.completeWithRetries : STATUS_COLORS.complete,
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
                                backgroundColor: STATUS_COLORS.active,
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
                        color: statusColor,
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
                            color: STATUS_COLORS.failed,
                        }}
                    >
                        <CancelIcon sx={{ fontSize: 12 }} />
                        <Typography sx={{ fontSize: 10, fontWeight: 700 }}>
                            {numFailedTasks}
                        </Typography>
                    </Box>
                )}
            </Box>
        </Tooltip>
    );
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

                {/* Left side - Exception icon, Title and Alert Badge */}
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                    {/* Exception icon for failed stages - positioned first */}
                    {status === "FAILED" && stageData?.failureReason && (
                        <Box sx={{
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "center",
                            transform: "scale(0.7)",
                            transformOrigin: "center",
                        }}>
                            <ExceptionIcon failureReason={stageData.failureReason} />
                        </Box>
                    )}
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
