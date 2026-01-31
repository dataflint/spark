import {
    Input as InputIcon,
    Output as OutputIcon,
    SwapVert as ShuffleReadIcon,
    SwapHoriz as ShuffleWriteIcon,
    WarningAmber as SpillIcon,
    Timer as DurationIcon,
    Schedule as ResourceDurationIcon,
} from "@mui/icons-material";
import {
    Box,
    LinearProgress,
    LinearProgressProps,
    Link,
    Typography
} from "@mui/material";
import { duration } from "moment";
import React from "react";
import { useAppSelector } from "../../Hooks";
import {
    SQLNodeExchangeStageData,
    SQLNodeStageData,
    SparkStageStore,
} from "../../interfaces/AppStore";
import { humanFileSize, humanizeTimeDiff } from "../../utils/FormatUtils";
import { BASE_CURRENT_PAGE } from "../../utils/UrlConsts";
import { getBaseAppUrl } from "../../utils/UrlUtils";
import BytesDistributionChart from "./BytesDistributionChart";
import DurationDistributionChart from "./DurationDistributionChart";
import NumbersDistributionChart from "./NumbersDistributionChart";

const linkToStage = (stageId: number, attemptId: number) => {
    window.open(
        `${getBaseAppUrl(BASE_CURRENT_PAGE)}/stages/stage/?id=${stageId}&attempt=${attemptId}`,
        "_blank",
    );
};

function LinearProgressWithLabel(props: LinearProgressProps & { completedTasks: number, runningTasks: number, numTasks: number }) {
    const completedTasksPercent = ((props.completedTasks) / props.numTasks) * 100;
    const runningTasksPercent = ((props.completedTasks + props.runningTasks) / props.numTasks) * 100;

    return (
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: "center" }}>
            <Box sx={{ width: '70%', m: 1, mr: 2 }}>
                <LinearProgress variant="buffer" value={completedTasksPercent} valueBuffer={runningTasksPercent} {...props} />
            </Box>
            <Box sx={{ minWidth: 35 }}>
                <Typography variant="body2" color="text.secondary">
                    {props.completedTasks}/{props.numTasks}
                </Typography>
            </Box>
        </Box>
    );
}

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
    if (readStage === undefined && writeStage === undefined) return <div></div>;

    return (
        <Box>
            <Box sx={{ m: 1 }} display="flex" gap={1}>
                {writeStage === undefined ? undefined : <Link
                    color="inherit"
                    sx={{ textAlign: "center", margin: "0 auto" }}
                    onClick={(evn) => linkToStage(stage.writeStage, 0)}
                >
                    Write Stage - {stage.writeStage}
                </Link>}
                {readStage === undefined ? undefined : <Link
                    color="inherit"
                    sx={{ textAlign: "center", margin: "0 auto" }}
                    onClick={(evn) => linkToStage(stage.readStage, 0)}
                >
                    Read Stage - {stage.readStage}
                </Link>}
            </Box>
            {writeStage === undefined ? undefined : <Box sx={{ m: 1 }}>
                <Typography variant="h6" sx={{ textAlign: "center", margin: "0 auto" }}>Write Stage</Typography>
                <LinearProgressWithLabel numTasks={writeStage.numTasks} runningTasks={writeStage.activeTasks} completedTasks={writeStage.completedTasks}></LinearProgressWithLabel>
                <StageSummary stageData={writeStage} />
                {writeStage.durationDistribution !== undefined ? <DurationDistributionChart durationDist={writeStage.durationDistribution} /> : undefined}
                {writeStage.shuffleWriteDistribution !== undefined && writeStage.shuffleWriteDistribution.some(x => x !== 0) ? <BytesDistributionChart title="shuffle write (bytes)" bytesDist={writeStage.shuffleWriteDistribution} /> : undefined}
                {writeStage.spillDiskDistriution !== undefined && writeStage.spillDiskDistriution.some(x => x !== 0) ? <NumbersDistributionChart title="spill disk (bytes)" numbersDist={writeStage.spillDiskDistriution} /> : undefined}
            </Box>}
            {readStage === undefined ? undefined : <Box sx={{ m: 1 }}>
                <Typography variant="h6" sx={{ textAlign: "center", margin: "0 auto" }} >Read Stage</Typography>
                <LinearProgressWithLabel numTasks={readStage.numTasks} runningTasks={readStage.activeTasks} completedTasks={readStage.completedTasks}></LinearProgressWithLabel>
                <StageSummary stageData={readStage} />
                {readStage.durationDistribution !== undefined ? <DurationDistributionChart durationDist={readStage.durationDistribution} /> : undefined}
                {readStage.shuffleReadDistribution !== undefined && readStage.shuffleReadDistribution.some(x => x !== 0) ? <BytesDistributionChart title="shuffle read (bytes)" bytesDist={readStage.shuffleReadDistribution} /> : undefined}
                {readStage.shuffleWriteDistribution !== undefined && readStage.shuffleWriteDistribution.some(x => x !== 0) ? <BytesDistributionChart title="shuffle write (bytes)" bytesDist={readStage.shuffleWriteDistribution} /> : undefined}
                {readStage.spillDiskDistriution !== undefined && readStage.spillDiskDistriution.some(x => x !== 0) ? <NumbersDistributionChart title="spill disk (bytes)" numbersDist={readStage.spillDiskDistriution} /> : undefined}
            </Box>}
        </Box>
    );
}

function SingleStageIconTooltip({
    stage,
}: {
    stage: SQLNodeStageData;
}): JSX.Element {
    const stages = useAppSelector((state) => state.spark.stages);
    const stagesData = stages?.filter(
        (currentStage) =>
            stage.type === "onestage" && stage.stageId === currentStage.stageId,
    );

    if (stagesData === undefined || stagesData.length === 0) return <div></div>;

    const hasFailures = stagesData.length > 1

    return (
        <Box>
            {stagesData.map(stageData => (
                <Box id={`stage-${stageData.stageId}-${stageData.attemptId}`} sx={{ m: 1 }} display="flex" flexDirection="column" gap={1}>
                    <Link color="inherit" onClick={(evn) => linkToStage(stage.stageId, stageData.attemptId)} sx={{ textAlign: "center", margin: "0 auto" }}>
                        Stage {stage.stageId} {hasFailures ? `(attempt ${stageData.attemptId})` : ""}
                    </Link>
                    <LinearProgressWithLabel numTasks={stageData.numTasks} runningTasks={stageData.activeTasks} completedTasks={stageData.completedTasks}></LinearProgressWithLabel>
                    <StageSummary stageData={stageData} />
                    {stageData.shuffleReadDistribution !== undefined && stageData.shuffleReadDistribution.some(x => x !== 0) ? <BytesDistributionChart title="shuffle read (bytes)" bytesDist={stageData.shuffleReadDistribution} /> : undefined}
                    {stageData.inputDistribution !== undefined && stageData.inputDistribution.some(x => x !== 0) ? <BytesDistributionChart title="input bytes" bytesDist={stageData.inputDistribution} /> : undefined}
                    {stageData.inputRowsDistribution !== undefined && stageData.inputRowsDistribution.some(x => x !== 0) ? <NumbersDistributionChart title="input rows" numbersDist={stageData.inputRowsDistribution} /> : undefined}
                    {stageData.durationDistribution !== undefined ? <DurationDistributionChart durationDist={stageData.durationDistribution} /> : undefined}
                    {stageData.outputDistribution !== undefined && stageData.outputDistribution.some(x => x !== 0) ? <BytesDistributionChart title="output bytes" bytesDist={stageData.outputDistribution} /> : undefined}
                    {stageData.outputRowsDistribution !== undefined && stageData.outputRowsDistribution.some(x => x !== 0) ? <NumbersDistributionChart title="output rows" numbersDist={stageData.outputRowsDistribution} /> : undefined}
                    {stageData.shuffleWriteDistribution !== undefined && stageData.shuffleWriteDistribution.some(x => x !== 0) ? <BytesDistributionChart title="shuffle write (bytes)" bytesDist={stageData.shuffleWriteDistribution} /> : undefined}
                    {stageData.spillDiskDistriution !== undefined && stageData.spillDiskDistriution.some(x => x !== 0) ? <NumbersDistributionChart title="spill disk (bytes)" numbersDist={stageData.spillDiskDistriution} /> : undefined}
                </Box>))}
        </Box>
    )
}

interface MetricData {
    icon: React.ReactNode;
    label: string;
    value: string;
    color: string;
}

function StageMetricItem({ icon, label, value, color }: MetricData): JSX.Element {
    return (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, flex: 1, minWidth: 0 }}>
            <Box sx={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'center',
                color: color,
                flexShrink: 0
            }}>
                {icon}
            </Box>
            <Typography variant="body2" sx={{ color: 'text.secondary', flexShrink: 0 }}>
                {label}:
            </Typography>
            <Typography variant="body2" sx={{ fontWeight: 600 }}>
                {value}
            </Typography>
        </Box>
    );
}

function MetricRow({ left, right, centered }: { left: MetricData; right?: MetricData; centered?: boolean }): JSX.Element {
    if (centered) {
        return (
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', py: 0.25 }}>
                <StageMetricItem {...left} />
            </Box>
        );
    }
    return (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, py: 0.25 }}>
            <StageMetricItem {...left} />
            {right && <StageMetricItem {...right} />}
        </Box>
    );
}

function StageSummary({
    stageData,
}: {
    stageData: SparkStageStore;
}): JSX.Element {
    const { inputBytes, outputBytes, shuffleReadBytes, shuffleWriteBytes, diskBytesSpilled, executorRunTime } = stageData.metrics;
    const stageDurationMs = stageData.stageRealTimeDurationMs;

    // Build metric objects
    const durationMetric: MetricData | null = stageDurationMs !== undefined && stageDurationMs > 0 
        ? { icon: <DurationIcon sx={{ fontSize: 16 }} />, label: "Duration", value: humanizeTimeDiff(duration(stageDurationMs)), color: "#607d8b" }
        : null;
    
    const resourceTimeMetric: MetricData | null = executorRunTime > 0 
        ? { icon: <ResourceDurationIcon sx={{ fontSize: 16 }} />, label: "Resource Time", value: humanizeTimeDiff(duration(executorRunTime)), color: "#795548" }
        : null;
    
    const inputMetric: MetricData | null = inputBytes > 0 
        ? { icon: <InputIcon sx={{ fontSize: 16 }} />, label: "Input", value: humanFileSize(inputBytes), color: "#3f51b5" }
        : null;
    
    const shuffleReadMetric: MetricData | null = shuffleReadBytes > 0 
        ? { icon: <ShuffleReadIcon sx={{ fontSize: 16 }} />, label: "Shuffle Read", value: humanFileSize(shuffleReadBytes), color: "#2196f3" }
        : null;
    
    const shuffleWriteMetric: MetricData | null = shuffleWriteBytes > 0 
        ? { icon: <ShuffleWriteIcon sx={{ fontSize: 16 }} />, label: "Shuffle Write", value: humanFileSize(shuffleWriteBytes), color: "#ff9800" }
        : null;
    
    const outputMetric: MetricData | null = outputBytes > 0 
        ? { icon: <OutputIcon sx={{ fontSize: 16 }} />, label: "Output", value: humanFileSize(outputBytes), color: "#9c27b0" }
        : null;
    
    const spillMetric: MetricData | null = diskBytesSpilled > 0 
        ? { icon: <SpillIcon sx={{ fontSize: 16 }} />, label: "Spill", value: humanFileSize(diskBytesSpilled), color: "#f44336" }
        : null;

    // Build rows with pairing logic
    const rows: { left: MetricData; right?: MetricData }[] = [];

    // Row 1: Duration (left) and Resource Time (right)
    if (durationMetric || resourceTimeMetric) {
        if (durationMetric && resourceTimeMetric) {
            rows.push({ left: durationMetric, right: resourceTimeMetric });
        } else if (durationMetric) {
            rows.push({ left: durationMetric });
        } else if (resourceTimeMetric) {
            rows.push({ left: resourceTimeMetric });
        }
    }

    // Track which data metrics are used
    const usedMetrics = new Set<string>();

    // Pairing rules for data metrics:
    // 1. Input + Shuffle Write
    // 2. Shuffle Read + Shuffle Write
    // 3. Shuffle Read + Output
    // 4. Input + Output
    
    if (inputMetric && shuffleWriteMetric && !usedMetrics.has('input') && !usedMetrics.has('shuffleWrite')) {
        rows.push({ left: inputMetric, right: shuffleWriteMetric });
        usedMetrics.add('input');
        usedMetrics.add('shuffleWrite');
    }
    
    if (shuffleReadMetric && shuffleWriteMetric && !usedMetrics.has('shuffleRead') && !usedMetrics.has('shuffleWrite')) {
        rows.push({ left: shuffleReadMetric, right: shuffleWriteMetric });
        usedMetrics.add('shuffleRead');
        usedMetrics.add('shuffleWrite');
    }
    
    if (shuffleReadMetric && outputMetric && !usedMetrics.has('shuffleRead') && !usedMetrics.has('output')) {
        rows.push({ left: shuffleReadMetric, right: outputMetric });
        usedMetrics.add('shuffleRead');
        usedMetrics.add('output');
    }
    
    if (inputMetric && outputMetric && !usedMetrics.has('input') && !usedMetrics.has('output')) {
        rows.push({ left: inputMetric, right: outputMetric });
        usedMetrics.add('input');
        usedMetrics.add('output');
    }

    // Add remaining unpaired metrics (excluding spill)
    const remainingMetrics: MetricData[] = [];
    if (inputMetric && !usedMetrics.has('input')) remainingMetrics.push(inputMetric);
    if (shuffleReadMetric && !usedMetrics.has('shuffleRead')) remainingMetrics.push(shuffleReadMetric);
    if (shuffleWriteMetric && !usedMetrics.has('shuffleWrite')) remainingMetrics.push(shuffleWriteMetric);
    if (outputMetric && !usedMetrics.has('output')) remainingMetrics.push(outputMetric);

    // Pair remaining metrics
    for (let i = 0; i < remainingMetrics.length; i += 2) {
        if (i + 1 < remainingMetrics.length) {
            rows.push({ left: remainingMetrics[i], right: remainingMetrics[i + 1] });
        } else {
            rows.push({ left: remainingMetrics[i] });
        }
    }

    if (rows.length === 0 && !spillMetric) return <div></div>;

    return (
        <Box sx={{ py: 0.5 }}>
            {rows.map((row, index) => (
                <MetricRow key={index} left={row.left} right={row.right} />
            ))}
            {spillMetric && (
                <MetricRow key="spill" left={spillMetric} centered />
            )}
        </Box>
    );
}

export default function StageIconDrawer({
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
