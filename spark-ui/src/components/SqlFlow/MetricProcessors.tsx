import { duration } from "moment";
import React from "react";
import SyntaxHighlighter from "react-syntax-highlighter";
import { a11yDark } from "react-syntax-highlighter/dist/esm/styles/hljs";
import { EnrichedSqlNode } from "../../interfaces/AppStore";
import { SqlMetric } from "../../interfaces/SparkSQLs";
import { getSizeFromMetrics } from "../../reducers/PlanGraphUtils";
import { truncateMiddle } from "../../reducers/PlanParsers/PlanParserUtils";
import {
    calculatePercentage,
    capitalizeWords,
    humanFileSize,
    humanizeTimeDiff,
    parseBytesString
} from "../../utils/FormatUtils";
import { MetricWithTooltip } from "./MetricDisplay";

export const processBaseMetrics = (node: EnrichedSqlNode): MetricWithTooltip[] => {
    return node.metrics
        .filter((metric: SqlMetric) => !!metric.value)
        .map(metric => ({ ...metric, name: capitalizeWords(metric.name) })) as MetricWithTooltip[];
};

export const processCachedStorageMetrics = (node: EnrichedSqlNode): MetricWithTooltip[] => {
    const metrics: MetricWithTooltip[] = [];
    const cachedStorage = node.cachedStorage;

    if (!cachedStorage) return metrics;

    if (cachedStorage.memoryUsed !== undefined) {
        metrics.push({
            name: "Bytes Cached In Memory",
            value: humanFileSize(cachedStorage.memoryUsed),
        });
    }

    if (cachedStorage.diskUsed !== undefined && cachedStorage.diskUsed !== 0) {
        metrics.push({
            name: "Bytes Cached In Disk",
            value: humanFileSize(cachedStorage.diskUsed),
        });
    }

    if (cachedStorage.numOfPartitions !== undefined) {
        metrics.push({
            name: "Num Of Partitions Cached",
            value: cachedStorage.numOfPartitions.toString(),
        });
    }

    if (cachedStorage.memoryUsed !== undefined &&
        cachedStorage.memoryUsed !== 0 &&
        cachedStorage.numOfPartitions !== undefined &&
        cachedStorage.numOfPartitions !== 0) {
        metrics.push({
            name: "Avg Cached Partition Size",
            value: humanFileSize(cachedStorage.memoryUsed / cachedStorage.numOfPartitions),
        });
    }

    if (cachedStorage.maxMemoryExecutorInfo !== undefined) {
        const memInfo = cachedStorage.maxMemoryExecutorInfo;
        if (memInfo.memoryUsed !== undefined &&
            memInfo.memoryRemaining !== undefined &&
            memInfo.memoryUsagePercentage !== undefined) {
            const tooltip = `${memInfo.memoryUsagePercentage.toFixed(2)}% - ${humanFileSize(memInfo.memoryUsed)} / ${humanFileSize(memInfo.memoryRemaining)}`;
            addTruncatedSmallTooltip(
                metrics,
                "Max Executor Memory Usage",
                tooltip,
                200
            );
        }
    }

    return metrics;
};

export const processIcebergCommitMetrics = (node: EnrichedSqlNode): MetricWithTooltip[] => {
    const metrics: MetricWithTooltip[] = [];
    const commit = node.icebergCommit;

    if (!commit) return metrics;

    const commitMetrics = commit.metrics;

    addTruncatedSmallTooltip(metrics, "Table Name", commit.tableName);

    let modeName: string | undefined = undefined;
    if (node.nodeName === "ReplaceData") {
        modeName = "copy on write";
    }
    if (node.nodeName === "WriteDelta") {
        modeName = "merge on read";
    }
    if (modeName !== undefined) {
        metrics.push({
            name: "Mode",
            value: modeName,
        });
    }

    metrics.push({
        name: "Commit Id",
        value: commit.commitId.toString(),
    });

    metrics.push({
        name: "Duration",
        value: humanizeTimeDiff(duration(commitMetrics.durationMS)),
    });

    metrics.push(
        ...handleAddedRemovedMetrics(
            "Records",
            commitMetrics.addedRecords,
            commitMetrics.removedRecords,
            commitMetrics.totalRecords,
            (x) => x.toString(),
        ),
    );

    metrics.push(
        ...handleAddedRemovedMetrics(
            "Files",
            commitMetrics.addedDataFiles,
            commitMetrics.removedDataFiles,
            commitMetrics.totalDataFiles,
            (x) => x.toString(),
        ),
    );

    const avgAddedFileSize = commitMetrics.addedDataFiles !== 0
        ? commitMetrics.addedFilesSizeInBytes / commitMetrics.addedDataFiles
        : 0;

    if (avgAddedFileSize !== 0) {
        metrics.push({
            name: "Average Added File Size",
            value: humanFileSize(avgAddedFileSize),
        });
    }

    metrics.push(
        ...handleAddedRemovedMetrics(
            "Delete Files",
            commitMetrics.addedPositionalDeletes,
            commitMetrics.removedPositionalDeletes,
            commitMetrics.totalPositionalDeletes,
            (x) => x.toString(),
        ),
    );

    metrics.push(
        ...handleAddedRemovedMetrics(
            "Bytes",
            commitMetrics.addedFilesSizeInBytes,
            commitMetrics.removedFilesSizeInBytes,
            commitMetrics.totalFilesSizeInBytes,
            humanFileSize,
        ),
    );

    return metrics;
};

export const processExchangeMetrics = (node: EnrichedSqlNode): MetricWithTooltip[] => {
    const metrics: MetricWithTooltip[] = [];

    if (node.nodeName !== "Exchange") return metrics;

    const partitionsMetric = parseFloat(
        node.metrics
            .find((metric) => metric.name === "partitions")
            ?.value?.replaceAll(",", "") ?? "0"
    );
    const shuffleWriteMetric = getSizeFromMetrics(node.metrics);

    if (partitionsMetric && shuffleWriteMetric) {
        const avgPartitionSize = shuffleWriteMetric / partitionsMetric;
        const avgPartitionSizeString = humanFileSize(avgPartitionSize);
        metrics.push({
            name: "Average Write Partition Size",
            value: avgPartitionSizeString
        });
    }

    return metrics;
};

export const processShuffleReadMetrics = (node: EnrichedSqlNode): MetricWithTooltip[] => {
    const metrics: MetricWithTooltip[] = [];

    if (node.nodeName !== "AQEShuffleRead") return metrics;

    const partitionsMetric = parseFloat(
        node.metrics
            .find((metric) => metric.name === "partitions")
            ?.value?.replaceAll(",", "") ?? "0"
    );
    const bytesReadMetric = getSizeFromMetrics(node.metrics);

    if (partitionsMetric && bytesReadMetric) {
        const avgPartitionSize = bytesReadMetric / partitionsMetric;
        const avgPartitionSizeString = humanFileSize(avgPartitionSize);
        metrics.push({
            name: "Average Read Partition Size",
            value: avgPartitionSizeString
        });
    }

    return metrics;
};

export const processInputNodeMetrics = (node: EnrichedSqlNode): MetricWithTooltip[] => {
    const metrics: MetricWithTooltip[] = [];

    if (node.type !== "input") return metrics;

    const filesReadMetric = parseFloat(
        node.metrics
            .find((metric) => metric.name === "files read")
            ?.value?.replaceAll(",", "") ?? "0",
    );
    const bytesReadMetric = parseBytesString(
        node.metrics.find((metric) => metric.name === "bytes read")?.value ?? "0",
    );
    const bytesPrunedMetric = parseBytesString(
        node.metrics
            .find((metric) => metric.name === "bytes pruned")
            ?.value ?? "0",
    );

    if (filesReadMetric && bytesReadMetric) {
        const avgFileSize = bytesReadMetric / filesReadMetric;
        const avgFileSizeString = humanFileSize(avgFileSize);
        metrics.push({
            name: "Average File Size",
            value: avgFileSizeString,
        });
    }

    if (bytesPrunedMetric && bytesReadMetric) {
        const prunePercentage = calculatePercentage(bytesPrunedMetric, bytesReadMetric + bytesPrunedMetric);
        metrics.push({
            name: "Bytes Pruned Ratio",
            value: prunePercentage.toFixed(2) + "%",
        });
    }

    return metrics;
};

export const processOutputNodeMetrics = (node: EnrichedSqlNode): MetricWithTooltip[] => {
    const metrics: MetricWithTooltip[] = [];

    if (node.type !== "output" || node.parsedPlan?.type !== "WriteToHDFS") return metrics;

    const fileWrittenMetric = parseFloat(
        node.metrics
            .find((metric) => metric.name === "files written")
            ?.value?.replaceAll(",", "") ?? "0",
    );
    const bytesWrittenMetric = parseBytesString(
        node.metrics.find((metric) => metric.name === "bytes written")?.value ?? "0",
    );

    if (fileWrittenMetric && bytesWrittenMetric) {
        const avgFileSize = bytesWrittenMetric / fileWrittenMetric;
        const avgFileSizeString = humanFileSize(avgFileSize);
        metrics.push({
            name: "Average File Size",
            value: avgFileSizeString,
        });
    }

    return metrics;
};

// Helper functions
export function handleAddedRemovedMetrics(
    name: string,
    added: number,
    removed: number,
    total: number,
    transformer: (x: number) => string,
): MetricWithTooltip[] {
    const previousSnapshotTotal = total - added + removed;
    const currentSnapshot = total;

    if (added === 0 && removed === 0) {
        return [];
    } else if (added !== 0 && removed === 0) {
        const addedPercentage = calculatePercentage(added, currentSnapshot).toFixed(1);
        return [
            {
                name: `Added ${name}`,
                value: transformer(added) + ` (${addedPercentage}%)`,
            },
        ];
    } else if (added === 0 && removed !== 0) {
        const removedPercentage = calculatePercentage(removed, previousSnapshotTotal).toFixed(1);
        return [
            {
                name: `Removed ${name}`,
                value: transformer(removed) + ` (${removedPercentage}%)`,
            },
        ];
    } else if (added === removed) {
        const updated = added;
        const updatedPercentage = calculatePercentage(updated, previousSnapshotTotal).toFixed(1);
        return [
            {
                name: `${name} Updated`,
                value: transformer(updated) + ` (${updatedPercentage}%)`,
            },
        ];
    } else {
        const addedPercentage = calculatePercentage(added, previousSnapshotTotal).toFixed(1);
        const removedPercentage = calculatePercentage(removed, previousSnapshotTotal).toFixed(1);

        return [
            {
                name: `Added ${name}`,
                value: transformer(added) + ` (${addedPercentage}%)`,
            },
            {
                name: `Removed ${name}`,
                value: transformer(removed) + ` (${removedPercentage}%)`,
            },
        ];
    }
}

export function addTruncatedSmallTooltip(
    dataTable: MetricWithTooltip[],
    name: string,
    value: string,
    limit: number = 25,
    pushEnd: boolean = false,
    showBlock: boolean = false,
    showSyntax: boolean = false,
) {
    const element = {
        name: name,
        value: truncateMiddle(value, limit),
        tooltip: value.length > limit ? value : undefined,
        showBlock: showBlock,
        showSyntax: showSyntax,
    };
    pushEnd ? dataTable.push(element) : dataTable.unshift(element);
}

export function addTruncatedCodeTooltip(
    dataTable: MetricWithTooltip[],
    name: string,
    value: string,
    limit: number = 120,
    pushEnd: boolean = true,
    showBlock: boolean = true,
) {
    const element = {
        name: name,
        value: truncateMiddle(value, limit),
        tooltip: (
            <React.Fragment>
                <SyntaxHighlighter
                    language="sql"
                    style={a11yDark}
                    customStyle={{
                        fontSize: "1em",
                    }}
                    wrapLongLines
                >
                    {value}
                </SyntaxHighlighter>
            </React.Fragment>
        ),
        showBlock: showBlock,
        showSyntax: true,
    };
    pushEnd ? dataTable.push(element) : dataTable.unshift(element);
}
