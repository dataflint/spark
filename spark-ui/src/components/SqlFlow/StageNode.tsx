import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import { Box, Tooltip, Typography } from "@mui/material";
import { duration } from "moment";
import React, { FC } from "react";
import SyntaxHighlighter from "react-syntax-highlighter";
import { a11yDark } from "react-syntax-highlighter/dist/esm/styles/hljs";
import { Handle, Position } from "reactflow";
import { useAppDispatch, useAppSelector } from "../../Hooks";
import { EnrichedSqlNode } from "../../interfaces/AppStore";
import { SqlMetric } from "../../interfaces/SparkSQLs";
import { setSelectedStage } from '../../reducers/GeneralSlice';
import { truncateMiddle } from "../../reducers/PlanParsers/PlanParserUtils";
import {
  calculatePercentage,
  humanFileSize,
  humanizeTimeDiff,
  parseBytesString,
} from "../../utils/FormatUtils";
import AlertBadge, { TransperantTooltip } from "../AlertBadge/AlertBadge";
import { ConditionalWrapper } from "../InfoBox/InfoBox";
import StageIcon from "./StageIcon";
import styles from "./node-style.module.css";

export const StageNodeName: string = "stageNode";

interface MetricWithTooltip {
  name: string;
  value: string;
  tooltip?: string | JSX.Element;
  showBlock?: boolean;
  showSyntax?: boolean;
}

function getBucketedColor(percentage: number): string {
  if (percentage > 100) {
    percentage = 100;
  }

  // Determine the bucket
  let bucket = Math.floor(percentage / 10);

  // Assign a color to each bucket
  switch (bucket) {
    case 0:
      return "#006400"; // Dark green
    case 1:
      return "#228B22"; // Forest green
    case 2:
      return "#6B8E23"; // Olive drab
    case 3:
      return "#B8860B"; // Dark goldenrod
    case 4: // Fall through
    case 5: // Fall through
    case 6: // Fall through
    case 7: // Fall through
    case 8: // Fall through
    case 9: // Fall through
    case 10:
      return "#FF4500"; // Red-orange
    default:
      return "#FF0000"; // Red
  }
}

function handleAddedRemovedMetrics(
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
    const addedPercentage = calculatePercentage(added, currentSnapshot).toFixed(
      1,
    );
    return [
      {
        name: `Added ${name}`,
        value: transformer(added) + ` (${addedPercentage}%)`,
      },
    ];
  } else if (added === 0 && removed !== 0) {
    const removedPercentage = calculatePercentage(
      removed,
      previousSnapshotTotal,
    ).toFixed(1);
    return [
      {
        name: `Removed ${name}`,
        value: transformer(removed) + ` (${removedPercentage}%)`,
      },
    ];
  } else if (added === removed) {
    const updated = added;
    const updatedPercentage = calculatePercentage(
      updated,
      previousSnapshotTotal,
    ).toFixed(1);
    return [
      {
        name: `${name} Updated`,
        value: transformer(updated) + ` (${updatedPercentage}%)`,
      },
    ];
  } else {
    const addedPercentage = calculatePercentage(
      added,
      previousSnapshotTotal,
    ).toFixed(1);
    const removedPercentage = calculatePercentage(
      removed,
      previousSnapshotTotal,
    ).toFixed(1);

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

export const StageNode: FC<{
  data: { sqlId: string; node: EnrichedSqlNode };
}> = ({ data }): JSX.Element => {
  const dispatch = useAppDispatch();
  const alerts = useAppSelector((state) => state.spark.alerts);
  const sqlNodeAlert = alerts?.alerts.find(
    (alert) =>
      alert.source.type === "sql" &&
      alert.source.sqlNodeId === data.node.nodeId &&
      alert.source.sqlId === data.sqlId,
  );

  const dataTable = data.node.metrics.filter(
    (metric: SqlMetric) => !!metric.value,
  ) as MetricWithTooltip[];

  if (data.node.cachedStorage !== undefined) {
    const cachedStorage = data.node.cachedStorage;
    if (cachedStorage.memoryUsed !== undefined) {
      dataTable.push({
        name: "bytes cached in memory",
        value: humanFileSize(cachedStorage.memoryUsed),
      });
    }
    if (cachedStorage.diskUsed !== undefined && cachedStorage.diskUsed !== 0) {
      dataTable.push({
        name: "bytes cached in disk",
        value: humanFileSize(cachedStorage.diskUsed),
      });
    }
    if (cachedStorage.numOfPartitions !== undefined) {
      dataTable.push({
        name: "num of partitions cached",
        value: cachedStorage.numOfPartitions.toString(),
      });
    }
    if (cachedStorage.memoryUsed !== undefined && cachedStorage.memoryUsed !== 0 && cachedStorage.numOfPartitions !== undefined && cachedStorage.numOfPartitions !== 0) {
      dataTable.push({
        name: "avg cached partition size",
        value: humanFileSize(cachedStorage.memoryUsed / cachedStorage.numOfPartitions),
      });
    }
    if (cachedStorage.maxMemoryExecutorInfo !== undefined) {
      if (cachedStorage.maxMemoryExecutorInfo.memoryUsed !== undefined && cachedStorage.maxMemoryExecutorInfo.memoryRemaining !== undefined && cachedStorage.maxMemoryExecutorInfo.memoryUsagePercentage !== undefined) {
        addTruncatedSmallTooltipMultiLine(
          dataTable,
          "max executor memory usage",
          [`${cachedStorage.maxMemoryExecutorInfo.memoryUsagePercentage.toFixed(2)}% - ${humanFileSize(cachedStorage.maxMemoryExecutorInfo.memoryUsed)} / ${humanFileSize(cachedStorage.maxMemoryExecutorInfo.memoryRemaining)}`,],
          200)
      }
    }
    // consider returning and parsing to something more human readable
    // if (cachedStorage.storageLevel !== undefined) {
    //   addTruncatedSmallTooltipMultiLine(
    //     dataTable,
    //     "storage level",
    //     [cachedStorage.storageLevel],
    //     100)
    // }
  }


  if (data.node.icebergCommit !== undefined) {
    const commit = data.node.icebergCommit;
    const metrics = commit.metrics;
    addTruncatedSmallTooltip(dataTable, "Table Name", commit.tableName);
    let modeName: string | undefined = undefined;
    if (data.node.nodeName === "ReplaceData") {
      modeName = "copy on write";
    }
    if (data.node.nodeName === "WriteDelta") {
      modeName = "merge on read";
    }
    if (modeName !== undefined) {
      dataTable.push({
        name: "Mode",
        value: modeName,
      });
    }

    dataTable.push({
      name: "Commit id",
      value: commit.commitId.toString(),
    });

    dataTable.push({
      name: "Duration",
      value: humanizeTimeDiff(duration(metrics.durationMS)),
    });

    dataTable.push(
      ...handleAddedRemovedMetrics(
        "Records",
        metrics.addedRecords,
        metrics.removedRecords,
        metrics.totalRecords,
        (x) => x.toString(),
      ),
    );
    dataTable.push(
      ...handleAddedRemovedMetrics(
        "Files",
        metrics.addedDataFiles,
        metrics.removedDataFiles,
        metrics.totalDataFiles,
        (x) => x.toString(),
      ),
    );
    const avgAddedFileSize =
      metrics.addedDataFiles !== 0
        ? metrics.addedFilesSizeInBytes / metrics.addedDataFiles
        : 0;
    if (avgAddedFileSize !== 0) {
      dataTable.push({
        name: "Average Added File Size",
        value: humanFileSize(avgAddedFileSize),
      });
    }
    dataTable.push(
      ...handleAddedRemovedMetrics(
        "Delete Files",
        metrics.addedPositionalDeletes,
        metrics.removedPositionalDeletes,
        metrics.totalPositionalDeletes,
        (x) => x.toString(),
      ),
    );
    dataTable.push(
      ...handleAddedRemovedMetrics(
        "Bytes",
        metrics.addedFilesSizeInBytes,
        metrics.removedFilesSizeInBytes,
        metrics.totalFilesSizeInBytes,
        humanFileSize,
      ),
    );
    dataTable.push(
      ...handleAddedRemovedMetrics(
        "Positional Deletes",
        metrics.addedPositionalDeletes,
        metrics.removedPositionalDeletes,
        metrics.totalPositionalDeletes,
        (x) => x.toString(),
      ),
    );
    dataTable.push(
      ...handleAddedRemovedMetrics(
        "Equality Deletes",
        metrics.addedEqualityDeletes,
        metrics.removedEqualityDeletes,
        metrics.totalEqualityDeletes,
        (x) => x.toString(),
      ),
    );
  }
  if (data.node.parsedPlan !== undefined) {
    const parsedPlan = data.node.parsedPlan;
    switch (parsedPlan.type) {
      case "CollectLimit":
        dataTable.push({
          name: "Limit",
          value: parsedPlan.plan.limit.toString(),
        });
        break;
      case "Filter":
        addTruncatedCodeTooltip(
          dataTable,
          "Condition",
          parsedPlan.plan.condition,
        );
        break;
      case "Coalesce":
        dataTable.push({
          name: "Partitions",
          value: parsedPlan.plan.partitionNum.toString(),
        });
        break;
      case "TakeOrderedAndProject":
        dataTable.push({
          name: "Limit",
          value: parsedPlan.plan.limit.toString(),
        });
        break;
      case "WriteToHDFS":
        if (parsedPlan.plan.tableName !== undefined) {
          addTruncatedSmallTooltip(
            dataTable,
            "Table Name",
            parsedPlan.plan.tableName,
          );
        }
        addTruncatedSmallTooltip(
          dataTable,
          "File Path",
          parsedPlan.plan.location,
        );
        dataTable.push({ name: "Format", value: parsedPlan.plan.format });
        dataTable.push({ name: "Mode", value: parsedPlan.plan.mode });
        if (parsedPlan.plan.partitionKeys !== undefined) {
          addTruncatedSmallTooltipMultiLine(
            dataTable,
            "Partition By",
            parsedPlan.plan.partitionKeys,
          );
        }
        break;
      case "FileScan":
        if (parsedPlan.plan.PushedFilters !== undefined && parsedPlan.plan.PushedFilters.length > 0) {
          addTruncatedCodeTooltipMultiline(
            dataTable,
            "Push Down Filters",
            parsedPlan.plan.PushedFilters,
            25,
            false,
          );
        }
        if (parsedPlan.plan.PartitionFilters !== undefined) {
          if (parsedPlan.plan.PartitionFilters.length > 0) {
            addTruncatedCodeTooltipMultiline(
              dataTable,
              "Partition Filters",
              parsedPlan.plan.PartitionFilters,
              100,
              false
            );
          }
          else {
            dataTable.push({
              name: "Partition Filters",
              value: "Full Scan",
            });
          }
        }
        if (parsedPlan.plan.Location !== undefined) {
          addTruncatedSmallTooltip(
            dataTable,
            "File Path",
            parsedPlan.plan.Location,
          );
        }
        if (parsedPlan.plan.tableName !== undefined) {
          addTruncatedSmallTooltip(
            dataTable,
            "Table",
            parsedPlan.plan.tableName,
          );
        }
        break;
      case "Exchange":
        if (
          parsedPlan.plan.fields !== undefined &&
          parsedPlan.plan.fields.length > 0
        ) {
          addTruncatedSmallTooltipMultiLine(
            dataTable,
            parsedPlan.plan.type === "hashpartitioning"
              ? parsedPlan.plan.fields.length === 1
                ? "hashed field"
                : "hashed fields"
              : parsedPlan.plan.fields.length === 1
                ? "ranged field"
                : "ranged fields",
            parsedPlan.plan.fields,
          );
        }
        break;
      case "Project":
        if (parsedPlan.plan.fields !== undefined) {
          addTruncatedCodeTooltipMultiline(
            dataTable,
            "Selected Fields",
            parsedPlan.plan.fields,
          );
        }
        break;
      case "HashAggregate":
        if (
          parsedPlan.plan.keys !== undefined &&
          parsedPlan.plan.keys.length > 0
        ) {
          addTruncatedCodeTooltipMultiline(
            dataTable,
            parsedPlan.plan.functions.length === 0 ? "Distinct By" : "Aggregate By",
            parsedPlan.plan.keys,
          );
        }
        if (
          parsedPlan.plan.functions !== undefined &&
          parsedPlan.plan.functions.length > 0
        ) {
          addTruncatedCodeTooltipMultiline(
            dataTable,
            "Expression",
            parsedPlan.plan.functions,
          );
        }
        break;
      case "Sort":
        if (
          parsedPlan.plan.fields !== undefined &&
          parsedPlan.plan.fields.length > 0
        ) {
          addTruncatedSmallTooltipMultiLine(
            dataTable,
            "Sort by",
            parsedPlan.plan.fields,
          );
        }
        break;
      case "Join":
        dataTable.push({
          name: "Join Type",
          value: parsedPlan.plan.joinSideType,
        });

        if (
          parsedPlan.plan.leftKeys !== undefined &&
          parsedPlan.plan.leftKeys.length > 0
        ) {
          dataTable.push({
            name:
              parsedPlan.plan.leftKeys.length > 1
                ? "Left Side Keys"
                : "Left Side Key",
            value: truncateMiddle(parsedPlan.plan.leftKeys.join(", "), 25),
            tooltip:
              parsedPlan.plan.leftKeys.length > 25
                ? parsedPlan.plan.leftKeys.join(", ")
                : undefined,
          });
        }
        if (
          parsedPlan.plan.rightKeys !== undefined &&
          parsedPlan.plan.rightKeys.length > 0
        ) {
          dataTable.push({
            name:
              parsedPlan.plan.rightKeys.length > 1
                ? "Right Side Keys"
                : "Right Side Key",
            value: truncateMiddle(parsedPlan.plan.rightKeys.join(", "), 25),
            tooltip:
              parsedPlan.plan.rightKeys.length > 25
                ? parsedPlan.plan.rightKeys.join(", ")
                : undefined,
          });
        }
        if (
          parsedPlan.plan.joinCondition !== undefined &&
          parsedPlan.plan.joinCondition !== ""
        ) {
          addTruncatedCodeTooltip(
            dataTable,
            "Join Condition",
            parsedPlan.plan.joinCondition,
          );
        }
        break;
      case "Window":
        if (
          parsedPlan.plan.partitionFields !== undefined &&
          parsedPlan.plan.partitionFields.length > 0
        ) {
          addTruncatedSmallTooltipMultiLine(
            dataTable,
            "Partition Fields",
            parsedPlan.plan.partitionFields,
          );
        }
        if (
          parsedPlan.plan.sortFields !== undefined &&
          parsedPlan.plan.sortFields.length > 0
        ) {
          addTruncatedSmallTooltipMultiLine(
            dataTable,
            "Sort Fields",
            parsedPlan.plan.sortFields,
          );
        }
        if (
          parsedPlan.plan.selectFields !== undefined &&
          parsedPlan.plan.selectFields.length > 0
        ) {
          addTruncatedCodeTooltipMultiline(
            dataTable,
            "Select Fields",
            parsedPlan.plan.selectFields,
          );
        }
    }
  }

  if (data.node.type === "input") {
    const filesReadMetric = parseFloat(
      data.node.metrics
        .find((metric) => metric.name === "files read")
        ?.value?.replaceAll(",", "") ?? "0",
    );
    const bytesReadMetric = parseBytesString(
      data.node.metrics.find((metric) => metric.name === "bytes read")?.value ??
      "0",
    );

    if (filesReadMetric && bytesReadMetric) {
      const avgFileSize = bytesReadMetric / filesReadMetric;
      const avgFileSizeString = humanFileSize(avgFileSize);
      dataTable.push({
        name: "Average File Size",
        value: avgFileSizeString,
      });
    }
  }
  const fileWrittenMetric = parseFloat(
    data.node.metrics
      .find((metric) => metric.name === "files written")
      ?.value?.replaceAll(",", "") ?? "0",
  );
  const bytesWrittenMetric = parseBytesString(
    data.node.metrics.find((metric) => metric.name === "bytes written")
      ?.value ?? "0",
  );

  if (
    data.node.type === "output" &&
    data.node.parsedPlan?.type === "WriteToHDFS" &&
    fileWrittenMetric &&
    bytesWrittenMetric
  ) {
    const avgFileSize = bytesWrittenMetric / fileWrittenMetric;
    const avgFileSizeString = humanFileSize(avgFileSize);
    dataTable.push({
      name: "Average File Size",
      value: avgFileSizeString,
    });
  }

  return (
    <>
      <Handle type="target" position={Position.Left} id="b" />
      <Box position="relative" width={280} height={280}>
        <div className={styles.node}>
          <div className={styles.textWrapper}>
            <Typography
              style={{
                marginBottom: "3px",
                display: "flex",
                justifyContent: "center",
                fontSize: "16px",
              }}
              variant="h6"
            >
              {data.node.enrichedName}
            </Typography>
            {dataTable.map((metric) => (
              <ConditionalWrapper
                key={metric.name}
                condition={metric.tooltip !== undefined}
                wrapper={(childern) =>
                  typeof metric.tooltip === "string" ? (
                    <Tooltip title={metric.tooltip}>{childern}</Tooltip>
                  ) : (
                    <TransperantTooltip title={metric.tooltip}>
                      {childern}
                    </TransperantTooltip>
                  )
                }
              >
                <Box
                  key={metric.name}
                  sx={
                    metric.showBlock
                      ? { justifyContent: "center", alignItems: "center" }
                      : { display: "flex", alignItems: "center" }
                  }
                >
                  <Typography sx={{ fontWeight: "bold" }} variant="body2">
                    {metric.name}:
                  </Typography>
                  <Typography sx={{ ml: 0.3, mt: 0, mb: 0, maxWidth: 260 }} variant="body2">
                    {metric.value}
                  </Typography>
                </Box>
              </ConditionalWrapper>
            ))}
          </div>
        </div>
        <AlertBadge alert={sqlNodeAlert} margin="20px" placement="top" />
        {(data.node.stage === undefined ||
          (data.node.stage?.type == 'onestage' && data.node.stage?.stageId === -1) ||
          (data.node.stage?.type == 'exchange' && data.node.stage?.readStage === -1 && data.node.stage?.writeStage === -1))
          ? undefined : <Box
            sx={{
              position: "absolute",
              top: "87%",
              right: "5%"
            }}
          >
            <ExpandMoreIcon color='info' style={{ width: "30px", height: "30px" }} onClick={() => dispatch(setSelectedStage({ selectedStage: data.node.stage }))} />
          </Box>}
        <Box
          sx={{
            position: "absolute",
            top: "87%",
            right: "85%",
          }}
        >
          <StageIcon stage={data.node.stage} />
        </Box>
        <Box
          sx={{
            position: "absolute",
            top: "89%",
            right: "42%",
            color: getBucketedColor(data.node.durationPercentage ?? 0),
          }}
        >
          <Tooltip
            sx={{ zIndex: 6 }}
            title={
              data.node.duration !== undefined
                ? humanizeTimeDiff(duration(data.node.duration))
                : undefined
            }
          >
            <Typography variant="body2">
              {data.node.durationPercentage !== undefined
                ? data.node.durationPercentage.toFixed(2) + "%"
                : ""}
            </Typography>
          </Tooltip>
        </Box>
      </Box>
      <Handle type="source" position={Position.Right} id="a" />
    </>
  );
};
function addTruncatedSmallTooltipMultiLine(
  dataTable: MetricWithTooltip[],
  name: string,
  value: string[],
  limit: number = 25,
  pushEnd: boolean = false,
) {
  addTruncatedSmallTooltip(
    dataTable,
    name,
    value.join(", "),
    limit,
    pushEnd,
    true,
  );
}

function addTruncatedCodeTooltipMultiline(
  dataTable: MetricWithTooltip[],
  name: string,
  value: string[],
  limit = 120,
  pushEnd: boolean = true,
  showBlock: boolean = true,

) {
  addTruncatedCodeTooltip(dataTable, name, value.join(",\n"), limit, pushEnd, showBlock);
}

function addTruncatedCodeTooltip(
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

function addTruncatedSmallTooltip(
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

function addTruncated(
  dataTable: MetricWithTooltip[],
  name: string,
  value: string,
) {
  dataTable.push({
    name: name,
    value: truncateMiddle(value, 120),
    showBlock: true,
  });
}
