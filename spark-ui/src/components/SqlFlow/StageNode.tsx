import CheckIcon from '@mui/icons-material/Check';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import PendingIcon from '@mui/icons-material/Pending';
import { Box, CircularProgress, Tooltip, Typography } from "@mui/material";
import { duration } from 'moment';
import React, { FC } from "react";
import SyntaxHighlighter from 'react-syntax-highlighter';
import { a11yDark } from 'react-syntax-highlighter/dist/esm/styles/hljs';
import { Handle, Position } from "reactflow";
import { useAppSelector } from "../../Hooks";
import { EnrichedSqlNode, SQLNodeExchangeStageData, SQLNodeStageData } from "../../interfaces/AppStore";
import { SqlMetric } from "../../interfaces/SparkSQLs";
import { truncateMiddle } from "../../reducers/PlanParsers/PlanParserUtils";
import { humanizeTimeDiff } from '../../utils/FormatUtils';
import AlertBadge, { TransperantTooltip } from "../AlertBadge/AlertBadge";
import { ConditionalWrapper } from "../InfoBox/InfoBox";
import styles from "./node-style.module.css";

export const StageNodeName: string = "stageNode";

interface MetricWithTooltip {
  name: string
  value: string
  tooltip?: string | JSX.Element
  showBlock?: boolean
  showSyntax?: boolean
}


function getBucketedColor(percentage: number): string {
  if (percentage > 100) {
    percentage = 100;
  }

  // Determine the bucket
  let bucket = Math.floor(percentage / 10);

  // Assign a color to each bucket
  switch (bucket) {
    case 0: return "#006400"; // Dark green
    case 1: return "#228B22"; // Forest green
    case 2: return "#6B8E23"; // Olive drab
    case 3: return "#B8860B"; // Dark goldenrod
    case 4: // Fall through
    case 5: // Fall through
    case 6: // Fall through
    case 7: // Fall through
    case 8: // Fall through
    case 9: // Fall through
    case 10: return "#FF4500"; // Red-orange
    default: return "#FF0000"; // Red
  }
}

const getStatusIcon = (status: string): JSX.Element => {
  switch (status) {
    case "ACTIVE":
      return (
        <CircularProgress
          color="info"
          style={{ width: "30px", height: "30px" }}
        />
      );
    case "COMPLETE":
      return (
        <CheckIcon color="success" style={{ width: "30px", height: "30px" }} />
      );
    case "FAILED":
      return (
        <ErrorOutlineIcon
          color="error"
          style={{ width: "30px", height: "30px" }}
        />
      );
    case "PENDING":
      return <PendingIcon
        sx={{ color: "#b2a300" }}
        style={{ width: "30px", height: "30px" }}
      />
        ;
    default:
      return <div></div>
  }
}

const StageIcon: FC<{ stage: SQLNodeStageData | SQLNodeExchangeStageData | undefined }> = ({ stage }): JSX.Element => {
  if (stage === undefined)
    return <div></div>

  const text = stage.type === "onestage" ?
    `Stage ${stage.stageId}` :
    `Write Stage: ${stage.writeStage}\n, Read Stage: ${stage.readStage}`

  return <Tooltip sx={{ zIndex: 6 }} title={text}>
    {getStatusIcon(stage.status)}
  </Tooltip>
}

export const StageNode: FC<{
  data: { sqlId: string; node: EnrichedSqlNode };
}> = ({ data }): JSX.Element => {
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
        addTruncatedCodeTooltip(dataTable, "Condition", parsedPlan.plan.condition);
        break;
      case "TakeOrderedAndProject":
        dataTable.push({
          name: "Limit",
          value: parsedPlan.plan.limit.toString(),
        });
        break;
      case "WriteToHDFS":
        if (parsedPlan.plan.tableName !== undefined) {
          addTruncatedSmallTooltip(dataTable, "Table Name", parsedPlan.plan.tableName);
        }
        addTruncatedSmallTooltip(dataTable, "File Path", parsedPlan.plan.location);
        dataTable.push({ name: "Format", value: parsedPlan.plan.format });
        dataTable.push({ name: "Mode", value: parsedPlan.plan.mode });
        if (parsedPlan.plan.partitionKeys !== undefined) {
          addTruncatedSmallTooltipMultiLine(dataTable, "Partition By", parsedPlan.plan.partitionKeys);
        }
        break;
      case "FileScan":
        if (parsedPlan.plan.Location !== undefined) {
          addTruncatedSmallTooltip(dataTable, "File Path", parsedPlan.plan.Location);
        }
        if (parsedPlan.plan.tableName !== undefined) {
          addTruncatedSmallTooltip(dataTable, "Table", parsedPlan.plan.tableName);
        }
        break;
      case "Exchange":
        if (parsedPlan.plan.fields !== undefined && parsedPlan.plan.fields.length > 0) {
          addTruncatedSmallTooltipMultiLine(dataTable, parsedPlan.plan.type === "hashpartitioning" ?
            (parsedPlan.plan.fields.length === 1 ? "hashed field" : "hashed fields") :
            (parsedPlan.plan.fields.length === 1 ? "ranged field" : "ranged fields"), parsedPlan.plan.fields)
        }
        break;
      case "Project":
        if (parsedPlan.plan.fields !== undefined) {
          addTruncatedCodeTooltipMultiline(dataTable, "Selected Fields", parsedPlan.plan.fields)
        }
        break;
      case "HashAggregate":
        if (parsedPlan.plan.keys !== undefined && parsedPlan.plan.keys.length > 0) {
          addTruncatedCodeTooltipMultiline(dataTable, "Aggregate By", parsedPlan.plan.keys)
        }
        if (parsedPlan.plan.functions !== undefined && parsedPlan.plan.functions.length > 0) {
          addTruncatedCodeTooltipMultiline(dataTable, "Expression", parsedPlan.plan.functions)
        }
        break;
      case "Sort":
        if (parsedPlan.plan.fields !== undefined && parsedPlan.plan.fields.length > 0) {
          addTruncatedSmallTooltipMultiLine(dataTable, "Sort by", parsedPlan.plan.fields)
        }
        break;
      case "Join":
        dataTable.push({
          name: "Join Type",
          value: parsedPlan.plan.joinSideType
        });

        if (parsedPlan.plan.leftKeys !== undefined && parsedPlan.plan.leftKeys.length > 0) {
          dataTable.push({
            name: parsedPlan.plan.leftKeys.length > 1 ? "Left Side Keys" : "Left Side Key",
            value: truncateMiddle(parsedPlan.plan.leftKeys.join(", "), 25),
            tooltip: parsedPlan.plan.leftKeys.length > 25 ? parsedPlan.plan.leftKeys.join(", ") : undefined
          });
        }
        if (parsedPlan.plan.rightKeys !== undefined && parsedPlan.plan.rightKeys.length > 0) {
          dataTable.push({
            name: parsedPlan.plan.rightKeys.length > 1 ? "Right Side Keys" : "Right Side Key",
            value: truncateMiddle(parsedPlan.plan.rightKeys.join(", "), 25),
            tooltip: parsedPlan.plan.rightKeys.length > 25 ? parsedPlan.plan.rightKeys.join(", ") : undefined
          });
        }
        if (parsedPlan.plan.joinCondition !== undefined && parsedPlan.plan.joinCondition !== "") {
          addTruncatedCodeTooltip(dataTable, "Join Condition", parsedPlan.plan.joinCondition);
        }
        break;
    }
  }

  return (
    <>
      <Handle type="target" position={Position.Left} id="b" />
      <Box position="relative" width={280} height={220}>
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
                wrapper={(childern) => (
                  typeof metric.tooltip === "string" ?
                    <Tooltip title={metric.tooltip}>{childern}</Tooltip> :
                    <TransperantTooltip title={metric.tooltip}>
                      {childern}
                    </TransperantTooltip>
                )}
              >
                <Box
                  key={metric.name}
                  sx={metric.showBlock ? { justifyContent: "center", alignItems: "center" } : { display: "flex", alignItems: "center" }}
                >
                  <Typography sx={{ fontWeight: "bold" }} variant="body2">
                    {metric.name}:
                  </Typography>
                  <Typography sx={{ ml: 0.3, mt: 0, mb: 0 }} variant="body2">
                    {metric.value}
                  </Typography>
                </Box>
              </ConditionalWrapper>
            ))}
          </div>
        </div>
        <AlertBadge alert={sqlNodeAlert} margin="20px" placement="top" />
        <Box sx={{
          position: "absolute",
          top: "80%",
          right: "85%",
        }}>
          <StageIcon stage={data.node.stage} />
        </Box>
        <Box sx={{
          position: "absolute",
          top: "85%",
          right: "42%",
          color: getBucketedColor(data.node.durationPercentage ?? 0)
        }}>
          <Tooltip sx={{ zIndex: 6 }} title={data.node.duration !== undefined ? humanizeTimeDiff(duration(data.node.duration)) : undefined}>
            <Typography variant="body2">
              {data.node.durationPercentage !== undefined ? data.node.durationPercentage.toFixed(2) + "%" : ""}
            </Typography>
          </Tooltip>
        </Box>
      </Box >
      <Handle type="source" position={Position.Right} id="a" />
    </>
  );
};
function addTruncatedSmallTooltipMultiLine(dataTable: MetricWithTooltip[], name: string, value: string[], limit: number = 25, pushEnd: boolean = false) {
  addTruncatedSmallTooltip(dataTable, name, value.join(", "), limit, pushEnd, true);
}

function addTruncatedCodeTooltipMultiline(dataTable: MetricWithTooltip[], name: string, value: string[]) {
  addTruncatedCodeTooltip(dataTable, name, value.join(",\n"));
}

function addTruncatedCodeTooltip(dataTable: MetricWithTooltip[], name: string, value: string, limit: number = 120, pushEnd: boolean = true, showBlock: boolean = true) {
  const element = {
    name: name,
    value: truncateMiddle(value, limit),
    tooltip: (
      <React.Fragment>
        <SyntaxHighlighter language="sql" style={a11yDark} customStyle={{
          fontSize: "1em"
        }} wrapLongLines>
          {value}
        </SyntaxHighlighter>
      </React.Fragment>
    ),
    showBlock: showBlock,
    showSyntax: true,
  };
  pushEnd ? dataTable.push(element) : dataTable.unshift(element);
}

function addTruncatedSmallTooltip(dataTable: MetricWithTooltip[], name: string, value: string, limit: number = 25, pushEnd: boolean = false, showBlock: boolean = false, showSyntax: boolean = false) {
  const element = {
    name: name,
    value: truncateMiddle(value, limit),
    tooltip: value.length > limit ? value : undefined,
    showBlock: showBlock,
    showSyntax: showSyntax,
  };
  pushEnd ? dataTable.push(element) : dataTable.unshift(element);
}

function addTruncated(dataTable: MetricWithTooltip[], name: string, value: string) {
  dataTable.push({
    name: name,
    value: truncateMiddle(value, 120),
    showBlock: true
  });
}

