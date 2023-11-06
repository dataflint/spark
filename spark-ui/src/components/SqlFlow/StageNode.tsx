import { Box, Tooltip, Typography } from "@mui/material";
import React, { FC } from "react";
import { Handle, Position } from "reactflow";
import { useAppSelector } from "../../Hooks";
import { EnrichedSqlNode } from "../../interfaces/AppStore";
import { SqlMetric } from "../../interfaces/SparkSQLs";
import { truncateMiddle } from "../../reducers/PlanParsers/PlanParserUtils";
import AlertBadge from "../AlertBadge/AlertBadge";
import { ConditionalWrapper } from "../InfoBox/InfoBox";
import styles from "./node-style.module.css";

export const StageNodeName: string = "stageNode";

interface MetricWithTooltip {
  name: string
  value: string
  tooltip?: string
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
      case "TakeOrderedAndProject":
        dataTable.push({
          name: "Limit",
          value: parsedPlan.plan.limit.toString(),
        });
        break;
      case "WriteToHDFS":
        if (parsedPlan.plan.tableName !== undefined) {
          dataTable.push({
            name: "Table name",
            value: truncateMiddle(parsedPlan.plan.tableName, 25),
            tooltip: parsedPlan.plan.tableName.length > 25 ? parsedPlan.plan.tableName : undefined
          });
        }
        dataTable.push({
          name: "File Path",
          value: truncateMiddle(parsedPlan.plan.location, 25),
          tooltip: parsedPlan.plan.location.length > 25 ? parsedPlan.plan.location : undefined

        });
        dataTable.push({ name: "Format", value: parsedPlan.plan.format });
        dataTable.push({ name: "Mode", value: parsedPlan.plan.mode });
        if (parsedPlan.plan.partitionKeys !== undefined) {
          dataTable.push({
            name: "Partition Keys",
            value: truncateMiddle(parsedPlan.plan.partitionKeys.join(","), 25),
            tooltip: parsedPlan.plan.partitionKeys.length > 25 ? parsedPlan.plan.partitionKeys.join(",") : undefined
          });
        }
        break;
      case "FileScan":
        if (parsedPlan.plan.Location !== undefined) {
          dataTable.unshift({
            name: "File Path",
            value: truncateMiddle(parsedPlan.plan.Location, 25),
            tooltip: parsedPlan.plan.Location
          });
        }
        if (parsedPlan.plan.tableName !== undefined) {
          dataTable.unshift({
            name: "Table",
            value: truncateMiddle(parsedPlan.plan.tableName, 25),
            tooltip: parsedPlan.plan.tableName
          });
        }
    }
  }
  return (
    <>
      <Handle type="target" position={Position.Left} id="b" />
      <Box position="relative">
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
                  <Tooltip title={metric.tooltip}>{childern}</Tooltip>
                )}
              >
                <Box
                  key={metric.name}
                  sx={{ display: "flex", alignItems: "center" }}
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
      </Box>
      <Handle type="source" position={Position.Right} id="a" />
    </>
  );
};
