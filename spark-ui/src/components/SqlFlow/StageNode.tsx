import React, { FC } from "react";
import { Handle, Position } from "reactflow";
import { SqlMetric } from "../../interfaces/SparkSQLs";
import { v4 as uuidv4 } from "uuid";
import styles from "./node-style.module.css";
import { EnrichedSqlNode } from "../../interfaces/AppStore";
import { Box, Typography } from "@mui/material";
import { truncateMiddle } from "../../reducers/PlanParsers/PlanParserUtils";

export const StageNodeName: string = "stageNode";

export const StageNode: FC<{ data: { node: EnrichedSqlNode } }> = ({
  data,
}): JSX.Element => {
  const dataTable = data.node.metrics.filter(
    (metric: SqlMetric) => !!metric.value,
  );
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
          });
        }
        dataTable.push({
          name: "File Path",
          value: truncateMiddle(parsedPlan.plan.location, 25),
        });
        dataTable.push({ name: "Format", value: parsedPlan.plan.format });
        dataTable.push({ name: "Mode", value: parsedPlan.plan.mode });
        if (parsedPlan.plan.partitionKeys !== undefined) {
          dataTable.push({
            name: "Partition Keys",
            value: truncateMiddle(parsedPlan.plan.partitionKeys.join(","), 25),
          });
        }
        break;
      case "FileScan":
        if (parsedPlan.plan.Location !== undefined) {
          dataTable.unshift({
            name: "File Path",
            value: truncateMiddle(parsedPlan.plan.Location, 25),
          });
        }
        if (parsedPlan.plan.tableName !== undefined) {
          dataTable.unshift({
            name: "Table",
            value: truncateMiddle(parsedPlan.plan.tableName, 25),
          });
        }
    }
  }
  return (
    <>
      <Handle type="target" position={Position.Left} id="b" />
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
          {dataTable.map((metric: SqlMetric) => {
            return (
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
            );
          })}
        </div>
      </div>
      <Handle type="source" position={Position.Right} id="a" />
    </>
  );
};
