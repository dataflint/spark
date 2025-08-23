import ErrorIcon from "@mui/icons-material/Error";
import WarningIcon from "@mui/icons-material/Warning";
import { Alert, AlertTitle, Box, Typography } from "@mui/material";
import React, { FC, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { Handle, Position } from "reactflow";
import { useAppSelector } from "../../Hooks";
import { EnrichedSqlNode } from "../../interfaces/AppStore";
import { TransperantTooltip } from "../AlertBadge/AlertBadge";
import MetricDisplay, { MetricWithTooltip } from "./MetricDisplay";
import {
  processBaseMetrics,
  processCachedStorageMetrics,
  processExchangeMetrics,
  processIcebergCommitMetrics,
  processInputNodeMetrics,
  processOutputNodeMetrics,
  processShuffleReadMetrics,
} from "./MetricProcessors";
import styles from "./node-style.module.css";
import NodeFooter from "./NodeFooter";
import NodeTypeIndicator from "./NodeTypeIndicator";
import PerformanceIndicator from "./PerformanceIndicator";
import PlanMetricsProcessor from "./PlanMetricsProcessor";

export const StageNodeName: string = "stageNode";

interface StageNodeProps {
  data: { sqlId: string; node: EnrichedSqlNode };
}

const StageNode: FC<StageNodeProps> = ({ data }) => {
  const [searchParams] = useSearchParams();
  const alerts = useAppSelector((state) => state.spark.alerts);

  // Memoized computations for better performance
  const { isHighlighted, sqlNodeAlert, allMetrics } = useMemo(() => {
    // Parse nodeIds from URL parameters
    const nodeIdsParam = searchParams.get('nodeids');
    const highlightedNodeIds = nodeIdsParam
      ? nodeIdsParam.split(',').map(id => parseInt(id.trim(), 10)).filter(id => !isNaN(id))
      : [];

    // Check if current node should be highlighted
    const highlighted = highlightedNodeIds.includes(data.node.nodeId);

    // Find any alerts for this node
    const alert = alerts?.alerts.find(
      (alert) =>
        alert.source.type === "sql" &&
        alert.source.sqlNodeId === data.node.nodeId &&
        alert.source.sqlId === data.sqlId,
    );

    // Process all metrics
    const metrics: MetricWithTooltip[] = [
      ...processBaseMetrics(data.node),
      ...processCachedStorageMetrics(data.node),
      ...processIcebergCommitMetrics(data.node),
      ...processExchangeMetrics(data.node),
      ...processShuffleReadMetrics(data.node),
      ...processInputNodeMetrics(data.node),
      ...processOutputNodeMetrics(data.node),
    ];

    // Process plan-specific metrics
    if (data.node.parsedPlan) {
      metrics.push(...PlanMetricsProcessor.processPlanMetrics(data.node.parsedPlan));
    }

    return {
      isHighlighted: highlighted,
      sqlNodeAlert: alert,
      allMetrics: metrics,
    };
  }, [data.node, data.sqlId, searchParams, alerts]);

  const nodeClass = isHighlighted ? styles.nodeHighlighted : styles.node;

  return (
    <>
      <Handle
        type="target"
        position={Position.Left}
        id="b"
        className={styles.handleLeft}
        aria-label="Input connection"
      />

      <Box className={nodeClass} role="article" aria-label={`SQL node: ${data.node.enrichedName}`}>
        {/* Performance indicator bar */}
        <PerformanceIndicator durationPercentage={data.node.durationPercentage} />

        {/* Node type indicator */}
        <NodeTypeIndicator nodeType={data.node.type} nodeName={data.node.nodeName} />

        {/* Alert badge */}
        {sqlNodeAlert && (
          <TransperantTooltip
            placement="left"
            title={
              <React.Fragment>
                <Alert
                  severity={sqlNodeAlert.type}
                  icon={sqlNodeAlert.type === "warning" ? <WarningIcon /> : <ErrorIcon />}
                >
                  <AlertTitle>{sqlNodeAlert.title}</AlertTitle>
                  {sqlNodeAlert.message}
                  {sqlNodeAlert.shortSuggestion !== undefined && (
                    <>
                      <br />
                      <b>Recommended Fix:</b>
                      <br />
                      {sqlNodeAlert.shortSuggestion}
                    </>
                  )}
                </Alert>
              </React.Fragment>
            }
          >
            <Box className={`${styles.alertBadgeContainer} ${styles[sqlNodeAlert.type]}`}>
              {sqlNodeAlert.type === "warning" ? (
                <WarningIcon
                  sx={{
                    color: "#ff9100",
                    fontSize: "16px",
                    filter: "drop-shadow(0 1px 2px rgba(0, 0, 0, 0.1))",
                  }}
                />
              ) : (
                <ErrorIcon
                  sx={{
                    color: "#bf360c",
                    fontSize: "16px",
                    filter: "drop-shadow(0 1px 2px rgba(0, 0, 0, 0.1))",
                  }}
                />
              )}
            </Box>
          </TransperantTooltip>
        )}

        {/* Header with title */}
        <Box className={styles.nodeHeader}>
          <Typography className={styles.nodeTitle} variant="h6" component="h3">
            {data.node.enrichedName}
          </Typography>
        </Box>

        {/* Metrics content */}
        <MetricDisplay metrics={allMetrics} />

        {/* Footer with controls */}
        <NodeFooter
          stage={data.node.stage}
          duration={data.node.duration}
          durationPercentage={data.node.durationPercentage}
        />
      </Box>

      <Handle
        type="source"
        position={Position.Right}
        id="a"
        className={styles.handleRight}
        aria-label="Output connection"
      />
    </>
  );
};

export { StageNode };
