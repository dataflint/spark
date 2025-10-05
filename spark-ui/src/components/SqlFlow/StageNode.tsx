import ErrorIcon from "@mui/icons-material/Error";
import WarningIcon from "@mui/icons-material/Warning";
import { Alert, AlertTitle, Box, Typography } from "@mui/material";
import React, { FC, memo, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { Handle, Position } from "reactflow";
import { Alert as AppAlert, EnrichedSqlNode } from "../../interfaces/AppStore";
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
  data: {
    sqlId: string;
    node: EnrichedSqlNode;
    sqlUniqueId?: string;
    sqlMetricUpdateId?: string;
    alert?: AppAlert; // Alert for this specific node
  };
}

const StageNodeComponent: FC<StageNodeProps> = ({ data }) => {
  const [searchParams] = useSearchParams();

  // Memoized computations for better performance
  const { isHighlighted, allMetrics, hasDeltaOptimizeWrite } = useMemo(() => {
    // Parse nodeIds from URL parameters
    const nodeIdsParam = searchParams.get('nodeids');
    const highlightedNodeIds = nodeIdsParam
      ? nodeIdsParam.split(',').map(id => parseInt(id.trim(), 10)).filter(id => !isNaN(id))
      : [];

    // Check if current node should be highlighted
    const highlighted = highlightedNodeIds.includes(data.node.nodeId);

    // Check if this node has delta optimize write enabled
    const hasDeltaOptimize =
      data.node.nodeName === "Exchange" &&
      data.node.parsedPlan?.type === "Exchange" &&
      data.node.parsedPlan.plan.deltaOptimizeWrite !== undefined;

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
      allMetrics: metrics,
      hasDeltaOptimizeWrite: hasDeltaOptimize,
    };
  }, [
    // Use SQL identifiers for optimal memoization when available
    data.sqlUniqueId || data.node.nodeId,
    data.sqlMetricUpdateId || data.node.metrics,
    data.sqlId,
    searchParams
  ]);

  // Use the alert passed in data prop
  const sqlNodeAlert = data.alert;

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
          {hasDeltaOptimizeWrite && (
            <Box
              sx={{
                display: "inline-block",
                ml: 1,
                px: 0.75,
                py: 0.25,
                backgroundColor: "#1976d2",
                color: "white",
                borderRadius: "4px",
                fontSize: "0.65rem",
                fontWeight: "bold",
                textTransform: "uppercase",
                letterSpacing: "0.5px",
              }}
            >
              Δ Optimized
            </Box>
          )}
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

// Simple memoization - prevents unnecessary re-renders
const StageNode = memo(StageNodeComponent);

export { StageNode };
