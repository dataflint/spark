import ErrorIcon from "@mui/icons-material/Error";
import WarningIcon from "@mui/icons-material/Warning";
import { Alert, AlertTitle, Box, Typography } from "@mui/material";
import React, { FC, memo, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { Handle, Position } from "reactflow";
import { Alert as AppAlert, EnrichedSqlNode, SQLNodeExchangeStageData, SQLNodeStageData } from "../../interfaces/AppStore";
import { humanFileSize, parseBytesString } from "../../utils/FormatUtils";
import { TransperantTooltip } from "../AlertBadge/AlertBadge";
import MetricDisplay, { MetricWithTooltip } from "./MetricDisplay";
import {
  addTruncatedCodeTooltip,
  processBaseMetrics,
  processCachedStorageMetrics,
  processDeltaLakeScanMetrics,
  processExchangeMetrics,
  processIcebergCommitMetrics,
  processInputNodeMetrics,
  processOptimizeTableMetrics,
  processOutputNodeMetrics,
  processShuffleReadMetrics,
} from "./MetricProcessors";
import styles from "./node-style.module.css";
import { DebugButton } from "./NodeDebugDialog";
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
    exchangeVariant?: "write" | "read"; // For split exchange nodes
  };
}

const StageNodeComponent: FC<StageNodeProps> = ({ data }) => {
  const [searchParams] = useSearchParams();
  const exchangeVariant = data.exchangeVariant;
  const isDebugMode = localStorage.getItem("DATAFLINT_DEBUG_MODE_VIEW") === "true";

  // Memoized computations for better performance
  const { isHighlighted, allMetrics, hasDeltaOptimizeWrite, displayName, variantStage, variantDuration, variantDurationPercentage } = useMemo(() => {
    // Parse nodeIds from URL parameters
    const nodeIdsParam = searchParams.get('nodeids');
    const highlightedNodeIds = nodeIdsParam
      ? nodeIdsParam.split(',').map(id => parseInt(id.trim(), 10)).filter(id => !isNaN(id))
      : [];

    // Parse stageId from URL parameters (support both lowercase and camelCase)
    const stageIdParam = searchParams.get('stageid') || searchParams.get('stageId');
    const highlightedStageId = stageIdParam ? parseInt(stageIdParam.trim(), 10) : null;

    // Check if current node should be highlighted by nodeId
    const highlightedByNodeId = highlightedNodeIds.includes(data.node.nodeId);

    // Check if current node should be highlighted by stageId
    // For single-stage nodes, check the stageId
    // For exchange nodes, check both readStage and writeStage (or specific one for split nodes)
    let highlightedByStageId = false;
    if (highlightedStageId !== null && data.node.stage) {
      if (data.node.stage.type === 'onestage') {
        highlightedByStageId = data.node.stage.stageId === highlightedStageId;
      } else if (data.node.stage.type === 'exchange') {
        if (exchangeVariant === "write") {
          highlightedByStageId = data.node.stage.writeStage === highlightedStageId;
        } else if (exchangeVariant === "read") {
          highlightedByStageId = data.node.stage.readStage === highlightedStageId;
        } else {
          // Non-split exchange nodes are highlighted if either read or write stage matches
          highlightedByStageId =
            data.node.stage.readStage === highlightedStageId ||
            data.node.stage.writeStage === highlightedStageId;
        }
      }
    }

    // Node is highlighted if either condition is true
    const highlighted = highlightedByNodeId || highlightedByStageId;

    // Check if this node has delta optimize write enabled
    const hasDeltaOptimize =
      data.node.nodeName === "Exchange" &&
      data.node.parsedPlan?.type === "Exchange" &&
      data.node.parsedPlan.plan.deltaOptimizeWrite !== undefined;

    // Process all metrics
    let metrics: MetricWithTooltip[] = [];
    const baseMetrics = processBaseMetrics(data.node);
    const findMetric = (pattern: string) => baseMetrics.find(m => m.name.toLowerCase().includes(pattern));

    if (exchangeVariant === "write") {
      // Write: Partitions → Records Written → Shuffle Write → Avg Size
      const partitions = findMetric("partitions");
      const records = findMetric("shuffle records written") || findMetric("records written");
      const bytes = baseMetrics.find(m => m.name.toLowerCase().includes("shuffle write") && !m.name.toLowerCase().includes("records"));
      const avg = processExchangeMetrics(data.node).find(m => m.name.toLowerCase().includes("avg"));

      if (partitions) metrics.push({ name: "Partitions", value: partitions.value });
      if (records) metrics.push(records);
      if (bytes) metrics.push(bytes);
      if (avg) metrics.push(avg);
    } else if (exchangeVariant === "read") {
      // Read: Partitions → Records Read → Shuffle Read → Avg Size
      const partitions = findMetric("partitions");
      const records = findMetric("shuffle records read");
      const localBytes = parseBytesString(findMetric("shuffle read (local)")?.value || "0");
      const remoteBytes = parseBytesString(findMetric("shuffle read (remote)")?.value || "0");
      const totalBytes = localBytes + remoteBytes;
      const numPartitions = parseInt(partitions?.value?.replace(/,/g, '') || '0', 10);

      if (partitions) metrics.push({ name: "Partitions", value: partitions.value });
      if (records) metrics.push(records);
      if (totalBytes > 0) {
        metrics.push({ name: "Shuffle Read", value: humanFileSize(totalBytes) });
        if (numPartitions > 0) {
          metrics.push({ name: "Avg Read Partition Size", value: humanFileSize(totalBytes / numPartitions) });
        }
      }
    } else {
      // Non-split node: show all metrics
      let allBaseMetrics = processBaseMetrics(data.node);

      // For Exchange nodes not in a stage (non-split), filter out shuffle read metrics
      // since they're only relevant when showing the read side separately
      const isNonSplitExchange = data.node.nodeName === "Exchange" && !exchangeVariant;
      if (isNonSplitExchange) {
        const shuffleReadMetricPatterns = [
          "shuffle records read",
          "shuffle read",
          "fetch wait time",
          "blocks read",
          "remote reqs",
          "remote merged reqs",
          "local blocks",
          "remote blocks",
        ];
        allBaseMetrics = allBaseMetrics.filter(m => {
          const nameLower = m.name.toLowerCase();
          return !shuffleReadMetricPatterns.some(pattern => nameLower.includes(pattern));
        });
      }

      const inputMetrics = processInputNodeMetrics(data.node);
      metrics = [
        ...inputMetrics,
        ...allBaseMetrics,
        ...processCachedStorageMetrics(data.node),
        ...processIcebergCommitMetrics(data.node),
        ...processDeltaLakeScanMetrics(data.node),
        ...processExchangeMetrics(data.node),
        ...processShuffleReadMetrics(data.node),
        ...processOutputNodeMetrics(data.node),
        ...processOptimizeTableMetrics(data.node),
      ];
    }

    // Process plan-specific metrics (for non-split or write variant only)
    if (data.node.parsedPlan && exchangeVariant !== "read") {
      metrics.push(...PlanMetricsProcessor.processPlanMetrics(data.node.parsedPlan));
    }

    // For read variant of Exchange, show hash/range partitioning fields as code block
    if (exchangeVariant === "read" && data.node.parsedPlan?.type === "Exchange") {
      const plan = data.node.parsedPlan.plan;
      if (plan.fields && plan.fields.length > 0 && (plan.type === "hashpartitioning" || plan.type === "rangepartitioning")) {
        const fieldLabel = plan.type === "hashpartitioning"
          ? (plan.fields.length === 1 ? "Hashed Field" : "Hashed Fields")
          : (plan.fields.length === 1 ? "Ranged Field" : "Ranged Fields");
        // Format as multi-line code block (same as write side)
        addTruncatedCodeTooltip(metrics, fieldLabel, plan.fields.join(",\n"), 120, true, true);
      }
    }

    const tableName =
      data.node.parsedPlan?.type === "FileScan"
        ? data.node.parsedPlan.plan.tableName
        : undefined;
    const isIcebergRead =
      data.node.parsedPlan?.type === "FileScan" &&
      data.node.parsedPlan.plan.isIcebergRead;
    if (tableName && isIcebergRead) {
      metrics = [
        { name: "Table Name", value: tableName },
        ...metrics.filter((metric) => metric.name !== "Table Name"),
      ];
    }

    // Keep original name - use icon indicator instead of text suffix
    const name = data.node.enrichedName;

    // Create variant-specific stage for footer (only show relevant stage for split nodes)
    let stage: SQLNodeStageData | SQLNodeExchangeStageData | undefined = data.node.stage;
    let duration: number | undefined = data.node.duration;
    let durationPct: number | undefined = data.node.durationPercentage;

    if (exchangeVariant && data.node.stage?.type === 'exchange') {
      const exchangeStage = data.node.stage as SQLNodeExchangeStageData;
      const exchangeMetrics = data.node.exchangeMetrics;
      const originalDuration = data.node.duration;
      const originalDurationPct = data.node.durationPercentage;

      if (exchangeVariant === "write") {
        // Create a single-stage representation for write stage
        stage = {
          type: "onestage" as const,
          stageId: exchangeStage.writeStage,
          status: exchangeStage.status,
          stageDuration: 0,
          restOfStageDuration: undefined,
        };
        // Use shuffle write time for write node duration
        if (exchangeMetrics) {
          const writeDuration = exchangeMetrics.writeDuration;
          const totalExchangeDuration = exchangeMetrics.duration;
          duration = writeDuration;
          // Calculate percentage proportionally, or use original if total is 0
          if (totalExchangeDuration > 0 && originalDurationPct !== undefined) {
            durationPct = (writeDuration / totalExchangeDuration) * originalDurationPct;
          } else if (writeDuration > 0 && originalDuration !== undefined && originalDuration > 0) {
            // Fallback: estimate percentage based on write duration ratio
            durationPct = originalDurationPct;
          } else {
            durationPct = originalDurationPct;
          }
        } else {
          // No exchange metrics: use original duration/percentage
          duration = originalDuration;
          durationPct = originalDurationPct;
        }
      } else if (exchangeVariant === "read") {
        // Create a single-stage representation for read stage
        stage = {
          type: "onestage" as const,
          stageId: exchangeStage.readStage,
          status: exchangeStage.status,
          stageDuration: 0,
          restOfStageDuration: undefined,
        };
        // Use fetch wait time for read node duration
        if (exchangeMetrics) {
          const readDuration = exchangeMetrics.readDuration;
          const totalExchangeDuration = exchangeMetrics.duration;
          duration = readDuration;
          // Calculate percentage proportionally, or use original if total is 0
          if (totalExchangeDuration > 0 && originalDurationPct !== undefined) {
            durationPct = (readDuration / totalExchangeDuration) * originalDurationPct;
          } else if (readDuration > 0 && originalDuration !== undefined && originalDuration > 0) {
            // Fallback: estimate percentage based on read duration ratio
            durationPct = originalDurationPct;
          } else {
            durationPct = originalDurationPct;
          }
        } else {
          // No exchange metrics: use original duration/percentage
          duration = originalDuration;
          durationPct = originalDurationPct;
        }
      }
    }

    return {
      isHighlighted: highlighted,
      allMetrics: metrics,
      hasDeltaOptimizeWrite: hasDeltaOptimize && exchangeVariant !== "read",
      displayName: name,
      variantStage: stage,
      variantDuration: duration,
      variantDurationPercentage: durationPct,
    };
  }, [
    // Use SQL identifiers for optimal memoization when available
    data.sqlUniqueId || data.node.nodeId,
    data.sqlMetricUpdateId || data.node.metrics,
    data.sqlId,
    searchParams,
    exchangeVariant
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

      <Box className={nodeClass} role="article" aria-label={`SQL node: ${displayName}`}>
        {/* Performance indicator bar */}
        <PerformanceIndicator durationPercentage={data.node.durationPercentage} />

        {/* Node type indicator - only show for non-exchange nodes */}
        {!exchangeVariant && (
          <NodeTypeIndicator nodeType={data.node.type} nodeName={data.node.nodeName} />
        )}

        {/* Debug button - only visible when DATAFLINT_DEBUG_MODE_VIEW is enabled */}
        {isDebugMode && (
          <Box
            sx={{
              position: "absolute",
              bottom: 8,
              right: 8,
              zIndex: 15,
            }}
          >
            <DebugButton
              nodeData={data}
              title={`Node ${data.node.nodeId}: ${displayName}`}
            />
          </Box>
        )}

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
        <Box className={styles.nodeHeader} sx={exchangeVariant ? { flexDirection: 'column', py: 0.5 } : undefined}>
          <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Typography className={styles.nodeTitle} variant="h6" component="h3">
              {displayName}
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
          {/* Subtitle for exchange nodes */}
          {exchangeVariant && (
            <Typography
              sx={{
                fontSize: "0.75rem",
                color: exchangeVariant === "write" ? "#ff9800" : "#42a5f5",
                fontWeight: 600,
                mt: 0.25,
                textTransform: "uppercase",
                letterSpacing: "0.5px",
              }}
            >
              {exchangeVariant === "write" ? "↑ shuffle write" : "↓ shuffle read"}
            </Typography>
          )}
        </Box>

        {/* Metrics content */}
        <MetricDisplay metrics={allMetrics} />

        {/* Footer with controls */}
        <NodeFooter
          stage={variantStage}
          duration={variantDuration}
          durationPercentage={variantDurationPercentage}
          nodeName={data.node.nodeName}
          hideStageDetails={!!exchangeVariant}
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
