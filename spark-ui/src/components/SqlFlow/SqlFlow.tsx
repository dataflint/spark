import React, { FC, useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  ConnectionLineType,
  Controls,
  ReactFlowInstance,
  addEdge,
  useEdgesState,
  useNodesState,
} from "reactflow";

import { CenterFocusStrong, ExpandLess, ExpandMore, Layers, LayersClear, Speed, Storage, Warning, ZoomIn, ZoomOut } from "@mui/icons-material";
import {
  Alert,
  Box,
  CircularProgress,
  Collapse,
  Drawer,
  Fade,
  IconButton,
  Paper,
  Tooltip,
  Typography
} from "@mui/material";
import { useSearchParams } from "react-router-dom";
import "reactflow/dist/style.css";
import { useAppDispatch, useAppSelector } from "../../Hooks";
import { EnrichedSparkSQL, GraphFilter } from "../../interfaces/AppStore";
import { setSQLMode, setSelectedStage, setShowStages } from "../../reducers/GeneralSlice";
import { parseBytesString } from "../../utils/FormatUtils";
import FlowLegend from "./FlowLegend";
import CustomMiniMap from "./MiniMap";
import SqlLayoutService from "./SqlLayoutService";
import { StageGroupNode, StageGroupNodeName } from "./StageGroupNode";
import StageIconDrawer from "./StageIconDrawer";
import { StageNode, StageNodeName } from "./StageNode";

const options = { hideAttribution: true };
const nodeTypes = {
  [StageNodeName]: StageNode,
  [StageGroupNodeName]: StageGroupNode,
};

// Types for navigation data
interface NodeWithAlert {
  nodeId: string;
  position: { x: number; y: number };
  alertType: "warning" | "error";
}

interface BiggestDurationNode {
  nodeId: string;
  position: { x: number; y: number };
  durationPercentage: number;
}

interface NodeWithSpill {
  nodeId: string;
  position: { x: number; y: number };
  spillSize: number;
  spillSizeFormatted: string;
}

interface NavigationData {
  nodesWithAlerts: NodeWithAlert[];
  biggestDurationNode: BiggestDurationNode | null;
  nodesByDuration: BiggestDurationNode[];
  nodesWithSpill: NodeWithSpill[];
}

const SqlFlow: FC<{ sparkSQL: EnrichedSparkSQL }> = ({
  sparkSQL,
}): JSX.Element => {
  // Get alerts and stages for passing to nodes
  const alerts = useAppSelector((state) => state.spark.alerts);
  const stages = useAppSelector((state) => state.spark.stages);

  const [instance, setInstance] = useState<ReactFlowInstance | undefined>();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchParams] = useSearchParams();
  const nodeIdsParam = searchParams.get('nodeids');
  // Support both lowercase and camelCase for stageid parameter
  const stageIdParam = searchParams.get('stageid') || searchParams.get('stageId');
  const initialFocusApplied = useRef<string | null>(null);
  const [currentAlertIndex, setCurrentAlertIndex] = useState(0);
  const [currentDurationIndex, setCurrentDurationIndex] = useState(0);
  const [currentSpillIndex, setCurrentSpillIndex] = useState(0);
  const [viewModeExpanded, setViewModeExpanded] = useState(false);

  const dispatch = useAppDispatch();
  const graphFilter = useAppSelector((state) => state.general.sqlMode);
  const selectedStage = useAppSelector((state) => state.general.selectedStage);
  const showStages = useAppSelector((state) => state.general.showStages);

  // Memoized statistics about the SQL flow
  const flowStats = useMemo(() => {
    if (!sparkSQL || !nodes.length) return null;

    const totalNodes = nodes.length;
    const totalEdges = edges.length;
    const highlightedNodes = nodeIdsParam
      ? nodeIdsParam.split(',').filter(id => id.trim()).length
      : 0;

    return { totalNodes, totalEdges, highlightedNodes };
  }, [nodes, edges, nodeIdsParam, sparkSQL]);

  // Memoized calculations for navigation features
  const navigationData = useMemo((): NavigationData => {
    if (!sparkSQL || !nodes.length) return { nodesWithAlerts: [], biggestDurationNode: null, nodesByDuration: [], nodesWithSpill: [] };

    // Find nodes with alerts - include both individual node alerts AND stage group alerts
    const nodesWithAlerts: NodeWithAlert[] = [];

    // Add individual node alerts (exclude stage group nodes)
    nodes
      .filter(node => node.data?.alert && node.type !== StageGroupNodeName)
      .forEach(node => {
        // Calculate absolute position
        let x = node.position.x;
        let y = node.position.y;
        if (node.parentNode) {
          const parentNode = nodes.find(n => n.id === node.parentNode);
          if (parentNode) {
            x += parentNode.position.x;
            y += parentNode.position.y;
          }
        }
        nodesWithAlerts.push({
          nodeId: node.id,
          position: { x, y },
          alertType: node.data.alert.type,
        });
      });

    // Add stage group alerts
    nodes
      .filter(node => node.type === StageGroupNodeName && node.data?.alerts && node.data.alerts.length > 0)
      .forEach(node => {
        const mostSevereAlert = node.data.alerts.find((a: any) => a.type === "error") || node.data.alerts[0];
        nodesWithAlerts.push({
          nodeId: node.id,
          position: { x: node.position.x, y: node.position.y },
          alertType: mostSevereAlert.type,
        });
      });

    // Get all nodes with duration percentage and sort by duration (highest first)
    const nodesByDuration: BiggestDurationNode[] = nodes
      .filter(node => node.data?.node?.durationPercentage !== undefined && node.type !== StageGroupNodeName)
      .map(node => {
        // Calculate absolute position
        let x = node.position.x;
        let y = node.position.y;
        if (node.parentNode) {
          const parentNode = nodes.find(n => n.id === node.parentNode);
          if (parentNode) {
            x += parentNode.position.x;
            y += parentNode.position.y;
          }
        }
        return {
          nodeId: node.id,
          position: { x, y },
          durationPercentage: node.data.node.durationPercentage!
        };
      })
      .sort((a, b) => b.durationPercentage - a.durationPercentage);

    // Find node with biggest duration percentage (first in sorted array)
    const biggestDurationNode: BiggestDurationNode | null = nodesByDuration.length > 0 ? nodesByDuration[0] : null;

    // Find nodes with spill size and sort by spill size (highest first)
    const nodesWithSpill: NodeWithSpill[] = nodes
      .filter(node => {
        if (node.type === StageGroupNodeName) return false;
        const spillMetric = node.data?.node?.metrics?.find((m: any) => m.name === "spill size");
        if (!spillMetric?.value) return false;
        const spillSize = parseBytesString(spillMetric.value);
        return spillSize > 0;
      })
      .map(node => {
        const spillMetric = node.data.node.metrics.find((m: any) => m.name === "spill size");
        const spillSize = parseBytesString(spillMetric!.value);
        // Calculate absolute position
        let x = node.position.x;
        let y = node.position.y;
        if (node.parentNode) {
          const parentNode = nodes.find(n => n.id === node.parentNode);
          if (parentNode) {
            x += parentNode.position.x;
            y += parentNode.position.y;
          }
        }
        return {
          nodeId: node.id,
          position: { x, y },
          spillSize,
          spillSizeFormatted: spillMetric!.value
        };
      })
      .sort((a, b) => b.spillSize - a.spillSize);

    return { nodesWithAlerts, biggestDurationNode, nodesByDuration, nodesWithSpill };
  }, [nodes, sparkSQL]);

  // Effect for metric updates only
  React.useEffect(() => {
    if (!sparkSQL) return;

    try {
      setIsLoading(true);
      setError(null);

      const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
        sparkSQL,
        graphFilter,
        alerts,
        stages,
        showStages,
      );

      setNodes(layoutNodes);
      setIsLoading(false);
    } catch (err) {
      setError(`Failed to update metrics: ${err instanceof Error ? err.message : 'Unknown error'}`);
      setIsLoading(false);
    }
  }, [sparkSQL.metricUpdateId, stages, showStages]);

  // Effect for SQL structure or filter changes
  useEffect(() => {
    if (!sparkSQL) return;

    try {
      setIsLoading(true);
      setError(null);

      const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
        sparkSQL,
        graphFilter,
        alerts,
        stages,
        showStages,
      );

      setNodes(layoutNodes);
      setEdges(layoutEdges);
      setIsLoading(false);
    } catch (err) {
      setError(`Failed to layout SQL flow: ${err instanceof Error ? err.message : 'Unknown error'}`);
      setIsLoading(false);
    }
  }, [sparkSQL.uniqueId, graphFilter, stages, showStages]);

  // Handle initial focus only when instance or search params change
  useEffect(() => {
    if (instance) {
      const nodeIdsParamLocal = searchParams.get('nodeids');
      // Support both lowercase and camelCase for stageid parameter
      const stageIdParamLocal = searchParams.get('stageid') || searchParams.get('stageId');
      const currentParamKey = `${nodeIdsParamLocal || ''}-${stageIdParamLocal || ''}-default`;

      // Only apply focus if we haven't done it for these parameters yet
      if (initialFocusApplied.current !== currentParamKey) {
        const applyFocus = () => {
          if (nodes.length > 0) {
            // First try to focus on stage if stageid parameter is provided
            if (stageIdParamLocal) {
              const stageId = parseInt(stageIdParamLocal.trim(), 10);
              if (!isNaN(stageId)) {
                const stageNode = nodes.find(node => node.id === `stage-${stageId}`);
                if (stageNode) {
                  // Focus on the stage group node
                  const nodeWidth = stageNode.style?.width as number || 400;
                  const nodeHeight = stageNode.style?.height as number || 400;
                  const centerX = stageNode.position.x + nodeWidth / 2;
                  const centerY = stageNode.position.y + nodeHeight / 2;

                  instance.setCenter(centerX, centerY, { zoom: 0.6 });
                  initialFocusApplied.current = currentParamKey;
                  return;
                }
              }
            }

            const highlightedNodeIds = nodeIdsParamLocal
              ? nodeIdsParamLocal.split(',').map(id => parseInt(id.trim(), 10)).filter(id => !isNaN(id))
              : [];

            if (highlightedNodeIds.length > 0) {
              // Find the first highlighted node
              const firstHighlightedNodeId = highlightedNodeIds[0];
              const targetNode = nodes.find(node => parseInt(node.id) === firstHighlightedNodeId);

              if (targetNode) {
                // Calculate absolute position (handle parent nodes)
                let centerX = targetNode.position.x;
                let centerY = targetNode.position.y;
                if (targetNode.parentNode) {
                  const parentNode = nodes.find(n => n.id === targetNode.parentNode);
                  if (parentNode) {
                    centerX += parentNode.position.x;
                    centerY += parentNode.position.y;
                  }
                }

                const nodeWidth = 280;
                const nodeHeight = 280;
                centerX += nodeWidth / 2;
                centerY += nodeHeight / 2;

                instance.setCenter(centerX, centerY, { zoom: 0.75 });
                initialFocusApplied.current = currentParamKey;
                return;
              }
            }

            // Default behavior - fit all nodes in view
            instance.fitView();
            initialFocusApplied.current = currentParamKey;
          } else {
            // Retry if nodes aren't ready yet
            setTimeout(applyFocus, 50);
          }
        };

        // Small delay to ensure layout is complete
        setTimeout(applyFocus, 100);
      }
    }
  }, [instance, edges, nodeIdsParam, stageIdParam]);

  // Reset alert index when nodes change
  useEffect(() => {
    setCurrentAlertIndex(0);
  }, [navigationData.nodesWithAlerts.length]);

  // Reset duration index when nodes change
  useEffect(() => {
    setCurrentDurationIndex(0);
  }, [navigationData.nodesByDuration.length]);

  // Reset spill index when nodes change
  useEffect(() => {
    setCurrentSpillIndex(0);
  }, [navigationData.nodesWithSpill.length]);

  const onConnect = useCallback(
    (params: any) =>
      setEdges((eds) =>
        addEdge(
          { ...params, type: ConnectionLineType.SmoothStep, animated: true },
          eds,
        ),
      ),
    [],
  );

  // Custom zoom controls
  const handleZoomIn = useCallback(() => {
    if (instance) {
      instance.zoomIn();
    }
  }, [instance]);

  const handleZoomOut = useCallback(() => {
    if (instance) {
      instance.zoomOut();
    }
  }, [instance]);

  const handleFitView = useCallback(() => {
    if (instance) {
      instance.fitView();
    }
  }, [instance]);

  // Cycle through nodes by duration percentage (highest to lowest)
  const handleFocusNextDuration = useCallback(() => {
    if (instance && navigationData.nodesByDuration.length > 0) {
      const node = navigationData.nodesByDuration[currentDurationIndex];
      const nodeWidth = 280;
      const nodeHeight = 280;
      const centerX = node.position.x + nodeWidth / 2;
      const centerY = node.position.y + nodeHeight / 2;

      instance.setCenter(centerX, centerY, { zoom: 0.75 });

      // Increment index for next click
      const nextIndex = (currentDurationIndex + 1) % navigationData.nodesByDuration.length;
      setCurrentDurationIndex(nextIndex);
    }
  }, [instance, navigationData.nodesByDuration, currentDurationIndex]);

  // Cycle through nodes with alerts
  const handleFocusNextAlert = useCallback(() => {
    if (instance && navigationData.nodesWithAlerts.length > 0) {
      const node = navigationData.nodesWithAlerts[currentAlertIndex];
      const nodeWidth = 280;
      const nodeHeight = 280;
      const centerX = node.position.x + nodeWidth / 2;
      const centerY = node.position.y + nodeHeight / 2;

      instance.setCenter(centerX, centerY, { zoom: 0.75 });

      // Increment index for next click
      const nextIndex = (currentAlertIndex + 1) % navigationData.nodesWithAlerts.length;
      setCurrentAlertIndex(nextIndex);
    }
  }, [instance, navigationData.nodesWithAlerts, currentAlertIndex]);

  // Cycle through nodes with spill (highest to lowest)
  const handleFocusNextSpill = useCallback(() => {
    if (instance && navigationData.nodesWithSpill.length > 0) {
      const node = navigationData.nodesWithSpill[currentSpillIndex];
      const nodeWidth = 280;
      const nodeHeight = 280;
      const centerX = node.position.x + nodeWidth / 2;
      const centerY = node.position.y + nodeHeight / 2;

      instance.setCenter(centerX, centerY, { zoom: 0.75 });

      // Increment index for next click
      const nextIndex = (currentSpillIndex + 1) % navigationData.nodesWithSpill.length;
      setCurrentSpillIndex(nextIndex);
    }
  }, [instance, navigationData.nodesWithSpill, currentSpillIndex]);

  if (error) {
    return (
      <Box
        sx={{
          height: "100%",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: 2
        }}
      >
        <Alert severity="error" sx={{ maxWidth: 600 }}>
          {error}
        </Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ position: "relative", height: "100%", overflow: "hidden" }}>
      {/* Loading overlay */}
      <Fade in={isLoading}>
        <Box
          sx={{
            position: "absolute",
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: "rgba(0, 0, 0, 0.5)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 1000,
          }}
        >
          <CircularProgress size={60} />
        </Box>
      </Fade>



      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        nodeTypes={nodeTypes}
        onInit={(flowInstance) => setInstance(flowInstance)}
        connectionLineType={ConnectionLineType.SmoothStep}
        edgesUpdatable={false}
        nodesDraggable={false}
        nodesConnectable={false}
        proOptions={options}
        minZoom={0.3}
        maxZoom={1.5}
        fitView
        attributionPosition="bottom-left"
      >

        <CustomMiniMap sparkSQL={sparkSQL} />
        <Controls
          position="bottom-center"
          showZoom={false}
          showFitView={false}
          showInteractive={false}
        />

        {/* Bottom controls container - Navigation, View Mode and Legend side by side */}
        <Box
          sx={{
            position: "absolute",
            bottom: 16,
            right: 16,
            zIndex: 10,
            display: "flex",
            flexDirection: "row",
            alignItems: "flex-end",
            gap: 1.5,
          }}
        >
          {/* Navigation buttons */}
          <Box sx={{ display: "flex", flexDirection: "row", gap: 0.5 }}>
            <Tooltip title="Zoom in" arrow placement="top">
              <IconButton
                onClick={handleZoomIn}
                sx={{
                  backgroundColor: "rgba(245, 247, 250, 0.95)",
                  color: "#424242",
                  border: "1px solid rgba(0, 0, 0, 0.15)",
                  "&:hover": { backgroundColor: "rgba(245, 247, 250, 1)" },
                }}
              >
                <ZoomIn />
              </IconButton>
            </Tooltip>

            <Tooltip title="Zoom out" arrow placement="top">
              <IconButton
                onClick={handleZoomOut}
                sx={{
                  backgroundColor: "rgba(245, 247, 250, 0.95)",
                  color: "#424242",
                  border: "1px solid rgba(0, 0, 0, 0.15)",
                  "&:hover": { backgroundColor: "rgba(245, 247, 250, 1)" },
                }}
              >
                <ZoomOut />
              </IconButton>
            </Tooltip>

            <Tooltip title="Fit to view" arrow placement="top">
              <IconButton
                onClick={handleFitView}
                sx={{
                  backgroundColor: "rgba(245, 247, 250, 0.95)",
                  color: "#424242",
                  border: "1px solid rgba(0, 0, 0, 0.15)",
                  "&:hover": { backgroundColor: "rgba(245, 247, 250, 1)" },
                }}
              >
                <CenterFocusStrong />
              </IconButton>
            </Tooltip>

            <Tooltip
              title={navigationData.nodesByDuration.length > 0
                ? `Focus on biggest node duration (${currentDurationIndex + 1}/${navigationData.nodesByDuration.length}) - ${navigationData.nodesByDuration[currentDurationIndex]?.durationPercentage.toFixed(1)}%`
                : "No duration data available"
              }
              arrow
              placement="top"
            >
              <IconButton
                onClick={handleFocusNextDuration}
                disabled={navigationData.nodesByDuration.length === 0}
                sx={{
                  backgroundColor: "rgba(245, 247, 250, 0.95)",
                  color: navigationData.nodesByDuration.length > 0 ? "#424242" : "#bdbdbd",
                  border: "1px solid rgba(0, 0, 0, 0.15)",
                  "&:hover": {
                    backgroundColor: navigationData.nodesByDuration.length > 0 ? "rgba(245, 247, 250, 1)" : "rgba(245, 247, 250, 0.95)"
                  },
                  "&:disabled": {
                    backgroundColor: "rgba(245, 247, 250, 0.5)",
                  }
                }}
              >
                <Speed />
              </IconButton>
            </Tooltip>

            <Tooltip
              title={navigationData.nodesWithAlerts.length > 0
                ? `Focus on alerts (${currentAlertIndex + 1}/${navigationData.nodesWithAlerts.length})`
                : "No alerts found"
              }
              arrow
              placement="top"
            >
              <IconButton
                onClick={handleFocusNextAlert}
                disabled={navigationData.nodesWithAlerts.length === 0}
                sx={{
                  backgroundColor: "rgba(245, 247, 250, 0.95)",
                  color: navigationData.nodesWithAlerts.length > 0 ? "#424242" : "#bdbdbd",
                  border: "1px solid rgba(0, 0, 0, 0.15)",
                  "&:hover": {
                    backgroundColor: navigationData.nodesWithAlerts.length > 0 ? "rgba(245, 247, 250, 1)" : "rgba(245, 247, 250, 0.95)"
                  },
                  "&:disabled": {
                    backgroundColor: "rgba(245, 247, 250, 0.5)",
                  }
                }}
              >
                <Warning />
              </IconButton>
            </Tooltip>

            <Tooltip
              title={navigationData.nodesWithSpill.length > 0
                ? `Focus on spill (${currentSpillIndex + 1}/${navigationData.nodesWithSpill.length}) - ${navigationData.nodesWithSpill[currentSpillIndex]?.spillSizeFormatted}`
                : "No spill detected"
              }
              arrow
              placement="top"
            >
              <IconButton
                onClick={handleFocusNextSpill}
                disabled={navigationData.nodesWithSpill.length === 0}
                sx={{
                  backgroundColor: "rgba(245, 247, 250, 0.95)",
                  color: navigationData.nodesWithSpill.length > 0 ? "#424242" : "#bdbdbd",
                  border: "1px solid rgba(0, 0, 0, 0.15)",
                  "&:hover": {
                    backgroundColor: navigationData.nodesWithSpill.length > 0 ? "rgba(245, 247, 250, 1)" : "rgba(245, 247, 250, 0.95)"
                  },
                  "&:disabled": {
                    backgroundColor: "rgba(245, 247, 250, 0.5)",
                  }
                }}
              >
                <Storage />
              </IconButton>
            </Tooltip>

            <Tooltip
              title={showStages ? "Hide stages grouping" : "Show stages grouping"}
              arrow
              placement="top"
            >
              <IconButton
                onClick={() => dispatch(setShowStages({ showStages: !showStages }))}
                sx={{
                  backgroundColor: showStages ? "rgba(25, 118, 210, 0.2)" : "rgba(245, 247, 250, 0.95)",
                  color: showStages ? "#1976d2" : "#424242",
                  border: showStages ? "1px solid rgba(25, 118, 210, 0.5)" : "1px solid rgba(0, 0, 0, 0.15)",
                  "&:hover": {
                    backgroundColor: showStages ? "rgba(25, 118, 210, 0.3)" : "rgba(245, 247, 250, 1)"
                  },
                }}
              >
                {showStages ? <Layers /> : <LayersClear />}
              </IconButton>
            </Tooltip>
          </Box>

          {/* View Mode selector */}
          <Paper
            elevation={3}
            sx={{
              backgroundColor: "rgba(30, 41, 59, 0.95)",
              backdropFilter: "blur(8px)",
              borderRadius: 2,
              overflow: "hidden",
              minWidth: 140,
              maxWidth: 220,
            }}
          >
            {/* Header */}
            <Box
              onClick={() => setViewModeExpanded(!viewModeExpanded)}
              sx={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                px: 1.5,
                py: 1,
                cursor: "pointer",
                borderBottom: viewModeExpanded ? "1px solid rgba(255, 255, 255, 0.1)" : "none",
                "&:hover": {
                  backgroundColor: "rgba(255, 255, 255, 0.05)",
                },
              }}
            >
              <Typography sx={{ fontSize: 12, fontWeight: 600, color: "#fff" }}>
                View: {graphFilter === "io" ? "I/O Only" : graphFilter === "basic" ? "Basic" : "Advanced"}
              </Typography>
              <IconButton size="small" sx={{ color: "rgba(255, 255, 255, 0.7)", p: 0 }}>
                {viewModeExpanded ? <ExpandMore fontSize="small" /> : <ExpandLess fontSize="small" />}
              </IconButton>
            </Box>

            {/* Collapsible options */}
            <Collapse in={viewModeExpanded}>
              <Box sx={{ py: 0.5 }}>
                {[
                  { value: "io", label: "Only I/O", desc: "Show only input/output nodes" },
                  { value: "basic", label: "Basic", desc: "Show main transformations" },
                  { value: "advanced", label: "Advanced", desc: "Show all nodes" },
                ].map((option) => (
                  <Box
                    key={option.value}
                    onClick={() => {
                      dispatch(setSQLMode({ newMode: option.value as GraphFilter }));
                      setViewModeExpanded(false);
                    }}
                    sx={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                      px: 1.5,
                      py: 0.75,
                      cursor: "pointer",
                      backgroundColor: graphFilter === option.value ? "rgba(25, 118, 210, 0.3)" : "transparent",
                      "&:hover": {
                        backgroundColor: graphFilter === option.value ? "rgba(25, 118, 210, 0.4)" : "rgba(255, 255, 255, 0.1)",
                      },
                    }}
                  >
                    <Typography sx={{ fontSize: 11, color: "rgba(255, 255, 255, 0.9)" }}>
                      {option.label}
                    </Typography>
                    {graphFilter === option.value && (
                      <Box
                        sx={{
                          width: 6,
                          height: 6,
                          borderRadius: "50%",
                          backgroundColor: "#90caf9",
                        }}
                      />
                    )}
                  </Box>
                ))}
              </Box>
            </Collapse>
          </Paper>

          {/* Legend */}
          <FlowLegend inline />
        </Box>
      </ReactFlow>

      {/* Stage details drawer */}
      <Drawer
        anchor="right"
        open={selectedStage !== undefined}
        onClose={() => dispatch(setSelectedStage({ selectedStage: undefined }))}
        PaperProps={{
          sx: {
            backgroundColor: "rgba(0, 0, 0, 0.95)",
            backdropFilter: "blur(10px)",
            borderLeft: "1px solid rgba(255, 255, 255, 0.1)",
            color: "#ffffff",
          },
        }}
      >
        <Box sx={{ minWidth: 450, maxWidth: 600, padding: 2 }}>
          <StageIconDrawer stage={selectedStage} />
        </Box>
      </Drawer>
    </Box>
  );
};

export default SqlFlow;
