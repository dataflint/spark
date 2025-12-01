import React, { FC, useCallback, useEffect, useMemo, useRef, useState } from "react";
import ReactFlow, {
  ConnectionLineType,
  Controls,
  ReactFlowInstance,
  addEdge,
  useEdgesState,
  useNodesState,
} from "reactflow";

import { CenterFocusStrong, Info as InfoIcon, Speed, Warning, ZoomIn, ZoomOut } from "@mui/icons-material";
import {
  Alert,
  Box,
  CircularProgress,
  Drawer,
  Fade,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  Stack,
  Tooltip
} from "@mui/material";
import { useSearchParams } from "react-router-dom";
import "reactflow/dist/style.css";
import { useAppDispatch, useAppSelector } from "../../Hooks";
import { EnrichedSparkSQL, GraphFilter } from "../../interfaces/AppStore";
import { setSQLMode, setSelectedStage } from "../../reducers/GeneralSlice";
import CustomMiniMap from "./MiniMap";
import SqlLayoutService from "./SqlLayoutService";
import StageIconDrawer from "./StageIconDrawer";
import { StageNode, StageNodeName } from "./StageNode";

const options = { hideAttribution: true };
const nodeTypes = { [StageNodeName]: StageNode };

// Types for navigation data
interface NodeWithAlert {
  nodeId: string;
  position: { x: number; y: number };
  alert: any;
}

interface BiggestDurationNode {
  nodeId: string;
  position: { x: number; y: number };
  durationPercentage: number;
}

interface NavigationData {
  nodesWithAlerts: NodeWithAlert[];
  biggestDurationNode: BiggestDurationNode | null;
  nodesByDuration: BiggestDurationNode[];
}

const SqlFlow: FC<{ sparkSQL: EnrichedSparkSQL }> = ({
  sparkSQL,
}): JSX.Element => {
  // Get alerts for passing to nodes
  const alerts = useAppSelector((state) => state.spark.alerts);

  const [instance, setInstance] = useState<ReactFlowInstance | undefined>();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [searchParams] = useSearchParams();
  const nodeIdsParam = searchParams.get('nodeids');
  const initialFocusApplied = useRef<string | null>(null);
  const [currentAlertIndex, setCurrentAlertIndex] = useState(0);
  const [currentDurationIndex, setCurrentDurationIndex] = useState(0);

  const dispatch = useAppDispatch();
  const graphFilter = useAppSelector((state) => state.general.sqlMode);
  const selectedStage = useAppSelector((state) => state.general.selectedStage);

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
    if (!sparkSQL || !nodes.length) return { nodesWithAlerts: [], biggestDurationNode: null, nodesByDuration: [] };

    // Find nodes with alerts
    const nodesWithAlerts: NodeWithAlert[] = nodes.filter(node => node.data?.alert).map(node => ({
      nodeId: node.id,
      position: node.position,
      alert: node.data.alert
    }));

    // Get all nodes with duration percentage and sort by duration (highest first)
    const nodesByDuration: BiggestDurationNode[] = nodes
      .filter(node => node.data?.node?.durationPercentage !== undefined)
      .map(node => ({
        nodeId: node.id,
        position: node.position,
        durationPercentage: node.data.node.durationPercentage!
      }))
      .sort((a, b) => b.durationPercentage - a.durationPercentage);

    // Find node with biggest duration percentage (first in sorted array)
    const biggestDurationNode: BiggestDurationNode | null = nodesByDuration.length > 0 ? nodesByDuration[0] : null;

    return { nodesWithAlerts, biggestDurationNode, nodesByDuration };
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
      );

      setNodes(layoutNodes);
      setIsLoading(false);
    } catch (err) {
      setError(`Failed to update metrics: ${err instanceof Error ? err.message : 'Unknown error'}`);
      setIsLoading(false);
    }
  }, [sparkSQL.metricUpdateId]);

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
      );

      setNodes(layoutNodes);
      setEdges(layoutEdges);
      setIsLoading(false);
    } catch (err) {
      setError(`Failed to layout SQL flow: ${err instanceof Error ? err.message : 'Unknown error'}`);
      setIsLoading(false);
    }
  }, [sparkSQL.uniqueId, graphFilter]);

  // Handle initial focus only when instance or search params change
  useEffect(() => {
    if (instance) {
      const nodeIdsParam = searchParams.get('nodeids');
      const currentParamKey = nodeIdsParam || 'default';

      // Only apply focus if we haven't done it for these parameters yet
      if (initialFocusApplied.current !== currentParamKey) {
        const applyFocus = () => {
          if (nodes.length > 0) {
            const highlightedNodeIds = nodeIdsParam
              ? nodeIdsParam.split(',').map(id => parseInt(id.trim(), 10)).filter(id => !isNaN(id))
              : [];

            if (highlightedNodeIds.length > 0) {
              // Find the first highlighted node
              const firstHighlightedNodeId = highlightedNodeIds[0];
              const targetNode = nodes.find(node => parseInt(node.id) === firstHighlightedNodeId);

              if (targetNode) {
                // Focus on the first highlighted node
                const nodeWidth = 280;
                const nodeHeight = 280;
                const centerX = targetNode.position.x + nodeWidth / 2;
                const centerY = targetNode.position.y + nodeHeight / 2;

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
  }, [instance, edges, nodeIdsParam]);

  // Reset alert index when nodes change
  useEffect(() => {
    setCurrentAlertIndex(0);
  }, [navigationData.nodesWithAlerts.length]);

  // Reset duration index when nodes change
  useEffect(() => {
    setCurrentDurationIndex(0);
  }, [navigationData.nodesByDuration.length]);

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



      {/* Custom controls */}
      <Box
        sx={{
          position: "absolute",
          top: 0,
          right: 16,
          zIndex: 5,
          display: "flex",
          flexDirection: "column",
          gap: 1,
        }}
      >
        <Tooltip title="Zoom in" arrow placement="left">
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

        <Tooltip title="Zoom out" arrow placement="left">
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

        <Tooltip title="Fit to view" arrow placement="left">
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
          placement="left"
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
          placement="left"
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
      </Box>

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

        {/* Mode selector */}
        <Box
          sx={{
            position: "absolute",
            bottom: 16,
            right: 16,
            zIndex: 5,
          }}
        >
          <FormControl
            size="small"
            sx={{
              backgroundColor: "rgba(0, 0, 0, 0.85)",
              borderRadius: 1,
              border: "1px solid rgba(255, 255, 255, 0.2)",
              "& .MuiOutlinedInput-root": {
                "& fieldset": {
                  borderColor: "rgba(255, 255, 255, 0.3)",
                },
                "&:hover fieldset": {
                  borderColor: "rgba(255, 255, 255, 0.5)",
                },
                "&.Mui-focused fieldset": {
                  borderColor: "#90caf9",
                },
              },
              "& .MuiInputLabel-root": {
                color: "rgba(255, 255, 255, 0.8)",
              },
              "& .MuiSelect-select": {
                color: "#ffffff",
              },
              "& .MuiSelect-icon": {
                color: "rgba(255, 255, 255, 0.8)",
              },
            }}
          >
            <InputLabel>View Mode</InputLabel>
            <Select
              value={graphFilter}
              label="View Mode"
              onChange={(event) =>
                dispatch(
                  setSQLMode({ newMode: event.target.value as GraphFilter }),
                )
              }
              MenuProps={{
                PaperProps: {
                  sx: {
                    backgroundColor: "rgba(0, 0, 0, 0.9)",
                    border: "1px solid rgba(255, 255, 255, 0.2)",
                    "& .MuiMenuItem-root": {
                      color: "#ffffff",
                      "&:hover": {
                        backgroundColor: "rgba(255, 255, 255, 0.1)",
                      },
                    },
                  },
                },
              }}
            >
              <MenuItem value={"io"}>
                <Stack direction="row" spacing={1} alignItems="center">
                  <span>Only I/O</span>
                  <InfoIcon fontSize="small" color="disabled" />
                </Stack>
              </MenuItem>
              <MenuItem value={"basic"}>
                <Stack direction="row" spacing={1} alignItems="center">
                  <span>Basic</span>
                  <InfoIcon fontSize="small" color="disabled" />
                </Stack>
              </MenuItem>
              <MenuItem value={"advanced"}>
                <Stack direction="row" spacing={1} alignItems="center">
                  <span>Advanced</span>
                  <InfoIcon fontSize="small" color="disabled" />
                </Stack>
              </MenuItem>
            </Select>
          </FormControl>
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
