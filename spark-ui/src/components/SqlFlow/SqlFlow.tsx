import React, { FC, useCallback, useEffect, useRef, useState } from "react";
import ReactFlow, {
  ConnectionLineType,
  Controls,
  ReactFlowInstance,
  addEdge,
  useEdgesState,
  useNodesState,
} from "reactflow";

import { Box, Drawer, FormControl, InputLabel, MenuItem, Select } from "@mui/material";
import { useSearchParams } from "react-router-dom";
import "reactflow/dist/style.css";
import { useAppDispatch, useAppSelector } from "../../Hooks";
import { EnrichedSparkSQL, GraphFilter } from "../../interfaces/AppStore";
import { setSQLMode, setSelectedStage } from "../../reducers/GeneralSlice";
import SqlLayoutService from "./SqlLayoutService";
import StageIconDrawer from "./StageIconDrawer";
import { StageNode, StageNodeName } from "./StageNode";

const options = { hideAttribution: true };
const nodeTypes = { [StageNodeName]: StageNode };

const SqlFlow: FC<{ sparkSQL: EnrichedSparkSQL }> = ({
  sparkSQL,
}): JSX.Element => {
  const [instance, setInstace] = useState<ReactFlowInstance | undefined>();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [searchParams] = useSearchParams();
  const nodeIdsParam = searchParams.get('nodeids');
  const initialFocusApplied = useRef<string | null>(null);

  const dispatch = useAppDispatch();
  const graphFilter = useAppSelector((state) => state.general.sqlMode);
  const selectedStage = useAppSelector((state) => state.general.selectedStage);

  React.useEffect(() => {
    if (!sparkSQL) return;
    const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
      sparkSQL,
      graphFilter,
    );

    setNodes(layoutNodes);
  }, [sparkSQL.metricUpdateId]);

  useEffect(() => {
    if (!sparkSQL) return;
    const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
      sparkSQL,
      graphFilter,
    );

    setNodes(layoutNodes);
    setEdges(layoutEdges);
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

  useEffect(() => { }, [nodes]);

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

  return (
    <div style={{ overflow: "hidden", height: "100%" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        nodeTypes={nodeTypes}
        onInit={(flowInstance) => setInstace(flowInstance)}
        connectionLineType={ConnectionLineType.SmoothStep}
        edgesUpdatable={false}
        nodesDraggable={false}
        nodesConnectable={false}
        proOptions={options}
        minZoom={0.6}
        maxZoom={0.9}
        fitView
      >
        <Controls />
        <Box display={"flex"}>
          <FormControl
            sx={{
              zIndex: 6,
              position: "absolute",
              bottom: "10px",
              right: "10px",
              width: "120px",
            }}
          >
            <InputLabel>Mode</InputLabel>
            <Select
              value={graphFilter}
              label="Mode"
              onChange={(event) =>
                dispatch(
                  setSQLMode({ newMode: event.target.value as GraphFilter }),
                )
              }
            >
              <MenuItem value={"io"}>Only IO</MenuItem>
              <MenuItem value={"basic"}>Basic</MenuItem>
              <MenuItem value={"advanced"}>Advanced</MenuItem>
            </Select>
          </FormControl>
          <Drawer
            anchor={"right"}
            open={selectedStage !== undefined}
            onClose={() => dispatch(setSelectedStage({ selectedStage: undefined }))}
          >
            <Box sx={{ minWidth: "400px" }}
            >
              <StageIconDrawer stage={selectedStage} />
            </Box>
          </Drawer>
        </Box>
      </ReactFlow>
    </div>
  );
};

export default SqlFlow;
