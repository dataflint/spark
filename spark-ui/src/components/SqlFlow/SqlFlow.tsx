import React, { FC, useCallback, useEffect, useState } from "react";
import ReactFlow, {
  addEdge,
  ConnectionLineType,
  Controls,
  ReactFlowInstance,
  useEdgesState,
  useNodesState,
} from "reactflow";

import {
  Box,
  Drawer,
  FormControl,
  InputLabel,
  MenuItem,
  Select,
} from "@mui/material";
import "reactflow/dist/style.css";
import { useAppDispatch, useAppSelector } from "../../Hooks";
import { EnrichedSparkSQL, GraphFilter } from "../../interfaces/AppStore";
import { setSelectedStage, setSQLMode } from "../../reducers/GeneralSlice";
import {
  sqlElementsToFlatLayout,
  sqlElementsToGroupedLayout,
} from "./SqlLayoutService/SqlLayoutService";
import StageIconDrawer from "./StageIconDrawer";
import { StageNode, StageNodeName } from "./StageNode";

const options = { hideAttribution: true };
const nodeTypes = {
  [StageNodeName]: StageNode,
};

// this is a mock to allow for use of external configuration
const shouldUseGroupedLayout = true;
// const shouldUseGroupedLayout = false;

const SqlFlow: FC<{ sparkSQL: EnrichedSparkSQL }> = ({
  sparkSQL,
}): JSX.Element => {
  const [instance, setInstace] = useState<ReactFlowInstance | undefined>();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const dispatch = useAppDispatch();
  const graphFilter = useAppSelector((state) => state.general.sqlMode);
  const selectedStage = useAppSelector((state) => state.general.selectedStage);

  useEffect(() => {
    if (!sparkSQL) return;

    const { layoutNodes, layoutEdges } = shouldUseGroupedLayout
      ? sqlElementsToGroupedLayout(sparkSQL, graphFilter)
      : sqlElementsToFlatLayout(sparkSQL, graphFilter);

    setNodes(layoutNodes);
    setEdges(layoutEdges);
  }, [sparkSQL.metricUpdateId, sparkSQL.uniqueId, graphFilter]);

  useEffect(() => {
    if (instance) {
      setTimeout(instance.fitView, 20);
    }
  }, [instance, edges]);

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
            onClose={() =>
              dispatch(setSelectedStage({ selectedStage: undefined }))
            }
          >
            <Box sx={{ minWidth: "400px" }}>
              <StageIconDrawer stage={selectedStage} />
            </Box>
          </Drawer>
        </Box>
      </ReactFlow>
    </div>
  );
};

export default SqlFlow;
