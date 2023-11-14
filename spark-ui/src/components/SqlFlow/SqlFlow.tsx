import React, { FC, useCallback, useEffect, useState } from "react";
import ReactFlow, {
  ConnectionLineType,
  Controls,
  ReactFlowInstance,
  addEdge,
  useEdgesState,
  useNodesState,
} from "reactflow";

import { Box, FormControl, InputLabel, MenuItem, Select } from "@mui/material";
import "reactflow/dist/style.css";
import { EnrichedSparkSQL, GraphFilter } from "../../interfaces/AppStore";
import SqlLayoutService from "./SqlLayoutService";
import { StageNode, StageNodeName } from "./StageNode";

const options = { hideAttribution: true };
const nodeTypes = { [StageNodeName]: StageNode };

const SqlFlow: FC<{ sparkSQL: EnrichedSparkSQL }> = ({
  sparkSQL
}): JSX.Element => {
  const [instance, setInstace] = useState<ReactFlowInstance | undefined>();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [graphFilter, setGraphFilter] = React.useState<GraphFilter>(
    "basic",
  );

  React.useEffect(() => {
    if (!sparkSQL) return;
    const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
      sparkSQL,
      graphFilter
    );

    setNodes(layoutNodes);
  }, [sparkSQL.metricUpdateId]);

  useEffect(() => {
    if (!sparkSQL) return;
    const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
      sparkSQL,
      graphFilter
    );

    setNodes(layoutNodes);
    setEdges(layoutEdges);
  }, [sparkSQL.uniqueId, graphFilter]);

  useEffect(() => {
    if (instance) {
      setTimeout(instance.fitView, 20);
    }
  }, [instance, edges]);

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
          <FormControl sx={{ zIndex: 6, position: "absolute", bottom: "10px", right: "10px", width: "120px" }}>
            <InputLabel>Mode</InputLabel>
            <Select
              value={graphFilter}
              label="Mode"
              onChange={(event) => setGraphFilter(event.target.value as GraphFilter)}
            >
              <MenuItem value={"io"}>Only IO</MenuItem>
              <MenuItem value={"basic"}>Basic</MenuItem>
              <MenuItem value={"advanced"}>Advanced</MenuItem>
            </Select>
          </FormControl>
        </Box>
      </ReactFlow>
    </div>
  );
};

export default SqlFlow;
