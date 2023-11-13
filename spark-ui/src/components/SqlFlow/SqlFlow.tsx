import React, { FC, useCallback, useEffect, useState } from "react";
import ReactFlow, {
  ConnectionLineType,
  Controls,
  ReactFlowInstance,
  addEdge,
  useEdgesState,
  useNodesState,
} from "reactflow";

import { Box, FormControlLabel, FormGroup, Switch } from "@mui/material";
import "reactflow/dist/style.css";
import { EnrichedSparkSQL } from "../../interfaces/AppStore";
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
  const [isAdvancedMode, setIsAdvancedMode] = React.useState<boolean>(
    false,
  );

  React.useEffect(() => {
    if (!sparkSQL) return;
    const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
      sparkSQL,
      isAdvancedMode
    );

    setNodes(layoutNodes);
  }, [sparkSQL.metricUpdateId]);

  useEffect(() => {
    if (!sparkSQL) return;
    const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
      sparkSQL,
      isAdvancedMode
    );

    setNodes(layoutNodes);
    setEdges(layoutEdges);
  }, [sparkSQL.uniqueId, isAdvancedMode]);

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
          <FormGroup>
            <FormControlLabel
              sx={{ zIndex: 6, position: "absolute", bottom: "10px", right: "10px" }}
              labelPlacement="bottom"
              label={isAdvancedMode ? "Advanced" : "Basic"}
              control={<Switch checked={isAdvancedMode} onChange={(event) => setIsAdvancedMode(event.target.checked)} />} />
          </FormGroup>
        </Box>
      </ReactFlow>
    </div>
  );
};

export default SqlFlow;
