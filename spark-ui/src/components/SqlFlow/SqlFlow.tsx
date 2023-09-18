import React, { FC, useCallback, useEffect, useState } from 'react';
import ReactFlow, {
    addEdge,
    ConnectionLineType,
    Panel,
    useNodesState,
    useEdgesState,
    ReactFlowInstance,
} from 'reactflow';
import dagre, { Edge } from 'dagre';

import 'reactflow/dist/style.css';
import SqlLayoutService from './SqlLayoutService';
import { EnrichedSparkSQL } from '../../interfaces/AppStore';
import { StageNode, StageNodeName } from './StageNode';


const options = { hideAttribution: true };
const nodeTypes = { [StageNodeName]: StageNode };


const SqlFlow: FC<{ sparkSQL: EnrichedSparkSQL }> = (
    { sparkSQL }): JSX.Element => {
    const [instance, setInstace] = useState<ReactFlowInstance | undefined>();
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);

    React.useEffect(() => {
        const { nodes, edges } = SqlLayoutService.SqlElementsToLayout(
            sparkSQL.nodes.filter((node) => node.isVisible), sparkSQL.edges);
        setNodes(nodes);
        setEdges(edges);
        instance?.fitView();
    }, [sparkSQL]);

    const onConnect = useCallback(
        (params: any) =>
            setEdges((eds) =>
                addEdge({ ...params, type: ConnectionLineType.SmoothStep, animated: true }, eds)
            ),
        []
    );

    return (
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
            fitView
        >
        </ReactFlow>
    );
};

export default SqlFlow;
