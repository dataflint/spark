import React, { FC, useCallback, useEffect, useState } from 'react';
import ReactFlow, {
    addEdge,
    ConnectionLineType,
    Panel,
    useNodesState,
    useEdgesState,
    ReactFlowInstance,
    Controls,
    MiniMap,
} from 'reactflow';
import dagre, { Edge } from 'dagre';
import styles from './node-style.module.css'


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
        if (!sparkSQL)
            return;
        const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
            sparkSQL.nodes, sparkSQL.edges);

        setNodes(layoutNodes);
    }, [sparkSQL.metricUpdateId]);

    useEffect(() => {
        if (!sparkSQL)
            return;
        const { layoutNodes, layoutEdges } = SqlLayoutService.SqlElementsToLayout(
            sparkSQL.nodes, sparkSQL.edges);

        setNodes(layoutNodes);
        setEdges(layoutEdges);
    }, [sparkSQL.uniqueId]);

    useEffect(() => {
        if (instance) {
            setTimeout(instance.fitView, 20);
        }
    }, [instance, edges]);

    useEffect(() => { }, [nodes]);

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
            minZoom={0.6}
            maxZoom={0.9}
            fitView
        >
            <Controls />
        </ReactFlow>
    );
};

export default SqlFlow;
