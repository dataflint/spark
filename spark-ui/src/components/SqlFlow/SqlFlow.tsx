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
import { SparkSQLs, SqlEdge, SqlNode } from '../../interfaces/SparkSQLs';


const options = { hideAttribution: true };

const SqlFlow: FC<{ sparkSQLs: SparkSQLs | undefined }> = (
    { sparkSQLs = [] }): JSX.Element => {
    const [instance, setInstace] = useState<ReactFlowInstance | undefined>();
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);

    React.useEffect(() => {
        if (!sparkSQLs || sparkSQLs.length === 0)
            return;
        const { nodes, edges } = SqlLayoutService.SqlElementsToLayout(
            sparkSQLs[sparkSQLs.length - 1].nodes, sparkSQLs[sparkSQLs.length - 1].edges);
        setNodes(nodes);
        setEdges(edges);
        instance?.fitView();
    }, [sparkSQLs]);

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
