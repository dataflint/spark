import React, { FC, useCallback, useEffect } from 'react';
import ReactFlow, {
    addEdge,
    ConnectionLineType,
    Panel,
    useNodesState,
    useEdgesState,
} from 'reactflow';
import dagre, { Edge } from 'dagre';

import 'reactflow/dist/style.css';
import SqlLayoutService from './SqlLayoutService';
import { SqlEdge, SqlNode } from '../../interfaces/SparkSQLs';

const SqlFlow: FC<{ initNodes: SqlNode[] | undefined, initEdges: SqlEdge[] | undefined }> = (
    { initNodes = [], initEdges = [] }): JSX.Element => {
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);

    React.useEffect(() => {
        const { nodes, edges } = SqlLayoutService.SqlElementsToLayout(initNodes, initEdges);
        setNodes(nodes);
        setEdges(edges);
    }, [initNodes, initEdges]);

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
            connectionLineType={ConnectionLineType.SmoothStep}
            fitView
        />
    );
};

export default SqlFlow;
