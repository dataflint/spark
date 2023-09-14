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
import { SqlEdge, SqlNode } from '../../interfaces/SqlInterfaces';
import SqlNodeService from './SqlNodeService';

// const initialEdges = [
//     { id: '1-2', source: '1', target: '2' },
//     { id: '1-3', source: '1', target: '3' },
//     // { id: '1-3', source: '1', target: '3' }
// ];

// const initialNodes = [
//     {
//         id: '1',
//         data: { label: 'Hello' },
//         // position: { x: 0, y: 0 },
//         type: 'input',
//     },
//     {
//         id: '2',
//         data: { label: 'World' },
//         // position: { x: 100, y: 100 },
//     },
//     {
//         id: '3',
//         data: { label: 'Hehehe' },
//         // position: { x: 100, y: 100 },
//     },
// ];


const SqlFlow: FC<{initNodes:SqlNode[] | undefined, initEdges: SqlEdge[] | undefined}> = ({initNodes=[], initEdges=[]}): JSX.Element => {
    const [nodes, setNodes, onNodesChange] = useNodesState([]);
    const [edges, setEdges, onEdgesChange] = useEdgesState([]);

    React.useEffect(() => {
        const {nodes, edges} = SqlNodeService.SqlElementsToLayout(initNodes,initEdges);
        setNodes(nodes);
        setEdges(edges);
    }, [initNodes,initEdges]);

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
