import dagre from "dagre";
import { Node, Edge, Position } from "reactflow"
import {v4 as uuidv4} from 'uuid';
import { SqlEdge, SqlNode } from "../../interfaces/SparkSQLs";


const dagreGraph = new dagre.graphlib.Graph();
dagreGraph.setDefaultEdgeLabel(() => ({}));

const nodeWidth = 172;
const nodeHeight = 36;

const getLayoutedElements = (nodes: Node[], edges: Edge[]): {nodes: Node[], edges: Edge[]} => {
    dagreGraph.setGraph({ rankdir: 'LR' });

    nodes.forEach((node) => {
        dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
    });

    edges.forEach((edge) => {
        dagreGraph.setEdge(edge.source, edge.target);
    });

    dagre.layout(dagreGraph);

    nodes.forEach((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);
        node.targetPosition = Position.Left;
        node.sourcePosition = Position.Right;

        // We are shifting the dagre node position (anchor=center center) to the top left
        // so it matches the React Flow node anchor point (top left).
        node.position = {
            x: nodeWithPosition.x - nodeWidth / 2,
            y: nodeWithPosition.y - nodeHeight / 2,
        };

        return node;
    });

    return { nodes, edges };
};

class SqlLayoutService {
    static SqlElementsToLayout(sqlNodes: SqlNode[], sqlEdges: SqlEdge[]): {nodes: Node[], edges: Edge[]} {
        const flowNodes: Node[] = sqlNodes.map((node: SqlNode) => {
            return {
                id: node.nodeId.toString(),
                data: { label: node.nodeName },
                position: {x:0, y:0}
            }
        });
        const flowEdges: Edge[] = sqlEdges.map((edge: SqlEdge) => {
            return {
                id: uuidv4(),
                source: edge.fromId.toString(),
                target: edge.toId.toString()
            }
        });
        
        const {nodes, edges} = getLayoutedElements(flowNodes, flowEdges);
        return {nodes,edges};
    }
}

export default SqlLayoutService;
