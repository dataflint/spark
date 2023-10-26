import dagre from "dagre";
import { Edge, Node, Position } from "reactflow";
import { v4 as uuidv4 } from "uuid";
import { EnrichedSqlEdge, EnrichedSqlNode } from "../../interfaces/AppStore";
import { StageNodeName } from "./StageNode";

const nodeWidth = 280;
const nodeHeight = 180;

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
): { nodes: Node[]; edges: Edge[] } => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: "LR" });

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
  static SqlElementsToLayout(
    sqlNodes: EnrichedSqlNode[],
    sqlEdges: EnrichedSqlEdge[],
  ): { layoutNodes: Node[]; layoutEdges: Edge[] } {
    const flowNodes: Node[] = sqlNodes
      .filter((node) => node.isVisible)
      .map((node: EnrichedSqlNode) => {
        return {
          id: node.nodeId.toString(),
          data: { node: node },
          type: StageNodeName,
          position: { x: 0, y: 0 },
        };
      });
    const flowEdges: Edge[] = sqlEdges.map((edge: EnrichedSqlEdge) => {
      return {
        id: uuidv4(),
        source: edge.fromId.toString(),
        animated: true,
        target: edge.toId.toString(),
      };
    });

    const { nodes, edges } = getLayoutedElements(flowNodes, flowEdges);
    return { layoutNodes: nodes, layoutEdges: edges };
  }
}

export default SqlLayoutService;
