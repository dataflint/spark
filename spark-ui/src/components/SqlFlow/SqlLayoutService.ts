import dagre from "dagre";
import { Edge, Node, Position } from "reactflow";
import { v4 as uuidv4 } from "uuid";
import { EnrichedSparkSQL, EnrichedSqlEdge, EnrichedSqlNode } from "../../interfaces/AppStore";
import { StageNodeName } from "./StageNode";

const nodeWidth = 280;
const nodeHeight = 200;

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
): { layoutNodes: Node[]; layoutEdges: Edge[] } => {
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

  return { layoutNodes: nodes, layoutEdges: edges };
};

class SqlLayoutService {
  static SqlElementsToLayout(
    sql: EnrichedSparkSQL,
    isAdvancedMode: boolean
  ): { layoutNodes: Node[]; layoutEdges: Edge[] } {
    const nodeIds = isAdvancedMode ? sql.advancedNodesIds : sql.basicNodesIds;
    const edges = isAdvancedMode ? sql.advancedEdges : sql.basicEdges;

    const flowNodes: Node[] = sql.nodes
      .filter((node) => nodeIds.includes(node.nodeId))
      .map((node: EnrichedSqlNode) => {
        return {
          id: node.nodeId.toString(),
          data: { sqlId: sql.id, node: node },
          type: StageNodeName,
          position: { x: 0, y: 0 },
        };
      });
    const flowEdges: Edge[] = edges.map((edge: EnrichedSqlEdge) => {
      return {
        id: uuidv4(),
        source: edge.fromId.toString(),
        animated: true,
        target: edge.toId.toString(),
      };
    });

    const { layoutNodes, layoutEdges } = getLayoutedElements(flowNodes, flowEdges);
    return { layoutNodes: layoutNodes, layoutEdges: layoutEdges };
  }
}

export default SqlLayoutService;
