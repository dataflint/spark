import dagre from "dagre";
import { Edge, Node, Position } from "reactflow";
import { isNodeAGroup } from "../flowComponents/StageGroupNode/StageGroupNode";

const buildDagreGraph = (rankdir: "LR" | "TB" = "LR") => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir });

  return dagreGraph;
};

const nodeSize = 280;
export const nodeWidth = nodeSize;
export const nodeHeight = nodeSize;
const groupPadding = 40;
const groupWidth = nodeWidth + groupPadding * 2;

export const getFlatElementsLayout = (
  nodes: Node[],
  edges: Edge[],
): { layoutNodes: Node[]; layoutEdges: Edge[] } => {
  const dagreGraph = buildDagreGraph();

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
  });

  return { layoutNodes: nodes, layoutEdges: edges };
};

interface GroupedElementsLayoutParams {
  topLevelNodes: Node[];
  topLevelEdges: Edge[];
}
export const getGroupedElementsLayout = ({
  topLevelNodes,
  topLevelEdges,
}: GroupedElementsLayoutParams): {
  layoutNodes: Node[];
  layoutEdges: Edge[];
} => {
  const topLevelGraph = buildDagreGraph();
  const innerLevelGraph = buildDagreGraph("TB");

  topLevelNodes.forEach((node) => {
    topLevelGraph.setNode(node.id, {
      width: isNodeAGroup(node) ? groupWidth : nodeWidth,
      height: nodeHeight,
    });
  });

  topLevelEdges.forEach((edge) => {
    topLevelGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(topLevelGraph);
  dagre.layout(innerLevelGraph);

  topLevelNodes.forEach((topLevelNode) => {
    const nodeWithPosition = topLevelGraph.node(topLevelNode.id);
    topLevelNode.targetPosition = Position.Left;
    topLevelNode.sourcePosition = Position.Right;

    topLevelNode.position = {
      x: nodeWithPosition.x - nodeWidth / 2,
      y: nodeWithPosition.y - nodeHeight / 2,
    };
  });

  return {
    layoutNodes: topLevelNodes,
    layoutEdges: topLevelEdges,
  };
};
