import dagre from "dagre";
import { Edge, Node, Position } from "reactflow";

const buildDagreGraph = (rankdir: "LR" | "TB" = "LR") => {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir });

  return dagreGraph;
};

const isNodeAGroup = (node: Node) => node.type === "group";

const nodeSize = 280;
const nodeWidth = nodeSize;
const nodeHeight = nodeSize;
const groupPadding = 40;
const nodeMargin = 30;
const groupWidth = nodeWidth + groupPadding * 2;
const groupHeight = nodeHeight + groupPadding * 2;

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
  innerLevelEdges: Edge[];
}
export const getGroupedElementsLayout = ({
  topLevelNodes,
  topLevelEdges,
  innerLevelEdges,
}: GroupedElementsLayoutParams): {
  layoutNodes: Node[];
  layoutEdges: Edge[];
} => {
  const topLevelGraph = buildDagreGraph();
  const innerLevelGraph = buildDagreGraph("TB");

  topLevelNodes.forEach((node) => {
    const nodeIsGroup = isNodeAGroup(node);

    topLevelGraph.setNode(node.id, {
      width: nodeIsGroup ? groupWidth : nodeWidth,
      height: nodeIsGroup ? groupHeight : nodeHeight,
    });

    if (nodeIsGroup) {
      node.data.nodes.forEach((childNode: Node) => {
        innerLevelGraph.setNode(childNode.id, {
          width: nodeWidth,
          height: nodeHeight,
        });
      });
    }
  });

  const innerLevelNodes = topLevelNodes
    .filter(isNodeAGroup)
    .map((node) => node.data.nodes)
    .flat();

  topLevelEdges.forEach((edge) => {
    topLevelGraph.setEdge(edge.source, edge.target);
  });

  innerLevelEdges.forEach((edge) => {
    innerLevelGraph.setEdge(edge.source, edge.target);
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

    if (isNodeAGroup(topLevelNode)) {
      topLevelNode.data.nodes.forEach((childNode: Node, index: number) => {
        childNode.data.isChildNode = true;

        childNode.targetPosition = Position.Top;
        childNode.sourcePosition = Position.Bottom;

        childNode.position = {
          x: topLevelNode.position.x + groupPadding,
          y:
            topLevelNode.position.y +
            index * (nodeHeight + nodeMargin) +
            groupPadding,
        };
      });
    }
  });

  return {
    layoutNodes: [...topLevelNodes, ...innerLevelNodes],
    layoutEdges: [...topLevelEdges, ...innerLevelEdges],
  };
};
