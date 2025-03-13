import { Node } from "reactflow";
import { v4 as uuidv4 } from "uuid";
import {
  EnrichedSparkSQL,
  EnrichedSqlEdge,
  EnrichedSqlNode,
  GraphFilter,
} from "../../../interfaces/AppStore";
import { StageNodeName } from "../StageNode";

const getPosition = (x = 0, y = 0) => ({ x, y });
const getStageIdString = (id = "") => `stage-${id}`;

export const getFlowNodes = (
  sql: EnrichedSparkSQL,
  graphFilter: GraphFilter,
) => {
  const { nodes } = sql;
  const { nodesIds } = sql.filters[graphFilter];

  return nodes
    .filter((node) => nodesIds.includes(node.nodeId))
    .map((node: EnrichedSqlNode) => ({
      id: node.nodeId.toString(),
      data: { sqlId: sql.id, node: node },
      type: StageNodeName,
      position: getPosition(),
    }));
};

export const getGroupNodes = (flowNodes: Node[]) => {
  const allGroupsWithNodes = flowNodes.reduce((stageGroups, node) => {
    const stageId = node.data.node.stage?.stageId;

    if (stageId === undefined) return stageGroups;

    if (!stageGroups.has(stageId)) {
      stageGroups.set(stageId, []);
    }

    stageGroups.get(stageId)?.push(node);

    return stageGroups;
  }, new Map<number, Node[]>());

  return Array.from(allGroupsWithNodes)
    .filter(([_, nodes]) => nodes.length > 1)
    .map(([stageId, nodes]) => ({
      id: getStageIdString(stageId.toString()),
      data: {
        nodes,
      },
      position: getPosition(),
    }));
};

interface ToFlowEdgeParams {
  fromId: string | number;
  toId: string | number;
}
export const toFlowEdge = ({ fromId, toId }: ToFlowEdgeParams) => ({
  id: uuidv4(),
  source: fromId.toString(),
  animated: true,
  target: toId.toString(),
});

const getNodeToGroupMap = (flowNodes: Node[]) =>
  flowNodes.reduce<Record<string, string>>((nodeToStageMap, node) => {
    const stageId = node.data.node.stage?.stageId;
    nodeToStageMap[node.id] = getStageIdString(stageId);
    return nodeToStageMap;
  }, {});

const getResolvedEdgeConnection = (
  nodeId: number,
  nodeIdToStageGroupId: Record<number, string>,
  groupNodes: Node[],
) => {
  const stageGroupId = nodeIdToStageGroupId[nodeId];
  const stageGroup = groupNodes.find((node) => node.id === stageGroupId);

  return stageGroup && stageGroup.data.nodes.length > 1
    ? stageGroupId
    : String(nodeId);
};

export const transformEdgesToGroupEdges = (
  flowNodes: Node[],
  groupNodes: Node[],
  originalEdges: EnrichedSqlEdge[],
) => {
  const nodeIdToStageGroupId = getNodeToGroupMap(flowNodes);

  return originalEdges
    .filter(({ fromId, toId }) => {
      const fromStageGroupId = nodeIdToStageGroupId[fromId];
      const toStageGroupId = nodeIdToStageGroupId[toId];

      return fromStageGroupId !== toStageGroupId;
    })
    .map(({ fromId, toId }) => {
      const resolvedFromId = getResolvedEdgeConnection(
        fromId,
        nodeIdToStageGroupId,
        groupNodes,
      );

      const resolvedToId = getResolvedEdgeConnection(
        toId,
        nodeIdToStageGroupId,
        groupNodes,
      );

      return toFlowEdge({ fromId: resolvedFromId, toId: resolvedToId });
    });
};

export function getInternalEdges(
  flowGroupNodes: Node[],
  originalEdges: EnrichedSqlEdge[],
) {
  return flowGroupNodes
    .map((node) => {
      const groupNodes = node.data.nodes;
      const groupNodeIds = new Set(groupNodes.map((node: Node) => node.id));

      return originalEdges
        .filter(
          ({ fromId, toId }) =>
            groupNodeIds.has(fromId.toString()) &&
            groupNodeIds.has(toId.toString()),
        )
        .map(({ fromId, toId }) =>
          toFlowEdge({
            fromId,
            toId,
          }),
        );
    })
    .flat();
}
