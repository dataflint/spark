import { Edge, Node } from "reactflow";
import { EnrichedSparkSQL, GraphFilter } from "../../../interfaces/AppStore";
import { getFlatElementsLayout } from "./dagreLayouts";
import {
  getFlowNodes,
  getGroupNodes,
  getInternalEdges,
  toFlowEdge,
  transformEdgesToGroupEdges,
} from "./layoutServiceBuilders";

export function sqlElementsToFlatLayout(
  sql: EnrichedSparkSQL,
  graphFilter: GraphFilter,
): { layoutNodes: Node[]; layoutEdges: Edge[] } {
  const { edges } = sql.filters[graphFilter];

  const flowNodes = getFlowNodes(sql, graphFilter);
  const flowEdges = edges.map(toFlowEdge);

  const { layoutNodes, layoutEdges } = getFlatElementsLayout(
    flowNodes,
    flowEdges,
  );

  return { layoutNodes: layoutNodes, layoutEdges: layoutEdges };
}

export function sqlElementsToGroupedLayout(
  sql: EnrichedSparkSQL,
  graphFilter: GraphFilter,
): { layoutNodes: Node[]; layoutEdges: Edge[] | [] } {
  const { edges } = sql.filters[graphFilter];

  const flowNodes = getFlowNodes(sql, graphFilter);
  const flowGroupNodes = getGroupNodes(flowNodes);
  const internalEdges = getInternalEdges(flowGroupNodes, edges);
  const nodeAndGroupEdges = transformEdgesToGroupEdges(
    flowNodes,
    flowGroupNodes,
    edges,
  );

  const { layoutNodes, layoutEdges } = getFlatElementsLayout(
    [...flowNodes, ...flowGroupNodes],
    [...nodeAndGroupEdges, ...internalEdges] as Edge[],
  );

  return {
    layoutNodes: layoutNodes,
    layoutEdges: layoutEdges,
  };
}
