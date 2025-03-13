import { Edge, Node } from "reactflow";
import { EnrichedSparkSQL, GraphFilter } from "../../../interfaces/AppStore";
import {
  getFlatElementsLayout,
  getGroupedElementsLayout,
} from "./dagreLayouts";
import {
  getFlowNodes,
  getTopLevelNodes,
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

  return { layoutNodes, layoutEdges };
}

export function sqlElementsToGroupedLayout(
  sql: EnrichedSparkSQL,
  graphFilter: GraphFilter,
): { layoutNodes: Node[]; layoutEdges: Edge[] | [] } {
  const { edges } = sql.filters[graphFilter];

  const flowNodes = getFlowNodes(sql, graphFilter);
  const topLevelNodes = getTopLevelNodes(flowNodes);
  const topLevelEdges = transformEdgesToGroupEdges(
    flowNodes,
    topLevelNodes,
    edges,
  );

  const { layoutNodes, layoutEdges } = getGroupedElementsLayout({
    topLevelNodes,
    topLevelEdges,
  });

  return {
    layoutNodes,
    layoutEdges,
  };
}
