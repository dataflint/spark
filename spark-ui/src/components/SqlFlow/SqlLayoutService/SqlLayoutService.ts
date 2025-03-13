import { EnrichedSparkSQL, GraphFilter } from "../../../interfaces/AppStore";
import { getLayoutElements } from "./dagreLayouts";
import {
  getFlowNodes,
  getTopLevelNodes,
  toFlowEdge,
  transformEdgesToGroupEdges,
} from "./layoutServiceBuilders";

const getFlatFlowElements = (
  sql: EnrichedSparkSQL,
  graphFilter: GraphFilter,
) => {
  const { edges } = sql.filters[graphFilter];
  const flowNodes = getFlowNodes(sql, graphFilter);
  const flowEdges = edges.map(toFlowEdge);

  return { flowNodes, flowEdges };
};

const getGroupedFlowElements = (
  sql: EnrichedSparkSQL,
  graphFilter: GraphFilter,
) => {
  const { edges } = sql.filters[graphFilter];

  const flowNodes = getFlowNodes(sql, graphFilter);
  const topLevelNodes = getTopLevelNodes(flowNodes);
  const topLevelEdges = transformEdgesToGroupEdges(
    flowNodes,
    topLevelNodes,
    edges,
  );

  return { flowNodes: topLevelNodes, flowEdges: topLevelEdges };
};

export function sqlElementsToLayout(
  sql: EnrichedSparkSQL,
  graphFilter: GraphFilter,
  useGroupedLayout: boolean,
) {
  const { flowNodes, flowEdges } = useGroupedLayout
    ? getGroupedFlowElements(sql, graphFilter)
    : getFlatFlowElements(sql, graphFilter);

  return getLayoutElements(flowNodes, flowEdges);
}
