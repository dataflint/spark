import { Edge, Node } from "reactflow";
import { EnrichedSparkSQL, GraphFilter } from "../../../interfaces/AppStore";
import { getFlatElementsLayout } from "./dagreLayouts";
import {
  getFlowNodes,
  getGroupNodes,
  toFlowEdge,
  transformEdgesToGroupEdges,
} from "./layoutServiceBuilders";

class SqlLayoutService {
  static SqlElementsToFlatLayout(
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
  static SqlElementsToGroupedLayout(
    sql: EnrichedSparkSQL,
    graphFilter: GraphFilter,
  ): { layoutNodes: Node[]; layoutEdges: Edge[] | [] } {
    const { edges } = sql.filters[graphFilter];

    const flowNodes = getFlowNodes(sql, graphFilter);
    const flowGroupNodes = getGroupNodes(flowNodes);
    const nodeAndGroupEdges = transformEdgesToGroupEdges(
      flowNodes,
      flowGroupNodes,
      edges,
    );

    return {
      layoutNodes: [...flowNodes, ...flowGroupNodes],
      layoutEdges: nodeAndGroupEdges as Edge[],
    };
  }
}

export default SqlLayoutService;
