import dagre from "dagre";
import { Edge, Node, Position } from "reactflow";
import { v4 as uuidv4 } from "uuid";
import {
  Alert,
  AlertsStore,
  EnrichedSparkSQL,
  EnrichedSqlEdge,
  GraphFilter,
  SQLAlertSourceData
} from "../../interfaces/AppStore";
import { StageNodeName } from "./StageNode";

// Cache for layout results to avoid expensive recalculations
const layoutCache = new Map<string, { layoutNodes: Node[]; layoutEdges: Edge[] }>();

// Cache for edge IDs to avoid regenerating UUIDs
const edgeIdCache = new Map<string, string>();

const nodeWidth = 320;
const nodeHeight = 320;

const getLayoutedElements = (
  nodes: Node[],
  edges: Edge[],
  cacheKey: string,
): { layoutNodes: Node[]; layoutEdges: Edge[] } => {
  // Check cache first to avoid expensive Dagre layout calculation
  const cached = layoutCache.get(cacheKey);
  if (cached) {
    return cached;
  }

  // Create new Dagre graph for layout calculation
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: "LR" });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  // This is the expensive operation - only do it if not cached
  dagre.layout(dagreGraph);

  // Create new nodes with positions (don't mutate input)
  const layoutNodes = nodes.map((node) => {
    const nodeWithPosition = dagreGraph.node(node.id);
    return {
      ...node,
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
      position: {
        x: nodeWithPosition.x - nodeWidth / 2,
        y: nodeWithPosition.y - nodeHeight / 2,
      },
    };
  });

  const result = { layoutNodes, layoutEdges: edges };

  // Cache the result for future use
  layoutCache.set(cacheKey, result);

  return result;
};

class SqlLayoutService {
  static SqlElementsToLayout(
    sql: EnrichedSparkSQL,
    graphFilter: GraphFilter,
    alerts?: AlertsStore, // Alerts store for node-specific alerts
  ): { layoutNodes: Node[]; layoutEdges: Edge[] } {
    // Helper function to find alert for a specific node
    const findNodeAlert = (nodeId: number): Alert | undefined => {
      return alerts?.alerts.find(
        (alert: Alert) => {
          // Type guard to ensure we're dealing with SQL alerts
          if (alert.source.type === "sql") {
            const sqlSource = alert.source as SQLAlertSourceData;
            return sqlSource.sqlNodeId === nodeId && sqlSource.sqlId === sql.id;
          }
          return false;
        }
      );
    };

    // Create cache key based on SQL structure and filter
    const cacheKey = `${sql.uniqueId}-${graphFilter}`;

    // Check if we have a cached result for this exact configuration
    const cached = layoutCache.get(cacheKey);
    if (cached) {
      // Update node data with current metrics and alerts while keeping cached layout
      const updatedNodes = cached.layoutNodes.map(node => ({
        ...node,
        data: {
          ...node.data,
          node: sql.nodes.find(n => n.nodeId.toString() === node.id) || node.data.node,
          sqlUniqueId: sql.uniqueId,
          sqlMetricUpdateId: sql.metricUpdateId,
          alert: findNodeAlert(parseInt(node.id)), // Add alert for this node
        }
      }));

      return { layoutNodes: updatedNodes, layoutEdges: cached.layoutEdges };
    }

    const { nodesIds, edges } = sql.filters[graphFilter];

    // Optimize node filtering and mapping
    const nodeMap = new Map(sql.nodes.map(node => [node.nodeId, node]));
    const flowNodes: Node[] = nodesIds
      .map((nodeId) => {
        const node = nodeMap.get(nodeId);
        if (!node) return null;

        return {
          id: node.nodeId.toString(),
          data: {
            sqlId: sql.id,
            node: node,
            sqlUniqueId: sql.uniqueId,
            sqlMetricUpdateId: sql.metricUpdateId,
            alert: findNodeAlert(node.nodeId), // Add alert for this node
          },
          type: StageNodeName,
          position: { x: 0, y: 0 },
        } as Node;
      })
      .filter((node): node is Node => node !== null);

    // Optimize edge creation with cached IDs
    const flowEdges: Edge[] = edges.map((edge: EnrichedSqlEdge) => {
      const edgeKey = `${edge.fromId}-${edge.toId}`;
      let edgeId = edgeIdCache.get(edgeKey);

      if (!edgeId) {
        edgeId = uuidv4();
        edgeIdCache.set(edgeKey, edgeId);
      }

      return {
        id: edgeId,
        source: edge.fromId.toString(),
        animated: true,
        target: edge.toId.toString(),
      };
    });

    const { layoutNodes, layoutEdges } = getLayoutedElements(
      flowNodes,
      flowEdges,
      cacheKey,
    );

    return { layoutNodes, layoutEdges };
  }

  // Method to clear cache when needed (e.g., on app restart)
  static clearCache(): void {
    layoutCache.clear();
    edgeIdCache.clear();
  }
}

export default SqlLayoutService;
