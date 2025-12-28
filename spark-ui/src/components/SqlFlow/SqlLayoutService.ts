import dagre from "dagre";
import { Edge, Node, Position } from "reactflow";
import { v4 as uuidv4 } from "uuid";
import {
  Alert,
  AlertsStore,
  EnrichedSparkSQL,
  EnrichedSqlEdge,
  EnrichedSqlNode,
  GraphFilter,
  SQLAlertSourceData,
  SparkStageStore,
  StageAlertSourceData,
} from "../../interfaces/AppStore";
import { StageGroupData, StageGroupNodeName } from "./StageGroupNode";
import { StageNodeName } from "./StageNode";

// Cache for layout results to avoid expensive recalculations
const layoutCache = new Map<string, { layoutNodes: Node[]; layoutEdges: Edge[] }>();

// Cache for edge IDs to avoid regenerating UUIDs
const edgeIdCache = new Map<string, string>();

const nodeWidth = 320;
const nodeHeight = 320;
const stagePadding = 20; // Padding inside stage group
const stageHeaderHeight = 40; // Height of stage header
const stageGap = 40; // Gap between stages

interface StageGroupAlert {
  id: string;
  title: string;
  message: string;
  type: "warning" | "error";
  shortSuggestion?: string;
}

interface StageGroup {
  stageId: number;
  status: string;
  stageDuration?: number;
  stageInfo?: SparkStageStore;
  nodeIds: string[];
  attemptCount?: number;
  durationPercentage?: number;
  alerts?: StageGroupAlert[];
}

/**
 * Groups nodes by their stage ID.
 * Exchange nodes (with read/write stages or nodeName === "Exchange") are kept outside of stage groups.
 */
function groupNodesByStage(
  nodes: EnrichedSqlNode[],
  flowNodeIds: Set<string>
): Map<number, StageGroup> {
  const stageGroups = new Map<number, StageGroup>();

  for (const node of nodes) {
    // Skip nodes not in the current filter
    if (!flowNodeIds.has(node.nodeId.toString())) {
      continue;
    }

    const stage = node.stage;
    if (!stage) continue;

    // Exchange nodes with read/write stages span multiple stages - keep them outside groups
    if (stage.type === "exchange") {
      continue;
    }

    // Exchange nodes by name (e.g., "Repartition By Round Robin") should also be outside groups
    // These are nodes with nodeName === "Exchange"
    if (node.nodeName === "Exchange") {
      continue;
    }

    // For single-stage nodes, group them by stageId
    if (stage.type === "onestage") {
      const stageId = stage.stageId;
      
      if (!stageGroups.has(stageId)) {
        stageGroups.set(stageId, {
          stageId,
          status: stage.status,
          stageDuration: stage.stageDuration,
          nodeIds: [],
        });
      }
      
      stageGroups.get(stageId)!.nodeIds.push(node.nodeId.toString());
    }
  }

  // Calculate total duration across all stages for percentage calculation
  let totalDuration = 0;
  stageGroups.forEach((group) => {
    if (group.stageDuration !== undefined) {
      totalDuration += group.stageDuration;
    }
  });

  // Calculate duration percentage for each stage
  if (totalDuration > 0) {
    stageGroups.forEach((group) => {
      if (group.stageDuration !== undefined) {
        group.durationPercentage = (group.stageDuration / totalDuration) * 100;
      }
    });
  }

  return stageGroups;
}

/**
 * Calculates layout for nodes within a stage group using dagre.
 * Returns the positions relative to the stage group's origin.
 * 
 * The algorithm considers not just direct edges within the stage, but also
 * indirect connections through nodes outside the stage (like Exchange nodes).
 */
function layoutNodesInStage(
  nodeIds: string[],
  edges: Edge[],
  allNodes: Map<string, Node>,
  allEdges: Edge[]
): { positions: Map<string, { x: number; y: number }>; width: number; height: number } {
  const positions = new Map<string, { x: number; y: number }>();
  
  if (nodeIds.length === 0) {
    return { positions, width: 0, height: 0 };
  }

  // For a single node, use simple positioning
  if (nodeIds.length === 1) {
    const nodeId = nodeIds[0];
    positions.set(nodeId, {
      x: stagePadding,
      y: stagePadding + stageHeaderHeight,
    });
    return {
      positions,
      width: nodeWidth + stagePadding * 2,
      height: nodeHeight + stagePadding * 2 + stageHeaderHeight,
    };
  }

  const nodeIdSet = new Set(nodeIds);

  // Build adjacency lists for the full graph to find indirect connections
  const outgoingEdges = new Map<string, string[]>();
  const incomingEdges = new Map<string, string[]>();
  
  for (const edge of allEdges) {
    if (!outgoingEdges.has(edge.source)) {
      outgoingEdges.set(edge.source, []);
    }
    outgoingEdges.get(edge.source)!.push(edge.target);
    
    if (!incomingEdges.has(edge.target)) {
      incomingEdges.set(edge.target, []);
    }
    incomingEdges.get(edge.target)!.push(edge.source);
  }

  // Find connections between stage nodes, including indirect ones through outside nodes
  // This handles cases like: NodeA (in stage) -> Exchange (outside) -> NodeB (in stage)
  const stageEdges: Array<{ source: string; target: string }> = [];
  
  // Direct edges within the stage
  for (const edge of edges) {
    if (nodeIdSet.has(edge.source) && nodeIdSet.has(edge.target)) {
      stageEdges.push({ source: edge.source, target: edge.target });
    }
  }
  
  // Find indirect connections (through nodes outside the stage)
  for (const nodeId of nodeIds) {
    // Look for paths: nodeId -> outside node -> another stage node
    const directTargets = outgoingEdges.get(nodeId) || [];
    for (const intermediateNode of directTargets) {
      if (!nodeIdSet.has(intermediateNode)) {
        // This is an outside node, check if it connects to another stage node
        const indirectTargets = outgoingEdges.get(intermediateNode) || [];
        for (const targetNode of indirectTargets) {
          if (nodeIdSet.has(targetNode) && targetNode !== nodeId) {
            // Found indirect connection: nodeId -> intermediateNode -> targetNode
            stageEdges.push({ source: nodeId, target: targetNode });
          }
        }
      }
    }
    
    // Also check reverse: outside node -> nodeId, and that outside node comes from another stage node
    const directSources = incomingEdges.get(nodeId) || [];
    for (const intermediateNode of directSources) {
      if (!nodeIdSet.has(intermediateNode)) {
        const indirectSources = incomingEdges.get(intermediateNode) || [];
        for (const sourceNode of indirectSources) {
          if (nodeIdSet.has(sourceNode) && sourceNode !== nodeId) {
            // Found indirect connection: sourceNode -> intermediateNode -> nodeId
            stageEdges.push({ source: sourceNode, target: nodeId });
          }
        }
      }
    }
  }

  // Create a subgraph for this stage's nodes
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: "LR", ranksep: 60, nodesep: 30 });

  for (const nodeId of nodeIds) {
    dagreGraph.setNode(nodeId, { width: nodeWidth, height: nodeHeight });
  }

  // Add all discovered edges (direct and indirect)
  const addedEdges = new Set<string>();
  for (const edge of stageEdges) {
    const edgeKey = `${edge.source}-${edge.target}`;
    if (!addedEdges.has(edgeKey)) {
      dagreGraph.setEdge(edge.source, edge.target);
      addedEdges.add(edgeKey);
    }
  }

  dagre.layout(dagreGraph);

  let minX = Infinity, minY = Infinity;
  let maxX = -Infinity, maxY = -Infinity;

  for (const nodeId of nodeIds) {
    const pos = dagreGraph.node(nodeId);
    if (pos) {
      minX = Math.min(minX, pos.x - nodeWidth / 2);
      minY = Math.min(minY, pos.y - nodeHeight / 2);
      maxX = Math.max(maxX, pos.x + nodeWidth / 2);
      maxY = Math.max(maxY, pos.y + nodeHeight / 2);
    }
  }

  // Normalize positions to start from (0, 0) with padding
  for (const nodeId of nodeIds) {
    const pos = dagreGraph.node(nodeId);
    if (pos) {
      positions.set(nodeId, {
        x: pos.x - nodeWidth / 2 - minX + stagePadding,
        y: pos.y - nodeHeight / 2 - minY + stagePadding + stageHeaderHeight,
      });
    }
  }

  const width = maxX - minX + stagePadding * 2;
  const height = maxY - minY + stagePadding * 2 + stageHeaderHeight;

  return { positions, width, height };
}

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

/**
 * Creates layout with stage grouping.
 * Nodes are grouped by their stage ID into parent group nodes.
 */
function getLayoutedElementsWithStages(
  flowNodes: Node[],
  flowEdges: Edge[],
  stageGroups: Map<number, StageGroup>,
  cacheKey: string,
  sqlId?: string,
  alerts?: Alert[],
): { layoutNodes: Node[]; layoutEdges: Edge[] } {
  // Check cache first
  const cached = layoutCache.get(cacheKey);
  if (cached) {
    // Update stage group nodes with current alerts while keeping cached layout
    const updatedNodes = cached.layoutNodes.map(node => {
      if (node.type === StageGroupNodeName && alerts && sqlId) {
        const stageId = node.data.stageId;
        const stageAlerts: StageGroupAlert[] = [];
        alerts.forEach((alert) => {
          if (alert.source.type === "stage") {
            const stageSource = alert.source as StageAlertSourceData;
            if (stageSource.sqlId === sqlId && stageSource.stageId === stageId) {
              stageAlerts.push({
                id: alert.id,
                title: alert.title,
                message: alert.message,
                type: alert.type,
                shortSuggestion: alert.shortSuggestion,
              });
            }
          }
        });
        return {
          ...node,
          data: {
            ...node.data,
            alerts: stageAlerts.length > 0 ? stageAlerts : undefined,
          }
        };
      }
      return node;
    });
    return { layoutNodes: updatedNodes, layoutEdges: cached.layoutEdges };
  }

  // If no stage groups, fall back to simple layout
  if (stageGroups.size === 0) {
    return getLayoutedElements(flowNodes, flowEdges, cacheKey);
  }

  const allNodes: Node[] = [];
  const nodeMap = new Map(flowNodes.map(n => [n.id, n]));
  
  // Identify nodes that belong to a stage group vs standalone nodes
  const groupedNodeIds = new Set<string>();
  stageGroups.forEach((group) => {
    group.nodeIds.forEach((nodeId) => {
      groupedNodeIds.add(nodeId);
    });
  });

  // Standalone nodes (exchanges and nodes without stage)
  const standaloneNodes = flowNodes.filter(n => !groupedNodeIds.has(n.id));

  // Create a high-level graph with stage groups as single nodes
  const highLevelGraph = new dagre.graphlib.Graph();
  highLevelGraph.setDefaultEdgeLabel(() => ({}));
  highLevelGraph.setGraph({ rankdir: "LR", ranksep: stageGap, nodesep: stageGap });

  // Calculate dimensions for each stage group
  const stageDimensions = new Map<number, { width: number; height: number; positions: Map<string, { x: number; y: number }> }>();
  
  stageGroups.forEach((group, stageId) => {
    const { positions, width, height } = layoutNodesInStage(
      group.nodeIds,
      flowEdges,
      nodeMap,
      flowEdges
    );
    stageDimensions.set(stageId, { positions, width, height });
    
    // Add stage group as a node in high-level graph
    highLevelGraph.setNode(`stage-${stageId}`, { 
      width: width, 
      height: height
    });
  });

  // Add standalone nodes to high-level graph
  for (const node of standaloneNodes) {
    highLevelGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
  }

  // Add edges to high-level graph (connecting stage groups and standalone nodes)
  for (const edge of flowEdges) {
    const sourceStage = findNodeStage(edge.source, stageGroups);
    const targetStage = findNodeStage(edge.target, stageGroups);
    
    const sourceId = sourceStage !== null ? `stage-${sourceStage}` : edge.source;
    const targetId = targetStage !== null ? `stage-${targetStage}` : edge.target;
    
    // Only add edge if it connects different entities
    if (sourceId !== targetId) {
      highLevelGraph.setEdge(sourceId, targetId);
    }
  }

  // Layout the high-level graph
  dagre.layout(highLevelGraph);

  // Create stage group nodes with calculated positions
  stageGroups.forEach((group, stageId) => {
    const stageNodeId = `stage-${stageId}`;
    const pos = highLevelGraph.node(stageNodeId);
    const dims = stageDimensions.get(stageId)!;
    
    // Find alerts for this stage
    const stageAlerts: StageGroupAlert[] = [];
    if (alerts && sqlId) {
      alerts.forEach((alert) => {
        if (alert.source.type === "stage") {
          const stageSource = alert.source as StageAlertSourceData;
          if (stageSource.sqlId === sqlId && stageSource.stageId === stageId) {
            stageAlerts.push({
              id: alert.id,
              title: alert.title,
              message: alert.message,
              type: alert.type,
              shortSuggestion: alert.shortSuggestion,
            });
          }
        }
      });
    }
    
    const stageGroupNode: Node<StageGroupData> = {
      id: stageNodeId,
      type: StageGroupNodeName,
      position: {
        x: pos.x - dims.width / 2,
        y: pos.y - dims.height / 2,
      },
      data: {
        stageId: group.stageId,
        status: group.status,
        stageDuration: group.stageDuration,
        stageInfo: group.stageInfo,
        nodeCount: group.nodeIds.length,
        attemptCount: group.attemptCount,
        durationPercentage: group.durationPercentage,
        sqlId: sqlId,
        alerts: stageAlerts.length > 0 ? stageAlerts : undefined,
      },
      style: {
        width: dims.width,
        height: dims.height,
      },
      // Don't make it draggable or selectable
      draggable: false,
      selectable: false,
    };
    
    allNodes.push(stageGroupNode);

    // Add child nodes with positions relative to parent
    const childPositions = dims.positions;
    group.nodeIds.forEach((nodeId) => {
      const originalNode = nodeMap.get(nodeId);
      if (originalNode) {
        const relativePos = childPositions.get(nodeId);
        if (relativePos) {
          const childNode: Node = {
            ...originalNode,
            parentNode: stageNodeId,
            extent: "parent" as const,
            position: relativePos,
            targetPosition: Position.Left,
            sourcePosition: Position.Right,
          };
          allNodes.push(childNode);
        }
      }
    });
  });

  // Add standalone nodes with their positions
  for (const node of standaloneNodes) {
    const pos = highLevelGraph.node(node.id);
    if (pos) {
      const standaloneNode: Node = {
        ...node,
        position: {
          x: pos.x - nodeWidth / 2,
          y: pos.y - nodeHeight / 2,
        },
        targetPosition: Position.Left,
        sourcePosition: Position.Right,
      };
      allNodes.push(standaloneNode);
    }
  }

  const result = { layoutNodes: allNodes, layoutEdges: flowEdges };
  layoutCache.set(cacheKey, result);

  return result;
}

/**
 * Finds which stage a node belongs to.
 */
function findNodeStage(nodeId: string, stageGroups: Map<number, StageGroup>): number | null {
  let foundStageId: number | null = null;
  stageGroups.forEach((group, stageId) => {
    if (group.nodeIds.includes(nodeId)) {
      foundStageId = stageId;
    }
  });
  return foundStageId;
}

class SqlLayoutService {
  static SqlElementsToLayout(
    sql: EnrichedSparkSQL,
    graphFilter: GraphFilter,
    alerts?: AlertsStore,
  ): { layoutNodes: Node[]; layoutEdges: Edge[] } {
    // Helper function to find alert for a specific node
    const findNodeAlert = (nodeId: number): Alert | undefined => {
      return alerts?.alerts.find(
        (alert: Alert) => {
          if (alert.source.type === "sql") {
            const sqlSource = alert.source as SQLAlertSourceData;
            return sqlSource.sqlNodeId === nodeId && sqlSource.sqlId === sql.id;
          }
          return false;
        }
      );
    };

    // Create cache key based on SQL structure and filter
    const cacheKey = `${sql.uniqueId}-${graphFilter}-staged-v4`;

    // Check if we have a cached result for this exact configuration
    const cached = layoutCache.get(cacheKey);
    if (cached) {
      // Update node data with current metrics and alerts while keeping cached layout
      const updatedNodes = cached.layoutNodes.map(node => {
        // Update stage group nodes with current alerts
        if (node.type === StageGroupNodeName) {
          const stageId = node.data.stageId;
          const stageAlerts: StageGroupAlert[] = [];
          if (alerts?.alerts) {
            alerts.alerts.forEach((alert) => {
              if (alert.source.type === "stage") {
                const stageSource = alert.source as StageAlertSourceData;
                if (stageSource.sqlId === sql.id && stageSource.stageId === stageId) {
                  stageAlerts.push({
                    id: alert.id,
                    title: alert.title,
                    message: alert.message,
                    type: alert.type,
                    shortSuggestion: alert.shortSuggestion,
                  });
                }
              }
            });
          }
          return {
            ...node,
            data: {
              ...node.data,
              alerts: stageAlerts.length > 0 ? stageAlerts : undefined,
            }
          };
        }
        return {
          ...node,
          data: {
            ...node.data,
            node: sql.nodes.find(n => n.nodeId.toString() === node.id) || node.data.node,
            sqlUniqueId: sql.uniqueId,
            sqlMetricUpdateId: sql.metricUpdateId,
            alert: findNodeAlert(parseInt(node.id)),
          }
        };
      });

      return { layoutNodes: updatedNodes, layoutEdges: cached.layoutEdges };
    }

    const { nodesIds, edges } = sql.filters[graphFilter];

    // Optimize node filtering and mapping
    const nodeMap = new Map(sql.nodes.map(node => [node.nodeId, node]));
    const flowNodeIds = new Set(nodesIds.map(id => id.toString()));
    
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
            alert: findNodeAlert(node.nodeId),
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

    // Group nodes by stage
    const stageGroups = groupNodesByStage(sql.nodes, flowNodeIds);

    // Use staged layout
    const { layoutNodes, layoutEdges } = getLayoutedElementsWithStages(
      flowNodes,
      flowEdges,
      stageGroups,
      cacheKey,
      sql.id,
      alerts?.alerts,
    );

    return { layoutNodes, layoutEdges };
  }

  // Method to clear cache when needed
  static clearCache(): void {
    layoutCache.clear();
    edgeIdCache.clear();
  }
}

export default SqlLayoutService;
