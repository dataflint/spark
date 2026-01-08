import dagre from "dagre";
import { Edge, Node, Position } from "reactflow";
import { v4 as uuidv4 } from "uuid";
import {
  Alert,
  AlertsStore,
  EnrichedSparkSQL,
  EnrichedSqlNode,
  GraphFilter,
  SQLAlertSourceData,
  SparkStageStore,
  StageAlertSourceData
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
 * Exchange nodes are now split into write/read nodes that go into their respective stages.
 */
function groupNodesByStage(
  nodes: EnrichedSqlNode[],
  flowNodeIds: Set<string>,
  splitExchangeNodeIds: Set<string>,
  stages?: SparkStageStore[],
  sqlDuration?: number
): Map<number, StageGroup> {
  const stageGroups = new Map<number, StageGroup>();

  // Build a map for O(1) stage lookup
  const stageMap = new Map<number, SparkStageStore>();
  if (stages) {
    for (const stage of stages) {
      stageMap.set(stage.stageId, stage);
    }
  }

  for (const node of nodes) {
    // Skip nodes not in the current filter
    if (!flowNodeIds.has(node.nodeId.toString())) {
      continue;
    }

    const stage = node.stage;
    if (!stage) continue;

    // For exchange nodes that are split, add write/read nodes to their respective stages
    if (stage.type === "exchange" && splitExchangeNodeIds.has(node.nodeId.toString())) {
      const writeNodeId = `write-${node.nodeId}`;
      const readNodeId = `read-${node.nodeId}`;

      // Add write node to write stage
      if (stage.writeStage !== -1) {
        if (!stageGroups.has(stage.writeStage)) {
          // Get actual stage duration from stages data
          const stageData = stageMap.get(stage.writeStage);
          const actualStageDuration = stageData?.stageRealTimeDurationMs ?? node.duration ?? 0;
          stageGroups.set(stage.writeStage, {
            stageId: stage.writeStage,
            status: stageData?.status ?? stage.status,
            stageDuration: actualStageDuration,
            stageInfo: stageData,
            nodeIds: [],
          });
        }
        stageGroups.get(stage.writeStage)!.nodeIds.push(writeNodeId);
      }

      // Add read node to read stage
      if (stage.readStage !== -1) {
        if (!stageGroups.has(stage.readStage)) {
          // Get actual stage duration from stages data
          const stageData = stageMap.get(stage.readStage);
          const actualStageDuration = stageData?.stageRealTimeDurationMs ?? node.duration ?? 0;
          stageGroups.set(stage.readStage, {
            stageId: stage.readStage,
            status: stageData?.status ?? stage.status,
            stageDuration: actualStageDuration,
            stageInfo: stageData,
            nodeIds: [],
          });
        }
        stageGroups.get(stage.readStage)!.nodeIds.push(readNodeId);
      }
      continue;
    }

    // Exchange nodes by name (e.g., "Repartition By Round Robin") that aren't split - keep outside groups
    if (node.nodeName === "Exchange" && !splitExchangeNodeIds.has(node.nodeId.toString())) {
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

  // Calculate duration percentage for each stage using SQL duration
  if (sqlDuration && sqlDuration > 0) {
    stageGroups.forEach((group) => {
      if (group.stageDuration !== undefined) {
        group.durationPercentage = (group.stageDuration / sqlDuration) * 100;
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
    // Update stage group nodes with current alerts and duration data while keeping cached layout
    const updatedNodes = cached.layoutNodes.map(node => {
      if (node.type === StageGroupNodeName) {
        const stageId = node.data.stageId;

        // Get updated duration data from stageGroups
        const stageGroup = stageGroups.get(stageId);
        const updatedDuration = stageGroup?.stageDuration ?? node.data.stageDuration;
        const updatedDurationPercentage = stageGroup?.durationPercentage ?? node.data.durationPercentage;

        // Get alerts for this stage
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

        return {
          ...node,
          data: {
            ...node.data,
            stageDuration: updatedDuration,
            durationPercentage: updatedDurationPercentage,
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
    stages?: SparkStageStore[],
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
    const cacheKey = `${sql.uniqueId}-${graphFilter}-staged-v8`;

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

        // Handle split exchange nodes (write-* and read-*)
        let nodeIdStr = node.id;
        let exchangeVariant: "write" | "read" | undefined = undefined;
        if (node.id.startsWith("write-")) {
          nodeIdStr = node.id.substring(6);
          exchangeVariant = "write";
        } else if (node.id.startsWith("read-")) {
          nodeIdStr = node.id.substring(5);
          exchangeVariant = "read";
        }

        const originalNode = sql.nodes.find(n => n.nodeId.toString() === nodeIdStr);
        return {
          ...node,
          data: {
            ...node.data,
            node: originalNode || node.data.node,
            sqlUniqueId: sql.uniqueId,
            sqlMetricUpdateId: sql.metricUpdateId,
            alert: findNodeAlert(parseInt(nodeIdStr)),
            exchangeVariant: exchangeVariant,
          }
        };
      });

      return { layoutNodes: updatedNodes, layoutEdges: cached.layoutEdges };
    }

    const { nodesIds, edges } = sql.filters[graphFilter];

    // Optimize node filtering and mapping
    const nodeMap = new Map(sql.nodes.map(node => [node.nodeId, node]));
    const flowNodeIds = new Set(nodesIds.map(id => id.toString()));

    // Identify exchange nodes that should be split into write/read nodes
    // Split if they have valid read AND write stages (not -1)
    const splitExchangeNodeIds = new Set<string>();
    for (const nodeId of nodesIds) {
      const node = nodeMap.get(nodeId);
      if (node?.stage?.type === "exchange" &&
        node.stage.readStage !== -1 &&
        node.stage.writeStage !== -1) {
        splitExchangeNodeIds.add(nodeId.toString());
      }
    }

    const flowNodes: Node[] = [];

    for (const nodeId of nodesIds) {
      const node = nodeMap.get(nodeId);
      if (!node) continue;

      // For split exchange nodes, create two visual nodes
      if (splitExchangeNodeIds.has(nodeId.toString())) {
        // Write node
        flowNodes.push({
          id: `write-${node.nodeId}`,
          data: {
            sqlId: sql.id,
            node: node,
            sqlUniqueId: sql.uniqueId,
            sqlMetricUpdateId: sql.metricUpdateId,
            alert: findNodeAlert(node.nodeId),
            exchangeVariant: "write" as const,
          },
          type: StageNodeName,
          position: { x: 0, y: 0 },
        });

        // Read node
        flowNodes.push({
          id: `read-${node.nodeId}`,
          data: {
            sqlId: sql.id,
            node: node,
            sqlUniqueId: sql.uniqueId,
            sqlMetricUpdateId: sql.metricUpdateId,
            alert: findNodeAlert(node.nodeId),
            exchangeVariant: "read" as const,
          },
          type: StageNodeName,
          position: { x: 0, y: 0 },
        });
      } else {
        // Normal node
        flowNodes.push({
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
        });
      }
    }

    // Optimize edge creation with cached IDs
    // For split exchange nodes, redirect edges and add internal write->read edge
    const flowEdges: Edge[] = [];
    const processedExchangeEdges = new Set<string>();

    for (const edge of edges) {
      const sourceId = edge.fromId.toString();
      const targetId = edge.toId.toString();

      // Determine actual source (if source is split exchange, use read node)
      let actualSource = sourceId;
      if (splitExchangeNodeIds.has(sourceId)) {
        actualSource = `read-${sourceId}`;
      }

      // Determine actual target (if target is split exchange, use write node)
      let actualTarget = targetId;
      if (splitExchangeNodeIds.has(targetId)) {
        actualTarget = `write-${targetId}`;
      }

      const edgeKey = `${actualSource}-${actualTarget}`;
      let edgeId = edgeIdCache.get(edgeKey);
      if (!edgeId) {
        edgeId = uuidv4();
        edgeIdCache.set(edgeKey, edgeId);
      }

      flowEdges.push({
        id: edgeId,
        source: actualSource,
        animated: true,
        target: actualTarget,
      });
    }

    // Add internal edges for split exchange nodes (write -> read)
    splitExchangeNodeIds.forEach((exchangeId) => {
      const internalEdgeKey = `write-${exchangeId}-read-${exchangeId}`;
      let internalEdgeId = edgeIdCache.get(internalEdgeKey);
      if (!internalEdgeId) {
        internalEdgeId = uuidv4();
        edgeIdCache.set(internalEdgeKey, internalEdgeId);
      }

      flowEdges.push({
        id: internalEdgeId,
        source: `write-${exchangeId}`,
        animated: true,
        target: `read-${exchangeId}`,
        style: { strokeDasharray: "5,5" }, // Dashed line for shuffle transfer
      });
    });

    // Group nodes by stage
    const stageGroups = groupNodesByStage(sql.nodes, flowNodeIds, splitExchangeNodeIds, stages, sql.duration);

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
