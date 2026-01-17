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
const stagePadding = 15; // Padding inside stage group
const stageHeaderHeight = 40; // Height of stage header

// Layout parameters - aligned with cleaner algorithm from dataflint-engine
const NODE_SEP = 10;      // Spacing between nodes (vertical in LR layout)
const RANK_SEP = 80;     // Spacing between layers (horizontal in LR layout)
const EDGE_SEP = 80;     // Spacing between edges
const MARGIN_X = 10;      // Horizontal margin
const MARGIN_Y = 10;      // Vertical margin

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

    // For split exchange nodes, add write/read nodes to their respective stages if we have stage info
    if (splitExchangeNodeIds.has(node.nodeId.toString())) {
      const writeNodeId = `write-${node.nodeId}`;
      const readNodeId = `read-${node.nodeId}`;

      // Determine write stage ID: from exchange type or from onestage type (during execution)
      let writeStageId: number | undefined;
      if (stage.type === "exchange" && stage.writeStage !== -1) {
        writeStageId = stage.writeStage;
      } else if (stage.type === "onestage") {
        // During execution, the node might have onestage type from the write stage
        writeStageId = stage.stageId;
      }

      // Determine read stage ID: only from exchange type
      let readStageId: number | undefined;
      if (stage.type === "exchange" && stage.readStage !== -1) {
        readStageId = stage.readStage;
      }

      // Add write node to write stage (even if read stage is not known yet)
      if (writeStageId !== undefined) {
        if (!stageGroups.has(writeStageId)) {
          const stageData = stageMap.get(writeStageId);
          const actualStageDuration = stageData?.stageRealTimeDurationMs ?? node.duration ?? 0;
          stageGroups.set(writeStageId, {
            stageId: writeStageId,
            status: stageData?.status ?? stage.status,
            stageDuration: actualStageDuration,
            stageInfo: stageData,
            nodeIds: [],
          });
        }
        stageGroups.get(writeStageId)!.nodeIds.push(writeNodeId);
      }

      // Add read node to read stage (only when read stage is known)
      if (readStageId !== undefined) {
        if (!stageGroups.has(readStageId)) {
          const stageData = stageMap.get(readStageId);
          const actualStageDuration = stageData?.stageRealTimeDurationMs ?? node.duration ?? 0;
          stageGroups.set(readStageId, {
            stageId: readStageId,
            status: stageData?.status ?? stage.status,
            stageDuration: actualStageDuration,
            stageInfo: stageData,
            nodeIds: [],
          });
        }
        stageGroups.get(readStageId)!.nodeIds.push(readNodeId);
      }
      // Read node stays outside stage groups if read stage is not yet known
      continue;
    }

    // For single-stage nodes, group them by stageId
    if (stage.type === "onestage") {
      const stageId = stage.stageId;

      if (!stageGroups.has(stageId)) {
        // Get actual wall-clock stage duration from stages data
        const stageData = stageMap.get(stageId);
        const wallClockDuration = stageData?.stageRealTimeDurationMs ?? stage.stageDuration;
        stageGroups.set(stageId, {
          stageId,
          status: stageData?.status ?? stage.status,
          stageDuration: wallClockDuration,
          stageInfo: stageData,
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

  // Create new Dagre graph for layout calculation with updated parameters
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({
    rankdir: "LR",
    nodesep: NODE_SEP,
    ranksep: RANK_SEP,
    edgesep: EDGE_SEP,
    marginx: MARGIN_X,
    marginy: MARGIN_Y,
  });

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
 * Calculate stage size based on the number of child nodes.
 * Uses a simple heuristic: arrange nodes in a grid-like pattern.
 */
function calculateStageSize(nodeCount: number): { width: number; height: number } {
  if (nodeCount === 0) {
    return { width: nodeWidth + stagePadding * 2, height: nodeHeight + stagePadding * 2 + stageHeaderHeight };
  }

  // For a single node
  if (nodeCount === 1) {
    return {
      width: nodeWidth + stagePadding * 2,
      height: nodeHeight + stagePadding * 2 + stageHeaderHeight,
    };
  }

  // Estimate layout: dagre will arrange nodes, but we need an estimate
  // Assume roughly sqrt(n) rows and columns for estimation
  const nodesPerRow = Math.ceil(Math.sqrt(nodeCount));
  const numRows = Math.ceil(nodeCount / nodesPerRow);

  const contentWidth = nodesPerRow * (nodeWidth + NODE_SEP) - NODE_SEP;
  const contentHeight = numRows * (nodeHeight + NODE_SEP) - NODE_SEP;

  return {
    width: contentWidth + stagePadding * 2,
    height: contentHeight + stagePadding * 2 + stageHeaderHeight,
  };
}

/**
 * Creates layout with stage grouping using dagre compound nodes.
 * This approach uses dagre's native parent/child support for better join visualization.
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

  const nodeMap = new Map(flowNodes.map(n => [n.id, n]));

  // Build a map from nodeId to stageId for quick lookup
  const nodeToStage = new Map<string, number>();
  stageGroups.forEach((group, stageId) => {
    group.nodeIds.forEach((nodeId) => {
      nodeToStage.set(nodeId, stageId);
    });
  });

  // Identify standalone nodes (not in any stage)
  const standaloneNodeIds = new Set<string>();
  flowNodes.forEach(n => {
    if (!nodeToStage.has(n.id)) {
      standaloneNodeIds.add(n.id);
    }
  });

  // Create dagre graph with compound node support
  const g = new dagre.graphlib.Graph({ compound: true });
  g.setDefaultEdgeLabel(() => ({}));
  g.setGraph({
    rankdir: "LR",
    nodesep: NODE_SEP,
    ranksep: RANK_SEP,
    edgesep: EDGE_SEP,
    marginx: MARGIN_X,
    marginy: MARGIN_Y,
  });

  // Step 1: Add all stage group nodes first (parent nodes must be added before children)
  // Sort stages by stageId to ensure consistent ordering
  const sortedStageIds = Array.from(stageGroups.keys()).sort((a, b) => a - b);

  sortedStageIds.forEach(stageId => {
    const group = stageGroups.get(stageId)!;
    const stageNodeId = `stage-${stageId}`;
    const stageSize = calculateStageSize(group.nodeIds.length);

    g.setNode(stageNodeId, {
      width: stageSize.width,
      height: stageSize.height,
    });
  });

  // Step 2: Add all child nodes and set their parents
  flowNodes.forEach(node => {
    const stageId = nodeToStage.get(node.id);

    g.setNode(node.id, {
      width: nodeWidth,
      height: nodeHeight,
    });

    // Set parent if node belongs to a stage
    if (stageId !== undefined) {
      g.setParent(node.id, `stage-${stageId}`);
    }
  });

  // Step 3: Add all edges
  flowEdges.forEach(edge => {
    g.setEdge(edge.source, edge.target);
  });

  // Step 4: Run dagre layout
  dagre.layout(g);

  // Step 5: Extract positions and build result nodes
  const allNodes: Node[] = [];

  // Create stage group nodes with dagre-calculated positions
  sortedStageIds.forEach(stageId => {
    const group = stageGroups.get(stageId)!;
    const stageNodeId = `stage-${stageId}`;
    const pos = g.node(stageNodeId);

    if (!pos) return;

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
        x: pos.x - pos.width / 2,
        y: pos.y - pos.height / 2,
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
        width: pos.width,
        height: pos.height,
      },
      draggable: false,
      selectable: false,
    };

    allNodes.push(stageGroupNode);
  });

  // Add child nodes with positions relative to their parent stage
  flowNodes.forEach(node => {
    const nodePos = g.node(node.id);
    if (!nodePos) return;

    const stageId = nodeToStage.get(node.id);
    const originalNode = nodeMap.get(node.id);
    if (!originalNode) return;

    // Convert dagre center coords to top-left
    let x = nodePos.x - nodeWidth / 2;
    let y = nodePos.y - nodeHeight / 2;

    // If node has a parent stage, calculate position relative to parent
    if (stageId !== undefined) {
      const stageNodeId = `stage-${stageId}`;
      const parentPos = g.node(stageNodeId);
      if (parentPos) {
        // Parent's top-left corner in absolute coordinates
        const parentX = parentPos.x - parentPos.width / 2;
        const parentY = parentPos.y - parentPos.height / 2;
        // Child position relative to parent
        x = x - parentX;
        // Offset y by half the header height so nodes are centered in content area below header
        y = y - parentY + stageHeaderHeight / 2;
      }
    }

    const layoutedNode: Node = {
      ...originalNode,
      position: { x, y },
      targetPosition: Position.Left,
      sourcePosition: Position.Right,
      ...(stageId !== undefined && {
        parentNode: `stage-${stageId}`,
        extent: "parent" as const,
      }),
    };

    allNodes.push(layoutedNode);
  });

  const result = { layoutNodes: allNodes, layoutEdges: flowEdges };
  layoutCache.set(cacheKey, result);

  return result;
}

class SqlLayoutService {
  static SqlElementsToLayout(
    sql: EnrichedSparkSQL,
    graphFilter: GraphFilter,
    alerts?: AlertsStore,
    stages?: SparkStageStore[],
    showStages: boolean = true,
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

    // Create cache key based on SQL structure, filter, and stage visibility
    // v16: Fixed vertical centering to account for stage header height
    const cacheKey = `${sql.uniqueId}-${graphFilter}-${showStages ? 'staged' : 'unstaged'}-v16`;

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
    // Always split Exchange nodes into shuffle write and shuffle read
    const splitExchangeNodeIds = new Set<string>();
    for (const nodeId of nodesIds) {
      const node = nodeMap.get(nodeId);
      if (node?.nodeName === "Exchange") {
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

    // Group nodes by stage (only if showStages is true)
    if (showStages) {
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
    } else {
      // Use simple layout without stages
      const { layoutNodes, layoutEdges } = getLayoutedElements(flowNodes, flowEdges, cacheKey);

      // Update node data with current metrics and alerts
      const updatedNodes = layoutNodes.map(node => {
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

      return { layoutNodes: updatedNodes, layoutEdges };
    }
  }

  // Method to clear cache when needed
  static clearCache(): void {
    layoutCache.clear();
    edgeIdCache.clear();
  }
}

export default SqlLayoutService;
