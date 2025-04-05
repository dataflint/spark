import { Graph } from "graphlib";
import { EnrichedSqlEdge, EnrichedSqlMetric, EnrichedSqlNode } from "../interfaces/AppStore";
import { parseBytesString } from "../utils/FormatUtils";
import { extractTotalFromStatisticsMetric } from "./SqlReducerUtils";

export function generateGraph(
    edges: EnrichedSqlEdge[],
    allNodes: EnrichedSqlNode[],
): Graph {
    var g = new Graph();
    allNodes.forEach((node) => g.setNode(node.nodeId.toString()));
    edges.forEach((edge) =>
        g.setEdge(edge.fromId.toString(), edge.toId.toString()),
    );
    return g;
}

export function findLastNodeWithInputRows(
    node: EnrichedSqlNode,
    graph: Graph,
    allNodes: EnrichedSqlNode[],
): EnrichedSqlNode | null {
    const inputEdges = graph.inEdges(node.nodeId.toString());

    if (!inputEdges || inputEdges.length !== 1) {
        return null;
    }
    const inputEdge = inputEdges[0];
    const inputNode = allNodes.find((n) => n.nodeId.toString() === inputEdge.v);

    if (!inputNode) {
        return null;
    }
    // if node is of type without row count but it does not effect the row count, we need to go more nodes back
    if (inputNode.nodeName === "Project" || inputNode.nodeName === "AQEShuffleRead" || inputNode.nodeName === "Coalesce" || inputNode.nodeName === "Sort" || inputNode.nodeName === "Exchange") {
        return findLastNodeWithInputRows(inputNode, graph, allNodes);
    } else {
        return inputNode;
    }
}

export function findLastInputNodeSize(
    node: EnrichedSqlNode,
    graph: Graph,
    allNodes: EnrichedSqlNode[],
): number | null {
    const inputEdges = graph.inEdges(node.nodeId.toString());

    if (!inputEdges || inputEdges.length !== 1) {
        return null;
    }
    const inputEdge = inputEdges[0];
    const inputNode = allNodes.find((n) => n.nodeId.toString() === inputEdge.v);

    if (!inputNode) {
        return null;
    }

    const inputSizeBytes = getSizeFromMetrics(inputNode.metrics);

    if (inputSizeBytes !== null) {
        return inputSizeBytes;
        // if node is of type without row size but it does not effect the row size, we need to go more nodes back
        // Filter is a special case, it does not effect the row size but it does not have a row size metric. So if the logic works, it will work for large size as well.
    } else if (inputNode.nodeName === "AQEShuffleRead" || inputNode.nodeName === "Coalesce" || inputNode.nodeName === "Sort" || inputNode.nodeName === "Exchange" || inputNode.nodeName === "Filter") {
        return findLastInputNodeSize(inputNode, graph, allNodes);
    } else {
        return null;
    }
}

export function getRowsFromMetrics(metrics: EnrichedSqlMetric[] | undefined): number | null {
    if (!metrics) return null;
    const rowsMetric = metrics.find((m) =>
        m.name.includes("rows") || m.name.includes("shuffle records written")
    );
    if (!rowsMetric) return null;

    const rows = parseFloat(rowsMetric.value.replace(/,/g, ""));
    return isNaN(rows) ? null : rows;
}

export function getSizeFromMetrics(metrics: EnrichedSqlMetric[] | undefined): number | null {
    if (!metrics) return null;
    const metric = metrics.find((metric) =>
        metric.name === "bytes written" ||
        metric.name === "shuffle write"
    );
    const metricValue = metric?.value;
    if (!metricValue) return null;
    const totalBytes = extractTotalFromStatisticsMetric(metricValue);
    if (!totalBytes) return null;

    const bytesWrittenMetric = parseBytesString(totalBytes);
    if (!bytesWrittenMetric) return null;
    return bytesWrittenMetric;
}

export function findTwoNodes(
    node: EnrichedSqlNode,
    graph: Graph,
    allNodes: EnrichedSqlNode[],
): EnrichedSqlNode[] | null {
    const inputEdges = graph.inEdges(node.nodeId.toString());
    if (!inputEdges || inputEdges.length !== 2) {
        return null;
    }

    const inputNodes = inputEdges.map(edge => allNodes.find(n => n.nodeId.toString() === edge.v));

    if (inputNodes.some(n => n === undefined)) {
        return null;
    }

    return inputNodes as EnrichedSqlNode[];
} 