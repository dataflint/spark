import { EnrichedSqlMetric, NodeType } from "../interfaces/AppStore";

const metricAllowlist: Record<NodeType, Array<string>> = {
    "input": ["number of output rows", "number of files", "size of files"],
    "output": ["number of written files", "number of output rows", "written output"],
    "join": ["number of output rows"],
    "transformation": ["number of output rows"],
    "other": []
}

export function nodeEnrichedNameBuilder(name: string): string {
    if(name === "Execute InsertIntoHadoopFsRelationCommand") {
        return "Write to HDFS";
    }
    return name
}

export function calcNodeMetrics(type: NodeType, metrics: EnrichedSqlMetric[]): EnrichedSqlMetric[] {
    const allowList = metricAllowlist[type];
    return metrics.filter(metric => allowList.includes(metric.name))
}

export function calcNodeType(name: string): NodeType {
    if(name === "Scan csv" || name === "Scan text") {
        return "input";
    } else if(name === "Execute InsertIntoHadoopFsRelationCommand") {
        return "output";
    } else if(name === "BroadcastHashJoin" || name === "SortMergeJoin" || name === "CollectLimit" || name === "BroadcastNestedLoopJoin") {
        return "join";
    } else if(name === "filter") {
        return "transformation";
    } else {
        return "other"
    }
}
