import { EnrichedSqlMetric, NodeType } from "../interfaces/AppStore";

const metricAllowlist: Record<NodeType, Array<string>> = {
    "input": ["number of output rows", "number of files read", "size of files read"],
    "output": ["number of written files", "number of output rows", "written output"],
    "join": ["number of output rows"],
    "transformation": ["number of output rows"],
    "other": []
}

const metricsValueTransformer: Record<string, (value: string) => string> = {
    "size of files read": (value: string) => { 
        const newlineSplit = value.split("\n");
        if(newlineSplit.length < 2){
            return value;
        }
        const bracetSplit = newlineSplit[1].split("(");
        if(bracetSplit.length === 0){
            return value;
        }

        return bracetSplit[0].trim()
    },
}

const metricsRenamer: Record<string, string> = {
    "number of output rows": "rows",
    "number of written files": "files written",
    "written output": "bytes written",
    "number of files read": "file read",
    "size of files read": "bytes read",
}

export function nodeEnrichedNameBuilder(name: string): string {
    if(name === "Execute InsertIntoHadoopFsRelationCommand") {
        return "Write to HDFS";
    }
    return name
}

export function calcNodeMetrics(type: NodeType, metrics: EnrichedSqlMetric[]): EnrichedSqlMetric[] {
    const allowList = metricAllowlist[type];
    return metrics.filter(metric => allowList.includes(metric.name)).map(metric => {
        const valueTransformer = metricsValueTransformer[metric.name]
        return valueTransformer === undefined ? metric : {...metric, value: valueTransformer(metric.value) };
    }).map(metric => {
        const metricNameRenamed = metricsRenamer[metric.name]
        return metricNameRenamed === undefined ? metric : {...metric, name: metricNameRenamed };
    })
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
