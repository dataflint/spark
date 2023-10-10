import { EnrichedSqlMetric, NodeType } from "../interfaces/AppStore";

const metricAllowlist: Record<NodeType, Array<string>> = {
    "input": ["number of output rows", "number of files read", "size of files read", "number of partitions read"],
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
    "number of partitions read": "partitions read"
}

const nodeTypeDict: Record<string, NodeType> = {
    "LocalTableScan": "input",
    "Range": "input",
    "Execute InsertIntoHadoopFsRelationCommand": "output",
    "CollectLimit": "output",
    "TakeOrderedAndProject": "output",
    "BroadcastHashJoin": "join",
    "SortMergeJoin": "join",
    "BroadcastNestedLoopJoin": "join",
    "filter": "transformation",
}

const nodeRenamerDict: Record<string, string> = {
    "Execute InsertIntoHadoopFsRelationCommand":  "Write to HDFS",
    "LocalTableScan":  "Scan in-memory table",
    "Execute RepairTableCommand":  "Repair table",
    "Execute CreateDataSourceTableCommand": "Create table",
    "Execute DropTableCommand":  "Drop table",
    "SetCatalogAndNamespace": "Set database",
    "TakeOrderedAndProject": "Take Ordered",
    "CollectLimit": "Collect"
}

export function nodeEnrichedNameBuilder(name: string): string {
    if(name.includes("Scan")) {
        const scanRenamed = name.replace("Scan", "Read");
        const scanNameSliced = scanRenamed.split(" ");
       if(scanNameSliced.length > 2) {
            return scanNameSliced.slice(0, 2).join(" ");
       }
       return scanRenamed;
    }
    const renamedNodeName = nodeRenamerDict[name]
    if(renamedNodeName === undefined) {
        return name
    }
    return renamedNodeName
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
    if(name.includes("Scan")) {
        return "input"
    }
    const renamedName = nodeTypeDict[name]
    if(renamedName === undefined) {
        return "other"
    }
    return renamedName
}
