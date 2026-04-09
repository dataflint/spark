import { ResolvedStageGroup, NodeDurationData } from "./AppStore";

export type NodesMetrics = NodeMetrics[];

export interface NodeMetrics {
  id: number;
  name: string;
  metrics: Metric[];
}

export interface Metric {
  name: string;
  value: string | undefined;
}

/** Enriched response from /dataflint/sqlmetrics — includes duration attribution from backend */
export interface SQLMetricsWithDuration {
  nodeMetrics: NodeMetrics[];
  stageGroups: ResolvedStageGroup[];
  nodeDurations: NodeDurationData[];
}