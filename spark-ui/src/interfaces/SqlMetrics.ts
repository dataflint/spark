export type NodesMetrics = NodeMetrics[];

export interface NodeMetrics {
  id: number;
  name: string;
  metrics: Metric[];
}

export interface Metric {
  name: string;
  value: string;
}
