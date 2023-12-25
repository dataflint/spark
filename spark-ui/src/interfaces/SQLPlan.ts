export type SQLPlans = SQLPlan[];

export interface SQLPlan {
  executionId: number;
  numOfNodes: number;
  nodesPlan: SQLNodePlan[];
  rddScopesToStages?: Record<string, StartAndAttempt>;
}

export interface StartAndAttempt {
  stageId: string;
  attemptId: string;
}

export interface SQLNodePlan {
  id: number;
  planDescription: string;
  rddScopeId?: string;
}
