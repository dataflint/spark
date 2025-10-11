export type SQLPlans = SQLPlan[];

export interface SQLPlan {
  executionId: number;
  rootExecutionId: number | undefined;
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
