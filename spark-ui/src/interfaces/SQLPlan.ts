export type SQLPlans = SQLPlan[];

import { ResolvedStageGroup, NodeDurationData } from "./AppStore";

export interface SQLPlan {
  executionId: number;
  rootExecutionId: number | undefined;
  numOfNodes: number;
  nodesPlan: SQLNodePlan[];
  rddScopesToStages?: Record<string, StartAndAttempt>;
  stageGroups?: ResolvedStageGroup[];
  nodeDurations?: NodeDurationData[];
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
