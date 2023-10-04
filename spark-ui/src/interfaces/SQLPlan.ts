export type SQLPlans = SQLPlan[]

export interface SQLPlan {
  executionId: number
  numOfNodes: number
  nodesPlan: SQLNodePlan[]
}

export interface SQLNodePlan {
  id: number
  planDescription: string
}
