import fixture from "./gluten-sql4-fixture.json";
import { EnrichedSparkSQL, EnrichedSqlEdge, EnrichedSqlNode, SparkStagesStore } from "../../interfaces/AppStore";
import { calcNodeType } from "../SqlReducerUtils";
import { calculateSqlStage } from "../SQLNodeStageReducer";

function buildEnrichedSql(): { sql: EnrichedSparkSQL; stages: SparkStagesStore; jobs: typeof fixture.jobs } {
  const stageBoundaryNames = new Set([
    "ColumnarExchange", "ColumnarBroadcastExchange", "Exchange", "BroadcastExchange",
    "AQEShuffleRead", "VeloxResizeBatches", "RowToVeloxColumnar", "VeloxColumnarToRow",
    "ColumnarCollectLimit", "AdaptiveSparkPlan", "ColumnarUnion",
  ]);

  // Step 1: Enrich nodes with type and wholeStageCodegenId (mimics calculateSql)
  const rawNodes = fixture.sql.nodes.map((node) => {
    const type = calcNodeType(node.nodeName);
    const isCodegenNode = node.nodeName.includes("WholeStageCodegen");
    let wholeStageCodegenId: number | undefined = undefined;
    if (isCodegenNode) {
      wholeStageCodegenId = parseInt(
        node.nodeName
          .replace("WholeStageCodegenTransformer (", "")
          .replace("WholeStageCodegen (", "")
          .replace(")", ""),
      );
    }
    return {
      ...node,
      type,
      isCodegenNode,
      wholeStageCodegenId,
      enrichedName: node.nodeName,
      metrics: node.metrics.map(m => ({ ...m, stageId: undefined as number | undefined })),
    } as unknown as EnrichedSqlNode;
  });

  // Step 2: Gluten codegen ID inference (mimics the logic in SqlReducer.ts)
  const hasGlutenCodegen = rawNodes.some(n => n.isCodegenNode && n.nodeName.includes("Transformer"));
  if (hasGlutenCodegen) {
    const sorted = [...rawNodes].sort((a, b) => a.nodeId - b.nodeId);
    let currentCodegenId: number | undefined = undefined;
    for (const node of sorted) {
      if (node.isCodegenNode) {
        currentCodegenId = node.wholeStageCodegenId;
      } else if (stageBoundaryNames.has(node.nodeName) || node.nodeName.includes("Scan")) {
        currentCodegenId = undefined;
      } else if (currentCodegenId !== undefined && node.wholeStageCodegenId === undefined) {
        (node as any).wholeStageCodegenId = currentCodegenId;
      }
    }
  }

  // Step 3: Separate codegen vs graph nodes
  const codegenNodes = rawNodes
    .filter(n => n.isCodegenNode)
    .map(n => ({ ...n, codegenDuration: undefined as number | undefined, nodeIdFromMetrics: undefined as number | undefined }));
  const graphNodes = rawNodes.filter(n => !n.isCodegenNode);

  // Mark last visible node as output if none exists
  const hasOutput = graphNodes.some(n => n.type === "output");
  if (!hasOutput) {
    const filtered = graphNodes.filter(n => n.nodeName !== "AdaptiveSparkPlan" && n.nodeName !== "ResultQueryStage");
    if (filtered.length > 0) {
      filtered[filtered.length - 1].type = "output";
    }
  }

  const edges: EnrichedSqlEdge[] = fixture.sql.edges.map(e => ({ fromId: e.fromId, toId: e.toId }));

  // Build stages store
  const stages: SparkStagesStore = fixture.stages.map(s => ({
    stageId: s.stageId,
    attemptId: s.attemptId,
    name: "",
    status: s.status,
    numTasks: s.numTasks,
    completedTasks: s.numCompleteTasks,
    failedTasks: s.numFailedTasks,
    activeTasks: s.numActiveTasks,
    pendingTasks: s.numTasks - s.numCompleteTasks - s.numFailedTasks - s.numActiveTasks,
    stageRealTimeDurationMs: undefined,
    stagesRdd: fixture.stagesRdd[String(s.stageId) as keyof typeof fixture.stagesRdd],
    durationDistribution: [0, 0, 0, 0, 0],
    outputDistribution: [0, 0, 0, 0, 0],
    outputRowsDistribution: [0, 0, 0, 0, 0],
    inputDistribution: [0, 0, 0, 0, 0],
    inputRowsDistribution: [0, 0, 0, 0, 0],
    spillDiskDistriution: [0, 0, 0, 0, 0],
    shuffleReadDistribution: [0, 0, 0, 0, 0],
    shuffleWriteDistribution: [0, 0, 0, 0, 0],
    stageProgress: 100,
    metrics: { executorRunTime: s.executorRunTime },
  } as any));

  const sql: EnrichedSparkSQL = {
    id: fixture.sql.id,
    description: fixture.sql.description,
    successJobIds: fixture.sql.successJobIds,
    runningJobIds: fixture.sql.runningJobIds,
    failedJobIds: fixture.sql.failedJobIds,
    nodes: graphNodes,
    edges,
    codegenNodes,
    metricUpdateId: "test",
  } as any;

  return { sql, stages, jobs: fixture.jobs };
}

describe("Gluten/Velox Stage Assignment - SQL 4 (Sort by line count)", () => {
  it("should assign correct stages to all nodes", () => {
    const { sql, stages, jobs } = buildEnrichedSql();

    const result = calculateSqlStage(sql, stages, jobs as any);

    // Debug: print all node stages
    for (const node of result.nodes) {
      const stageInfo = node.stage
        ? node.stage.type === "onestage" ? `onestage:${(node.stage as any).stageId}` 
          : node.stage.type === "exchange" ? `exchange:w=${(node.stage as any).writeStage},r=${(node.stage as any).readStage}` 
          : `${node.stage.type}`
        : "NONE";
      console.log(`  Node ${node.nodeId}: ${node.nodeName.padEnd(45)} stage=${stageInfo} wcid=${node.wholeStageCodegenId}`);
    }

    // Pre-shuffle nodes should be in stage 8
    const scanNode = result.nodes.find(n => n.nodeName === "Scan csv");
    expect(scanNode?.stage?.type).toBe("onestage");
    expect((scanNode?.stage as any)?.stageId).toBe(8);

    const filterNode = result.nodes.find(n => n.nodeName === "FilterExecTransformer");
    expect(filterNode?.stage?.type).toBe("onestage");
    expect((filterNode?.stage as any)?.stageId).toBe(8);

    const flushableAgg = result.nodes.find(n => n.nodeName === "FlushableHashAggregateExecTransformer");
    expect(flushableAgg?.stage?.type).toBe("onestage");
    expect((flushableAgg?.stage as any)?.stageId).toBe(8);

    // ColumnarExchange should be split: write=8, read=10
    const exchange = result.nodes.find(n => n.nodeName === "ColumnarExchange");
    expect(exchange?.stage?.type).toBe("exchange");
    expect((exchange?.stage as any)?.writeStage).toBe(8);
    expect((exchange?.stage as any)?.readStage).toBe(10);

    // Post-shuffle nodes should be in stage 10
    const aqeRead = result.nodes.find(n => n.nodeName === "AQEShuffleRead");
    expect(aqeRead?.stage?.type).toBe("onestage");
    expect((aqeRead?.stage as any)?.stageId).toBe(10);

    const regularAgg = result.nodes.find(n => n.nodeName === "RegularHashAggregateExecTransformer");
    expect(regularAgg?.stage?.type).toBe("onestage");
    expect((regularAgg?.stage as any)?.stageId).toBe(10);

    const takeOrdered = result.nodes.find(n => n.nodeName === "TakeOrderedAndProjectExecTransformer");
    expect(takeOrdered?.stage?.type).toBe("onestage");
    expect((takeOrdered?.stage as any)?.stageId).toBe(10);
  });
});
