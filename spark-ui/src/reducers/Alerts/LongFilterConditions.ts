import { Alerts, SparkSQLStore } from "../../interfaces/AppStore";

const FILTER_CONDITION_TOO_LONG_CHARACTERS_THRESHOLF = 1000;

export function reduceLongFilterConditions(sql: SparkSQLStore, alerts: Alerts) {
  sql.sqls.forEach((sql) => {
    sql.nodes.forEach((node) => {
      const filterCondition = node.nodeName === "Filter" && node.parsedPlan?.type === "Filter" ? node.parsedPlan.plan.condition : undefined;
      if (filterCondition !== undefined && filterCondition.length > FILTER_CONDITION_TOO_LONG_CHARACTERS_THRESHOLF) {
        const filterConditionLength = filterCondition.length;
        alerts.push({
          id: `longFilterCondition${sql.id}_${node.nodeId}_${filterConditionLength}`,
          name: "longFilterCondition",
          title: "Long Filter Condition",
          location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
          message: `Condition length is ${filterConditionLength}, which is too long and can cause performance issues`,
          suggestion: `
    1. Try to convert your filter condition to a join statement, by creating a DF of your filter condition and inner joining it with your main DF
    2. Consider rewriting your filter condition to be shorter
                        `,
          type: "warning",
          source: {
            type: "sql",
            sqlId: sql.id,
            sqlNodeId: node.nodeId,
          },
        });
      }

    });
  });
}