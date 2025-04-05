import {
    Alerts,
    SparkSQLStore,
} from "../../interfaces/AppStore";

// 1 trillion threshold
const CROSS_JOIN_SCANNED_ROWS_THRESHOLD = 10_000_000_000;

export function reduceLargeCrossJoinScanAlert(
    sql: SparkSQLStore,
    alerts: Alerts,
) {
    sql.sqls.forEach((sql) => {
        sql.nodes.forEach((node) => {
            // Check if this is a cross join node (BroadcastNestedLoopJoin or CartesianProduct)
            if (node.nodeName === "BroadcastNestedLoopJoin" || node.nodeName === "CartesianProduct") {
                // Find the Cross Join Scanned Rows metric
                const crossJoinScannedRowsMetric = node.metrics.find(
                    (metric) => metric.name === "Cross Join Scanned Rows"
                );

                if (crossJoinScannedRowsMetric !== undefined) {
                    // Parse the value as a number
                    const scannedRows = parseFloat(crossJoinScannedRowsMetric.value.replace(/,/g, ""));

                    // Check if the scanned rows exceeds the threshold
                    if (!isNaN(scannedRows) && scannedRows > CROSS_JOIN_SCANNED_ROWS_THRESHOLD) {
                        // Format the number with commas for thousands separators
                        const formattedScannedRows = scannedRows.toLocaleString();

                        alerts.push({
                            id: `largeCrossJoinScan_${sql.id}_${node.nodeId}`,
                            name: "largeCrossJoinScan",
                            title: "Large Cross Join Scan",
                            location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
                            message: `Cross join is scanning ${formattedScannedRows} rows, which is too large and can cause performance issues or query failure`,
                            suggestion: `
  1. Add specific join conditions to convert the cross join to a more efficient join type
  2. Avoid using cross joins for large datasets
`,
                            type: "error",
                            source: {
                                type: "sql",
                                sqlId: sql.id,
                                sqlNodeId: node.nodeId,
                            },
                        });
                    }
                }
            }
        });
    });
} 