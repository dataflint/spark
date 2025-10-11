import {
    Alerts,
    SparkSQLStore,
} from "../../interfaces/AppStore";

/**
 * Check if any of the column names appear in the filters
 * Note: This uses simple substring matching which is not perfect but catches most cases.
 * A more robust solution would parse the filter expressions.
 */
function hasColumnInFilters(columns: string[], filters: string[] | undefined): boolean {
    if (!filters || filters.length === 0) {
        return false;
    }

    const filtersString = filters.join(" ").toLowerCase();
    return columns.some(column => filtersString.includes(column.toLowerCase()));
}

/**
 * Alert reducer for detecting full scans on Delta Lake tables
 * Triggers alerts when:
 * 1. Partitioned Delta Lake table is scanned without partition filters
 * 2. Liquid clustering table is scanned without cluster key filters
 * 3. Z-indexed table is scanned without z-index filters
 */
export function reduceFullScanAlert(
    sql: SparkSQLStore,
    alerts: Alerts,
) {
    sql.sqls.forEach((sql) => {
        sql.nodes.forEach((node) => {
            // Check if this is a scan node with Delta Lake metadata
            const isScanNode = node.nodeName.includes("Scan");
            const hasDeltaLakeScan = node.deltaLakeScan !== undefined;

            if (!isScanNode || !hasDeltaLakeScan) {
                return;
            }

            const deltaLakeScan = node.deltaLakeScan!;
            const parsedPlan = node.parsedPlan;

            // Extract partition filters and data filters from the parsed plan
            let partitionFilters: string[] | undefined;
            let dataFilters: string[] | undefined;

            if (parsedPlan?.type === "FileScan") {
                partitionFilters = parsedPlan.plan.PartitionFilters;
                dataFilters = parsedPlan.plan.PushedFilters;
            }

            // Condition 1: Partitioned Delta Lake table without partition filter
            const hasPartitionFilters = partitionFilters && partitionFilters.length > 0;
            if (deltaLakeScan.partitionColumns.length > 0 && !hasPartitionFilters) {
                const partitionKeys = deltaLakeScan.partitionColumns.join(", ");

                alerts.push({
                    id: `fullScan_partition_${sql.id}_${node.nodeId}`,
                    name: "fullScanPartitionedTable",
                    title: "Full Scan on Partitioned Delta Lake Table",
                    location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
                    message: `Scanning partitioned Delta Lake table without partition filter. This can lead to poor performance as all partitions are being scanned.`,
                    suggestion: `Filter by partition key(s): ${partitionKeys}`,
                    shortSuggestion: `Add partition filter: ${partitionKeys}`,
                    type: "warning",
                    source: {
                        type: "sql",
                        sqlId: sql.id,
                        sqlNodeId: node.nodeId,
                    },
                });
            }

            // Condition 2: Liquid clustering table without cluster key filters
            const hasClusterColumnInFilters = hasColumnInFilters(deltaLakeScan.clusteringColumns, dataFilters);
            if (deltaLakeScan.clusteringColumns.length > 0 && !hasClusterColumnInFilters) {
                const clusterKeys = deltaLakeScan.clusteringColumns.join(", ");

                alerts.push({
                    id: `fullScan_clustering_${sql.id}_${node.nodeId}`,
                    name: "fullScanLiquidClusteringTable",
                    title: "Full Scan on Liquid Clustering Table",
                    location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
                    message: `Scanning liquid clustering table without cluster keys index. This bypasses the clustering optimization and can lead to poor performance.`,
                    suggestion: `Filter by cluster key(s): ${clusterKeys}`,
                    shortSuggestion: `Add cluster key filter: ${clusterKeys}`,
                    type: "warning",
                    source: {
                        type: "sql",
                        sqlId: sql.id,
                        sqlNodeId: node.nodeId,
                    },
                });
            }

            // Condition 3: Z-indexed table (without partitioning) without z-index filters
            const hasZOrderColumnInFilters = hasColumnInFilters(deltaLakeScan.zorderColumns, dataFilters);
            if (deltaLakeScan.zorderColumns.length > 0 &&
                deltaLakeScan.partitionColumns.length === 0 &&
                !hasZOrderColumnInFilters) {
                const zindexFields = deltaLakeScan.zorderColumns.join(", ");

                alerts.push({
                    id: `fullScan_zindex_${sql.id}_${node.nodeId}`,
                    name: "fullScanZIndexTable",
                    title: "Full Scan on Z-Indexed Table",
                    location: `In: SQL query "${sql.description}" (id: ${sql.id}) and node "${node.nodeName}"`,
                    message: `Scanning non-partition table with z-index not by z-index. This bypasses the z-order optimization and can lead to poor performance.`,
                    suggestion: `Filter by z-index field(s): ${zindexFields}`,
                    shortSuggestion: `Add z-index filter: ${zindexFields}`,
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

