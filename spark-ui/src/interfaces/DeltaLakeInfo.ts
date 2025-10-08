export interface DeltaLakeInfo {
    scans: DeltaLakeScanInfo[];
}

export interface DeltaLakeScanInfo {
    executionId: number;
    nodeId: number;
    tablePath: string;
    tableName: string | null;
    partitionColumns: string[];
    clusteringColumns: string[];
    zorderColumns: string[];
}

