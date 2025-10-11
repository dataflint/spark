export interface DeltaLakeInfo {
    scans: DeltaLakeScanInfo[];
}

export interface DeltaLakeScanInfo {
    minExecutionId: number;
    tablePath: string;
    tableName: string | null;
    partitionColumns: string[];
    clusteringColumns: string[];
    zorderColumns: string[];
}

