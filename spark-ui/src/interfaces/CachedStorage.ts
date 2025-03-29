export interface DataflintExecutorStorageInfo {
    memoryUsed: number;
    memoryRemaining: number;
    memoryUsagePercentage: number;
}

export interface RddStorageInfo {
    rddId: number;
    memoryUsed: number;
    diskUsed: number;
    numOfPartitions: number;
    storageLevel: string;
    maxMemoryExecutorInfo: DataflintExecutorStorageInfo | undefined;
}

export interface CachedStorage {
    [stageId: string]: RddStorageInfo[];
}
