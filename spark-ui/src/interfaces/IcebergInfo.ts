export interface IcebergInfo {
  commitsInfo: IcebergCommitsInfo[];
}

export interface IcebergCommitsInfo {
  executionId: number;
  tableName: string;
  commitId: number;
  operation: string;
  metrics: IcebergCommitMetrics;
}

export interface IcebergCommitMetrics {
  durationMS: number;
  attempts: number;
  addedDataFiles: number;
  removedDataFiles: number;
  totalDataFiles: number;
  addedDeleteFiles: number;
  addedEqualityDeleteFiles: number;
  addedPositionalDeleteFiles: number;
  removedDeleteFiles: number;
  removedEqualityDeleteFiles: number;
  removedPositionalDeleteFiles: number;
  totalDeleteFiles: number;
  addedRecords: number;
  removedRecords: number;
  totalRecords: number;
  addedFilesSizeInBytes: number;
  removedFilesSizeInBytes: number;
  totalFilesSizeInBytes: number;
  addedPositionalDeletes: number;
  removedPositionalDeletes: number;
  totalPositionalDeletes: number;
  addedEqualityDeletes: number;
  removedEqualityDeletes: number;
  totalEqualityDeletes: number;
}
