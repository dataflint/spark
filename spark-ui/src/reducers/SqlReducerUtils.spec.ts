import { calcNodeMetrics, nodeEnrichedNameBuilder } from "./SqlReducerUtils";
import { EnrichedSqlMetric, ParsedNodePlan } from "../interfaces/AppStore";

describe("nodeEnrichedNameBuilder - Iceberg write", () => {
  it("should return 'Iceberg - Overwrite by Expression' for OverwriteByExpression with IcebergWrite plan", () => {
    const plan: ParsedNodePlan = {
      type: "WriteToIceberg",
      plan: { tableName: "catalog.db.table", format: "PARQUET", tableType: "Iceberg" },
    };
    expect(nodeEnrichedNameBuilder("OverwriteByExpression", plan)).toBe("Iceberg - Overwrite by Expression");
  });

  it("should return 'Iceberg - Overwrite Partitions Dynamic' for OverwritePartitionsDynamic with IcebergWrite plan", () => {
    const plan: ParsedNodePlan = {
      type: "WriteToIceberg",
      plan: { tableName: "catalog.db.table", format: "PARQUET", tableType: "Iceberg" },
    };
    expect(nodeEnrichedNameBuilder("OverwritePartitionsDynamic", plan)).toBe("Iceberg - Overwrite Partitions Dynamic");
  });

  it("should return 'Iceberg - Append data' for AppendData with IcebergWrite plan", () => {
    const plan: ParsedNodePlan = {
      type: "WriteToIceberg",
      plan: { tableName: "catalog.db.events", format: "ORC", tableType: "Iceberg" },
    };
    expect(nodeEnrichedNameBuilder("AppendData", plan)).toBe("Iceberg - Append data");
  });

  it("should return generic name for OverwriteByExpression without IcebergWrite plan", () => {
    expect(nodeEnrichedNameBuilder("OverwriteByExpression", undefined)).toBe("Overwrite by Expression");
  });

  it("should return generic name for OverwritePartitionsDynamic without IcebergWrite plan", () => {
    expect(nodeEnrichedNameBuilder("OverwritePartitionsDynamic", undefined)).toBe("Overwrite Partitions Dynamic");
  });

  it("should return generic name for AppendData without IcebergWrite plan", () => {
    expect(nodeEnrichedNameBuilder("AppendData", undefined)).toBe("Append data");
  });

  it("should return generic name for ReplaceData without IcebergWrite plan", () => {
    expect(nodeEnrichedNameBuilder("ReplaceData", undefined)).toBe("Replace data");
  });

  it("should return generic name for WriteDelta without IcebergWrite plan", () => {
    expect(nodeEnrichedNameBuilder("WriteDelta", undefined)).toBe("Write Delta");
  });

  it("should return 'Iceberg - Replace data' for ReplaceData with IcebergWrite plan", () => {
    const plan: ParsedNodePlan = {
      type: "WriteToIceberg",
      plan: { tableName: "catalog.db.table", format: "PARQUET", tableType: "Iceberg" },
    };
    expect(nodeEnrichedNameBuilder("ReplaceData", plan)).toBe("Iceberg - Replace data");
  });

  it("should return 'Iceberg - Write Delta' for WriteDelta with IcebergWrite plan", () => {
    const plan: ParsedNodePlan = {
      type: "WriteToIceberg",
      plan: { tableName: "catalog.db.table", format: "PARQUET", tableType: "Iceberg" },
    };
    expect(nodeEnrichedNameBuilder("WriteDelta", plan)).toBe("Iceberg - Write Delta");
  });
});

describe("nodeEnrichedNameBuilder - BigQuery", () => {
  it("should return 'BigQuery Read' for a FileScan plan with isBigQueryRead", () => {
    const plan: ParsedNodePlan = {
      type: "FileScan",
      plan: {
        tableName: "myproject.mydataset.mytable",
        isBigQueryRead: true,
      },
    };
    expect(nodeEnrichedNameBuilder("BatchScan myproject.mydataset.mytable", plan)).toBe("BigQuery Read");
  });

  it("should NOT return 'BigQuery Read' for a FileScan plan without isBigQueryRead", () => {
    const plan: ParsedNodePlan = {
      type: "FileScan",
      plan: {
        tableName: "my_iceberg_table",
        isIcebergRead: true,
      },
    };
    const result = nodeEnrichedNameBuilder("BatchScan my_iceberg_table", plan);
    expect(result).not.toBe("BigQuery Read");
  });
});

describe("calcNodeMetrics - BigQuery metrics", () => {
  it("should rename and pass through BQ count metrics", () => {
    const metrics: EnrichedSqlMetric[] = [
      { name: "number of BQ rows read", value: "1000" },
      { name: "number of BQ bytes read", value: "500000" },
      { name: "number of read streams", value: "4" },
    ];
    const result = calcNodeMetrics("input", metrics);

    const names = result.map((m) => m.name);
    expect(names).toContain("rows");
    expect(names).toContain("bytes read");
    expect(names).toContain("number of read streams");

    expect(result.find((m) => m.name === "rows")?.value).toBe("1000");
    expect(result.find((m) => m.name === "bytes read")?.value).toBe("500000");
    expect(result.find((m) => m.name === "number of read streams")?.value).toBe("4");
  });

  it("should extract total and rename BQ time metrics", () => {
    const summaryValue = "total (min, med, max (stageId: taskId))\n120 ms (10 ms, 50 ms, 120 ms (stage 1.0: task 5))";
    const metrics: EnrichedSqlMetric[] = [
      { name: "scan time for BQ", value: summaryValue },
      { name: "parsing time for BQ", value: summaryValue },
      { name: "time spent in spark", value: summaryValue },
    ];
    const result = calcNodeMetrics("input", metrics);

    const names = result.map((m) => m.name);
    expect(names).toContain("scan time");
    expect(names).toContain("parsing time");
    expect(names).toContain("time in spark");

    // All three should have their total extracted ("120 ms")
    result.forEach((m) => {
      expect(m.value).toBe("120 ms");
    });
  });

  it("should filter out non-allowlisted metrics and keep BQ ones", () => {
    const metrics: EnrichedSqlMetric[] = [
      { name: "number of BQ rows read", value: "500" },
      { name: "some unknown metric", value: "xyz" },
    ];
    const result = calcNodeMetrics("input", metrics);

    expect(result.some((m) => m.name === "rows")).toBe(true);
    expect(result.some((m) => m.name === "some unknown metric")).toBe(false);
  });
});