import { calcNodeMetrics, nodeEnrichedNameBuilder } from "./SqlReducerUtils";
import { EnrichedSqlMetric, ParsedNodePlan } from "../interfaces/AppStore";

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
    expect(names).toContain("bq rows");
    expect(names).toContain("bq bytes read");
    expect(names).toContain("bq read streams");

    expect(result.find((m) => m.name === "bq rows")?.value).toBe("1000");
    expect(result.find((m) => m.name === "bq bytes read")?.value).toBe("500000");
    expect(result.find((m) => m.name === "bq read streams")?.value).toBe("4");
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
    expect(names).toContain("bq scan time");
    expect(names).toContain("bq parsing time");
    expect(names).toContain("bq time in spark");

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

    expect(result.some((m) => m.name === "bq rows")).toBe(true);
    expect(result.some((m) => m.name === "some unknown metric")).toBe(false);
  });
});