import { parseExchange } from "./ExchangeParser";

describe("parseExchange", () => {
  test("parses hash partitioning correctly", () => {
    const input =
      "Exchange hashpartitioning(ss_quantity#9, 200), REPARTITION_BY_COL, [plan_id=40]";
    expect(parseExchange(input)).toEqual({
      type: "hashpartitioning",
      fields: ["ss_quantity"],
      isBroadcast: false,
    });
  });

  test("parses single partition correctly", () => {
    const input =
      "Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=14514]";
    expect(parseExchange(input)).toEqual({
      type: "SinglePartition",
      fields: [],
      isBroadcast: false,
    });
  });

  test("parses range partitioning correctly", () => {
    const input =
      "Exchange rangepartitioning(ca_county#787 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=83408]";
    expect(parseExchange(input)).toEqual({
      type: "rangepartitioning",
      fields: ["ca_county ASC NULLS FIRST"],
      isBroadcast: false,
    });
  });

  test("parses broadcast correctly", () => {
    const input =
      "Exchange SinglePartition, EXECUTOR_BROADCAST, [plan_id=270]";
    expect(parseExchange(input)).toEqual({
      type: "SinglePartition",
      fields: [],
      isBroadcast: true,
    });
  });

  test("parses delta optimized write partitioning correctly", () => {
    const input =
      "Exchange deltaoptimizedwritepartitioning((spark.databricks.delta.optimize.minFileSize,268435456), (spark.databricks.delta.autoCompact.maxFileSize,134217728), (spark.databricks.delta.optimize.maxFileSize,268435456), (spark.databricks.delta.autoCompact.minFileSize,67108864)), DELTA_OPTIMIZED_WRITE, [plan_id=10033]";
    expect(parseExchange(input)).toEqual({
      type: "deltaoptimizedwrites",
      fields: [],
      isBroadcast: false,
      deltaOptimizeWrite: {
        minFileSize: "256.00 MB",
        maxFileSize: "256.00 MB",
        autoCompactMinFileSize: "64.00 MB",
        autoCompactMaxFileSize: "128.00 MB",
      },
    });
  });

  // Add more test cases as necessary
});
