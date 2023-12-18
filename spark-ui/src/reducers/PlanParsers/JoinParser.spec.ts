import { ParsedJoinPlan } from "../../interfaces/AppStore";
import { parseJoin } from "./JoinParser";

describe("parseJoin", () => {
  it("should parse SortMergeJoin with LeftSemi and condition", () => {
    const input =
      "SortMergeJoin [cs_order_number#1291L], [cs_order_number#40513L], LeftSemi, NOT (cs_warehouse_sk#1288 = cs_warehouse_sk#40510)";
    const expected: ParsedJoinPlan = {
      joinType: "SortMergeJoin",
      leftKeys: ["cs_order_number"],
      rightKeys: ["cs_order_number"],
      joinCondition: "NOT (cs_warehouse_sk = cs_warehouse_sk)",
      joinSideType: "LeftSemi",
    };
    expect(parseJoin(input)).toEqual(expected);
  });

  it("should parse SortMergeJoin without condition", () => {
    const input =
      "SortMergeJoin [coalesce(i_brand_id#39095, 0), isnull(i_brand_id#39095), coalesce(i_class_id#39097, 0), isnull(i_class_id#39097), coalesce(i_category_id#39099, 0), isnull(i_category_id#39099)], [coalesce(i_brand_id#39117, 0), isnull(i_brand_id#39117), coalesce(i_class_id#39119, 0), isnull(i_class_id#39119), coalesce(i_category_id#39121, 0), isnull(i_category_id#39121)], LeftSemi";
    const expected: ParsedJoinPlan = {
      joinType: "SortMergeJoin",
      leftKeys: [
        "coalesce(i_brand_id, 0)",
        "isnull(i_brand_id)",
        "coalesce(i_class_id, 0)",
        "isnull(i_class_id)",
        "coalesce(i_category_id, 0)",
        "isnull(i_category_id)",
      ],
      rightKeys: [
        "coalesce(i_brand_id, 0)",
        "isnull(i_brand_id)",
        "coalesce(i_class_id, 0)",
        "isnull(i_class_id)",
        "coalesce(i_category_id, 0)",
        "isnull(i_category_id)",
      ],
      joinCondition: undefined,
      joinSideType: "LeftSemi",
    };
    expect(parseJoin(input)).toEqual(expected);
  });

  // Add more test cases for other join types as needed

  it("should throw an error for invalid input", () => {
    const input = "InvalidInput";
    expect(() => parseJoin(input)).toThrowError("Invalid input format");
  });

  it("should parse BroadcastNestedLoopJoin with Cross", () => {
    const input = "BroadcastNestedLoopJoin BuildRight, Cross";
    const expected: ParsedJoinPlan = {
      joinType: "BroadcastNestedLoopJoin",
      leftKeys: undefined,
      rightKeys: undefined,
      joinSideType: "Cross",
      joinCondition: undefined,
    };
    expect(parseJoin(input)).toEqual(expected);
  });

  it("should parse BroadcastHashJoin with Inner and condition", () => {
    const input =
      "BroadcastHashJoin [customer_id#21060], [customer_id#21229], Inner, BuildRight, (CASE WHEN (year_total#21078 > 0.000000) THEN (year_total#21217 / year_total#21078) END > CASE WHEN (year_total#21088 > 0.000000) THEN (year_total#21237 / year_total#21088) END), false";
    const expected: ParsedJoinPlan = {
      joinType: "BroadcastHashJoin",
      leftKeys: ["customer_id"],
      rightKeys: ["customer_id"],
      joinSideType: "Inner",
      joinCondition:
        "(CASE WHEN (year_total > 0.000000) THEN (year_total / year_total) END > CASE WHEN (year_total > 0.000000) THEN (year_total / year_total) END)",
    };
    expect(parseJoin(input)).toEqual(expected);
  });

  it("should parse BroadcastHashJoin with Inner and condition without condtition", () => {
    const input =
      "BroadcastHashJoin [cr_returning_addr_sk#2871], [ca_address_sk#780], Inner, BuildRight, false";
    const expected: ParsedJoinPlan = {
      joinType: "BroadcastHashJoin",
      leftKeys: ["cr_returning_addr_sk"],
      rightKeys: ["ca_address_sk"],
      joinSideType: "Inner",
      joinCondition: undefined,
    };
    expect(parseJoin(input)).toEqual(expected);
  });
});
