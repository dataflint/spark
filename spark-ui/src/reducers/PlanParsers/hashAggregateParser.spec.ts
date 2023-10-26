// Test cases
// parseStringTest.ts
import { parseHashAggregate } from "./hashAggregateParser";

describe("parseHashAggregate", () => {
  const testCases = [
    {
      input:
        "HashAggregate(keys=[sr_customer_sk#1188, sr_store_sk#1192], functions=[partial_sum(UnscaledValue(sr_return_amt#1196))])",
      expected: {
        keys: ["sr_customer_sk", "sr_store_sk"],
        functions: ["partial_sum(UnscaledValue(sr_return_amt))"],
        operations: ["partial_sum"],
      },
    },
    {
      input: "HashAggregate(keys=[], functions=[partial_count(1)])",
      expected: {
        keys: [],
        functions: ["partial_count(1)"],
        operations: ["partial_count"],
      },
    },
    {
      input: "HashAggregate(keys=[], functions=[partial_count(1),sum(1)])",
      expected: {
        keys: [],
        functions: ["partial_count(1)", "sum(1)"],
        operations: ["partial_count", "sum"],
      },
    },
    {
      input: "HashAggregate(keys=[], functions=[sum(1),sum(1)])",
      expected: {
        keys: [],
        functions: ["sum(1)", "sum(1)"],
        operations: ["sum"],
      },
    },
    {
      input:
        "HashAggregate(keys=[i_item_id#897, i_item_desc#900, i_category#908, i_class#906, i_current_price#901], functions=[partial_sum(UnscaledValue(ws_ext_sales_price#1263))])",
      expected: {
        keys: [
          "i_item_id",
          "i_item_desc",
          "i_category",
          "i_class",
          "i_current_price",
        ],
        functions: ["partial_sum(UnscaledValue(ws_ext_sales_price))"],
        operations: ["partial_sum"],
      },
    },
    {
      input: "HashAggregate(keys=[], functions=[count(1)])",
      expected: {
        keys: [],
        functions: ["count(1)"],
        operations: ["count"],
      },
    },
    {
      input: "HashAggregate(keys=[speaker#33], functions=[])",
      expected: {
        keys: ["speaker"],
        functions: [],
        operations: [],
      },
    },
    {
      input:
        "HashAggregate(keys=[], functions=[sum(UnscaledValue(ws_ext_ship_cost#96446)), sum(UnscaledValue(ws_net_profit#96451)), count(distinct ws_order_number#96435L)])",
      expected: {
        keys: [],
        functions: [
          "sum(UnscaledValue(ws_ext_ship_cost))",
          "sum(UnscaledValue(ws_net_profit))",
          "count(distinct ws_order_numberL)",
        ],
        operations: ["sum", "count_distinct"],
      },
    },
  ];

  testCases.forEach((testCase, index) => {
    it(`should extract keys, functions, and operations correctly for test case ${
      index + 1
    }`, () => {
      const result = parseHashAggregate(testCase.input);
      expect(result.keys).toEqual(testCase.expected.keys);
      expect(result.functions).toEqual(testCase.expected.functions);
      expect(result.operations).toEqual(testCase.expected.operations);
    });
  });
});
