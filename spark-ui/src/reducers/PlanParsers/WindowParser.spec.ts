import { ParsedWindowPlan } from "../../interfaces/AppStore";
import { parseWindow } from "./WindowParser";

describe("parseWindow", () => {
  it("should parse simple Window", () => {
    const input =
      "Window [approx_count_distinct(user_id#0, 0.05, 0, 0) windowspecdefinition(category#2, day#3, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS distinct_users#113L], [category#2, day#3]";
    const expected: ParsedWindowPlan = {
      selectFields: ["approx_count_distinct(user_id, 0.05, 0, 0) windowspecdefinition(category, day, specifiedwindowframe(RowFrame, unboundedpreceding$(), unboundedfollowing$())) AS distinct_users"],
      partitionFields: ["category", "day"],
      sortFields: [],
    };
    expect(parseWindow(input)).toEqual(expected);
  });

  it("should parse window with sort field", () => {
    const input =
      "Window [row_number() windowspecdefinition(category#2, day#3 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_number#63004], [category#2], [day#3 ASC NULLS FIRST]";
    const expected: ParsedWindowPlan = {
      selectFields: ["row_number() windowspecdefinition(category, day ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_number"],
      partitionFields: ["category"],
      sortFields: ["day ASC NULLS FIRST"],
    };
    expect(parseWindow(input)).toEqual(expected);
  });

  it("should parse window with sort field", () => {
    const input = "Window [row_number() windowspecdefinition(category#2, day#3 ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_number#63004], [category#2], [day#3 ASC NULLS FIRST]"
    const expected: ParsedWindowPlan = {
      selectFields: ["row_number() windowspecdefinition(category, day ASC NULLS FIRST, specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())) AS row_number"],
      partitionFields: ["category"],
      sortFields: ["day ASC NULLS FIRST"],
    };
    expect(parseWindow(input)).toEqual(expected);
  });



})