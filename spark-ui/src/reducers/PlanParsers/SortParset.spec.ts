import { parseSort } from "./SortParser";

describe("parseSort", () => {
  it("should correctly parse the example input", () => {
    const input =
      "Sort [supplier_count#2941L DESC NULLS LAST, p_brand#267 ASC NULLS FIRST, p_type#268 ASC NULLS FIRST, p_size#269L ASC NULLS FIRST], true, 0";
    const expected = {
      fields: [
        "supplier_count DESC NULLS LAST",
        "p_brand ASC NULLS FIRST",
        "p_type ASC NULLS FIRST",
        "p_size ASC NULLS FIRST",
      ],
    };
    expect(parseSort(input)).toEqual(expected);
  });
});
