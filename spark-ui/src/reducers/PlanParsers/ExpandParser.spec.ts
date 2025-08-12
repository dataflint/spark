import { parseExpand } from "./ExpandParser";

describe("parseExpand", () => {
    test("should parse expand plan correctly", () => {
        const input =
            "Expand [[ss_customer_sk#2, null, null, null, 1], [null, ss_item_sk#1, null, null, 2], [null, null, ss_store_sk#6, null, 3], [null, null, null, ss_promo_sk#7, 4]], [ss_customer_sk#60, ss_item_sk#61, ss_store_sk#62, ss_promo_sk#63, gid#59]";

        const result = parseExpand(input);

        expect(result).toEqual({
            fields: [
                "ss_customer_sk",
                "ss_item_sk",
                "ss_store_sk",
                "ss_promo_sk"
            ],
            idField: "gid"
        });
    });

    test("should handle expand plan with different field names", () => {
        const input =
            "Expand [[field1#1, null, 0], [null, field2#2, 1]], [output1#10, output2#11, group_id#12]";

        const result = parseExpand(input);

        expect(result).toEqual({
            fields: [
                "output1",
                "output2"
            ],
            idField: "group_id"
        });
    });

    test("should throw error for invalid format", () => {
        const input = "Invalid expand format";

        expect(() => parseExpand(input)).toThrow("Invalid Expand plan format: could not find output fields");
    });
});
