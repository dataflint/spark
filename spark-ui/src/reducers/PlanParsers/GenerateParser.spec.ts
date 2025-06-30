import { ParsedGeneratePlan } from "../../interfaces/AppStore";
import { parseGenerate } from "./GenerateParser";

describe("parseGenerate", () => {
    it("should parse Generate explode operation with the provided example", () => {
        const input = "Generate explode(tokens#68), [domain#2, original_url#3, type#5, timestamp#6, final_url#4], false, [token#77]";
        const expected: ParsedGeneratePlan = {
            operation: "explode",
            explodeByField: "tokens",
            explodeField: ["token"],
            selectedFields: ["domain", "original_url", "type", "timestamp", "final_url"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse Generate explode operation with single selected field", () => {
        const input = "Generate explode(items#123), [id#1], false, [item#456]";
        const expected: ParsedGeneratePlan = {
            operation: "explode",
            explodeByField: "items",
            explodeField: ["item"],
            selectedFields: ["id"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse Generate explode operation with no selected fields", () => {
        const input = "Generate explode(data#100), [], false, [value#200]";
        const expected: ParsedGeneratePlan = {
            operation: "explode",
            explodeByField: "data",
            explodeField: ["value"],
            selectedFields: [],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse Generate explode operation with complex field names", () => {
        const input = "Generate explode(nested_array#1), [user_id#2, user_name#3, complex_field_name#4], false, [array_element#5]";
        const expected: ParsedGeneratePlan = {
            operation: "explode",
            explodeByField: "nested_array",
            explodeField: ["array_element"],
            selectedFields: ["user_id", "user_name", "complex_field_name"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse Generate explode operation with true flag", () => {
        const input = "Generate explode(elements#1), [col1#2, col2#3], true, [element#4]";
        const expected: ParsedGeneratePlan = {
            operation: "explode outer",
            explodeByField: "elements",
            explodeField: ["element"],
            selectedFields: ["col1", "col2"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    // New test cases from JSON data
    it("should parse JSON example: explode with false flag", () => {
        const input = "Generate explode(items#8), [id#7], false, [item#21]";
        const expected: ParsedGeneratePlan = {
            operation: "explode",
            explodeByField: "items",
            explodeField: ["item"],
            selectedFields: ["id"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse JSON example: explode with true flag", () => {
        const input = "Generate explode(items#8), [id#7], true, [item#34]";
        const expected: ParsedGeneratePlan = {
            operation: "explode outer",
            explodeByField: "items",
            explodeField: ["item"],
            selectedFields: ["id"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse JSON example: explode with multiple output fields", () => {
        const input = "Generate explode(properties#54), [id#53], false, [key#66, value#67]";
        const expected: ParsedGeneratePlan = {
            operation: "explode",
            explodeByField: "properties",
            explodeField: ["key", "value"],
            selectedFields: ["id"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse JSON example: posexplode operation", () => {
        const input = "Generate posexplode(items#8), [id#7], false, [position#84, item#85]";
        const expected: ParsedGeneratePlan = {
            operation: "posexplode",
            explodeByField: "items",
            explodeField: ["position", "item"],
            selectedFields: ["id"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse JSON example: inline operation", () => {
        const input = "Generate inline(structs#110), [id#109], true, [_1#122, _2#123]";
        const expected: ParsedGeneratePlan = {
            operation: "inline outer",
            explodeByField: "structs",
            explodeField: ["_1", "_2"],
            selectedFields: ["id"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should throw error for invalid input format", () => {
        const input = "InvalidInput";
        expect(() => parseGenerate(input)).toThrowError("Invalid Generate plan format");
    });

    it("should throw error for malformed Generate plan", () => {
        const input = "Generate explode(field), missing_brackets";
        expect(() => parseGenerate(input)).toThrowError("Invalid Generate plan format");
    });

    it("should throw error for unsupported operation", () => {
        const input = "Generate unsupported_op(array#1), [field#2], false, [output#3]";
        expect(() => parseGenerate(input)).toThrowError("Unsupported Generate operation: unsupported_op");
    });

    it("should handle empty explode field", () => {
        const input = "Generate explode(my_array#1), [field#2], false, []";
        const expected: ParsedGeneratePlan = {
            operation: "explode",
            explodeByField: "my_array",
            explodeField: [],
            selectedFields: ["field"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });

    it("should parse outer posexplode operation", () => {
        const input = "Generate posexplode(data#1), [col#2], true, [pos#3, val#4]";
        const expected: ParsedGeneratePlan = {
            operation: "posexplode outer",
            explodeByField: "data",
            explodeField: ["pos", "val"],
            selectedFields: ["col"],
        };
        expect(parseGenerate(input)).toEqual(expected);
    });
}); 