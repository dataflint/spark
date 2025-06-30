import { ParsedBatchEvalPythonPlan } from "../../interfaces/AppStore";
import { parseBatchEvalPython } from "./batchEvalPythonParser";

describe("parseBatchEvalPython", () => {
    it("should parse BatchEvalPython with functionNames and udfNames", () => {
        const input =
            "BatchEvalPython [udf_filter_quantity(ss_quantity#9)#46, udf_filter_quantity_2(ss_quantity#9)#48], [pythonUDF0#72, pythonUDF1#73]";
        const expected: ParsedBatchEvalPythonPlan = {
            functionNames: [
                "udf_filter_quantity(ss_quantity)",
                "udf_filter_quantity_2(ss_quantity)",
            ],
            udfNames: ["pythonUDF0", "pythonUDF1"],
        };
        expect(parseBatchEvalPython(input)).toEqual(expected);
    });

    it("should parse BatchEvalPython with single function and UDF", () => {
        const input =
            "BatchEvalPython [my_udf(col1#123)#456], [pythonUDF0#789]";
        const expected: ParsedBatchEvalPythonPlan = {
            functionNames: ["my_udf(col1)"],
            udfNames: ["pythonUDF0"],
        };
        expect(parseBatchEvalPython(input)).toEqual(expected);
    });

    it("should parse BatchEvalPython with empty lists", () => {
        const input = "BatchEvalPython [], []";
        const expected: ParsedBatchEvalPythonPlan = {
            functionNames: [],
            udfNames: [],
        };
        expect(parseBatchEvalPython(input)).toEqual(expected);
    });

    it("should parse BatchEvalPython with complex function names", () => {
        const input =
            "BatchEvalPython [complex_udf(col1#1, col2#2, nested_func(col3#3))#4], [pythonUDF0#5]";
        const expected: ParsedBatchEvalPythonPlan = {
            functionNames: ["complex_udf(col1, col2, nested_func(col3))"],
            udfNames: ["pythonUDF0"],
        };
        expect(parseBatchEvalPython(input)).toEqual(expected);
    });

    it("should parse BatchEvalPython with extra whitespace", () => {
        const input =
            "BatchEvalPython   [  udf_test(col#1)#2  ,  udf_test2(col#3)#4  ]  ,  [  pythonUDF0#5  ,  pythonUDF1#6  ]";
        const expected: ParsedBatchEvalPythonPlan = {
            functionNames: ["udf_test(col)", "udf_test2(col)"],
            udfNames: ["pythonUDF0", "pythonUDF1"],
        };
        expect(parseBatchEvalPython(input)).toEqual(expected);
    });

    it("should throw an error for invalid input format", () => {
        const input = "InvalidInput";
        expect(() => parseBatchEvalPython(input)).toThrowError(
            "Invalid Python evaluation input format"
        );
    });

    it("should throw an error for malformed BatchEvalPython", () => {
        const input = "BatchEvalPython [udf1], missing_second_list";
        expect(() => parseBatchEvalPython(input)).toThrowError(
            "Invalid Python evaluation input format"
        );
    });

    it("should parse ArrowEvalPython with complex function and additional parameters", () => {
        const input =
            "ArrowEvalPython [tokenize_js_pandas(CASE WHEN (isnull(splitted#43) OR (size(splitted#43, true) <= 2)) THEN  ELSE concat( , array_join(transform(slice(splitted#43, 2, size(splitted#43, true)), lambdafunction(split(lambda x#52, </script>, 2)[0], lambda x#52, false)),  , None)) END)#67], [pythonUDF0#116], 200";
        const expected: ParsedBatchEvalPythonPlan = {
            functionNames: [
                "tokenize_js_pandas(CASE WHEN (isnull(splitted) OR (size(splitted, true) <= 2)) THEN  ELSE concat( , array_join(transform(slice(splitted, 2, size(splitted, true)), lambdafunction(split(lambda x, </script>, 2)[0], lambda x, false)),  , None)) END)"
            ],
            udfNames: ["pythonUDF0"],
        };
        expect(parseBatchEvalPython(input)).toEqual(expected);
    });
}); 