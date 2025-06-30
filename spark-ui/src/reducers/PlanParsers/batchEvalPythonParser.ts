import { ParsedBatchEvalPythonPlan } from "../../interfaces/AppStore";
import { bracedSplit, hashNumbersRemover } from "./PlanParserUtils";

export function parseBatchEvalPython(input: string): ParsedBatchEvalPythonPlan {
    // Remove hash numbers for cleaner parsing
    const cleanedInput = hashNumbersRemover(input);

    // Pattern: Any text followed by [first_list], [second_list] - with flexible whitespace and optional content at end
    const regex = /^.*?\[\s*(.*?)\s*\]\s*,\s*\[\s*(.*?)\s*\].*$/;
    const match = cleanedInput.match(regex);

    if (!match) {
        throw new Error("Invalid Python evaluation input format");
    }

    const [, functionNamesStr, udfNamesStr] = match;

    // Parse the function names list
    let functionNames: string[] = [];
    if (functionNamesStr && functionNamesStr.trim()) {
        functionNames = bracedSplit(functionNamesStr).map(name => name.trim());
    }

    // Parse the UDF names list
    let udfNames: string[] = [];
    if (udfNamesStr && udfNamesStr.trim()) {
        udfNames = bracedSplit(udfNamesStr).map(name => name.trim());
    }

    return {
        functionNames,
        udfNames,
    };
} 