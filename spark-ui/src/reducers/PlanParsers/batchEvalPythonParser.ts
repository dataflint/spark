import { ParsedBatchEvalPythonPlan } from "../../interfaces/AppStore";
import { bracedSplit, hashNumbersRemover } from "./PlanParserUtils";

export function parseBatchEvalPython(input: string): ParsedBatchEvalPythonPlan {
    // Remove hash numbers for cleaner parsing
    const cleanedInput = hashNumbersRemover(input);

    // Pattern: Any text followed by [first_list], [second_list] - with flexible whitespace and optional content at end
    const regex = /^.*?\[\s*(.*?)\s*\]\s*,\s*\[\s*(.*?)\s*\].*$/;
    const match = cleanedInput.match(regex);

    if (match) {
        const [, functionNamesStr, udfNamesStr] = match;
        const functionNames = functionNamesStr?.trim()
            ? bracedSplit(functionNamesStr).map(name => name.trim())
            : [];
        const udfNames = udfNamesStr?.trim()
            ? bracedSplit(udfNamesStr).map(name => name.trim())
            : [];
        return { functionNames, udfNames };
    }

    // MapInPandas/FlatMapGroupsInPandas/FlatMapCoGroupsInPandas have the function name
    // as a bare identifier outside brackets:
    //   "MapInPandas compute_func(col1, col2), [output_cols], false"
    //   "FlatMapGroupsInPandas [group_keys], enrich_group(col1, col2), [output_cols]"
    const funcMatch = cleanedInput.match(/\b(\w+)\([^)]*\)[^,]*,\s*\[/);
    if (funcMatch) {
        return { functionNames: [funcMatch[1]], udfNames: [] };
    }

    throw new Error("Invalid Python evaluation input format");
} 