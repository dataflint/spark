import { ParsedHashAggregatePlan } from "../../interfaces/AppStore";
import { hashNumbersRemover, onlyUnique } from "./PlanParserUtils";

export function parseHashAggregate(input: string): ParsedHashAggregatePlan {
    const cleanInput = hashNumbersRemover(input);
    const keysMatch = cleanInput.match(/keys=\[([^\]]+)\]/);
    const functionsMatch = cleanInput.match(/functions=\[([^\]]+)\]/);

    let keys: string[] = [];
    let functions: string[] = [];
    let operations: string[] = [];

    if (keysMatch && keysMatch[1]) {
        keys = keysMatch[1].split(',').map(key => key.trim());
    }

    if (functionsMatch && functionsMatch[1]) {
        functions = functionsMatch[1].split(',').map(func => func.trim());

        // Extracting only the outermost operation
        operations = functions.map(func => {
            if(func.includes('count(distinct')) {
                return "count_distinct"
            }
            const match = func.match(/^\w+/);
            return match ? match[0] : '';
        }).filter(Boolean).filter(onlyUnique);
    }

    return {
        keys,
        functions,
        operations
    };
}
