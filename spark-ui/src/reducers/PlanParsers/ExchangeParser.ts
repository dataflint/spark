import { ParsedExchangePlan } from "../../interfaces/AppStore";
import { bracedSplit, hashNumbersRemover } from "./PlanParserUtils";

export function parseExchange(input: string): ParsedExchangePlan {
    const typeRegex = /Exchange (\w+)/;

    const typeMatch = input.match(typeRegex);

    const parenthesisContent = input.match(/\(([^)]+)\)/)?.[1] ?? '';
    const allFields = bracedSplit(parenthesisContent).map(field => hashNumbersRemover(field.trim()));
    // Remove the last element if it is a number (partition number)
    if (allFields.length > 0 && !isNaN(Number(allFields[allFields.length - 1]))) {
        allFields.pop();
    }

    const type = typeMatch ? typeMatch[1] : "";

    return { type, fields: allFields };
}
