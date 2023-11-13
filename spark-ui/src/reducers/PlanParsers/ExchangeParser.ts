import { ParsedExchangePlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseExchange(input: string): ParsedExchangePlan {
    const typeRegex = /Exchange (\w+)/;
    const fieldsRegex = /\(([^,]+?)(?:, \d+)?\)/;

    const typeMatch = input.match(typeRegex);
    const fieldsMatch = input.match(fieldsRegex);

    const type = typeMatch ? typeMatch[1] : "";
    const fields = fieldsMatch ? fieldsMatch[1].split(',').map(field => hashNumbersRemover(field.trim())) : [];

    return { type, fields };
}
