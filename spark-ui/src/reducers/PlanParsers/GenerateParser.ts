import { ParsedGeneratePlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseGenerate(input: string): ParsedGeneratePlan {
    // Remove hash numbers from the input
    const cleanInput = hashNumbersRemover(input);

    // Parse the Generate plan: "Generate explode(tokens), [domain, original_url, type, timestamp, final_url], false, [token]"
    const generateRegex = /^Generate\s+(\w+)\(([^)]+)\),\s*\[([^\]]*)\],\s*(\w+),\s*\[([^\]]*)\]$/;
    const match = cleanInput.match(generateRegex);

    if (!match) {
        throw new Error("Invalid Generate plan format");
    }

    const [, baseOperation, explodeByFieldStr, selectedFieldsStr, outerFlagStr, explodeFieldStr] = match;

    // Validate that this is a supported operation
    const supportedOperations = ["explode", "posexplode", "inline"];
    if (!supportedOperations.includes(baseOperation)) {
        throw new Error(`Unsupported Generate operation: ${baseOperation}`);
    }

    // Determine if it's an outer operation
    const isOuter = outerFlagStr.toLowerCase() === "true";
    const operation = isOuter ? `${baseOperation} outer` : baseOperation;

    // Parse explodeByField (field being exploded) - single field
    const explodeByField = explodeByFieldStr.trim();

    // Parse selectedFields (fields being selected)
    const selectedFields = selectedFieldsStr
        .split(",")
        .map(field => field.trim())
        .filter(field => field.length > 0);

    // Parse explodeField (output fields from explode) - can be multiple fields
    const explodeField = explodeFieldStr
        .split(",")
        .map(field => field.trim())
        .filter(field => field.length > 0);

    return {
        operation,
        explodeByField,
        explodeField,
        selectedFields,
    };
} 