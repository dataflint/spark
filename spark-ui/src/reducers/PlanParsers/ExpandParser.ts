import { ParsedExpandPlan } from "../../interfaces/AppStore";
import { bracedSplit, hashNumbersRemover } from "./PlanParserUtils";

export function parseExpand(input: string): ParsedExpandPlan {
    // Clean input by removing hash numbers
    const cleanInput = hashNumbersRemover(input);

    // Parse the Expand plan: "Expand [[ss_customer_sk#2, null, null, null, 1], [null, ss_item_sk#1, null, null, 2], [null, null, ss_store_sk#6, null, 3], [null, null, null, ss_promo_sk#7, 4]], [ss_customer_sk#60, ss_item_sk#61, ss_store_sk#62, ss_promo_sk#63, gid#59]"
    // We need to extract the output fields from the final bracketed section after the nested arrays

    // Look for the pattern: ]], [fields...]
    const finalBracketRegex = /\]\],\s*\[([^\]]+)\]$/;
    const match = cleanInput.match(finalBracketRegex);

    if (!match) {
        throw new Error("Invalid Expand plan format: could not find output fields");
    }

    const fieldsStr = match[1];

    // Split the fields by comma and clean them up
    const allFields = bracedSplit(fieldsStr)
        .map(field => field.trim())
        .filter(field => field.length > 0);

    if (allFields.length === 0) {
        throw new Error("Invalid Expand plan format: no output fields found");
    }

    // Extract the last field as idField and remove it from the fields array
    const idField = allFields[allFields.length - 1];
    const fields = allFields.slice(0, -1);

    return {
        fields: fields,
        idField: idField
    };
}
