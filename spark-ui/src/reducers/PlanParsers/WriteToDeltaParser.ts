import { ParsedWriteToDeltaPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseWriteToDelta(input: string): ParsedWriteToDeltaPlan {
    // Remove the "Execute WriteIntoDeltaCommand" prefix
    input = input.replace("Execute WriteIntoDeltaCommand", "").trim();

    // Parse the OutputSpec structure: OutputSpec(location, Map(), Vector(fields))
    const outputSpecRegex = /^OutputSpec\(([^,]+),\s*Map\([^)]*\),\s*Vector\(([^)]+)\)\)$/;
    const match = input.match(outputSpecRegex);

    if (!match) {
        throw new Error("Invalid WriteIntoDeltaCommand format");
    }

    const location = match[1].trim();
    const fieldsStr = match[2];

    // Parse the fields from the Vector
    const fields = fieldsStr
        .split(",")
        .map((field) => hashNumbersRemover(field.trim()))
        .filter((field) => field.length > 0);

    return {
        location,
        fields,
    };
}
