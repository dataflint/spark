import { ParsedWriteToHDFSPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function specialSplit(input: string): string[] {
  const result: string[] = [];
  let buffer = "";
  let bracketCount = 0;
  let inQuotes = false;

  for (let i = 0; i < input.length; i++) {
    const char = input[i];

    if (char === "[") bracketCount++;
    if (char === "]") bracketCount--;
    if (char === '"') inQuotes = !inQuotes;

    if (char === "," && bracketCount === 0 && !inQuotes) {
      result.push(buffer.trim());
      buffer = "";
    } else {
      buffer += char;
    }
  }

  if (buffer) result.push(buffer.trim());
  return result;
}

export function parseWriteToHDFS(input: string): ParsedWriteToHDFSPlan {
  input = input.replace("Execute InsertIntoHadoopFsRelationCommand", "").trim();
  const parts = specialSplit(input);

  let parsed: ParsedWriteToHDFSPlan = {
    location: parts[0],
    format: "unknown",
    mode: "unknown",
    tableName: undefined,
    partitionKeys: undefined,
  };

  if (parts[2].includes("[")) {
    (parsed.partitionKeys = hashNumbersRemover(parts[2].slice(1, -1)).split(
      ",",
    )),
      (parsed.format = parts[3]);
    parsed.mode = parts[5];
  } else {
    parsed.format = parts[2];
    parsed.mode = parts[4];
  }

  if (parts[4].includes("`")) {
    parsed.tableName = parts[4];
  } else if (parts[5].includes("`")) {
    parsed.tableName = parts[5];
  } else if (parts.length > 6 && parts[6].includes("`")) {
    parsed.tableName = parts[6];
  }

  return parsed;
}
