import { DeltaOptimizeWrite, ParsedExchangePlan } from "../../interfaces/AppStore";
import { bracedSplit, bytesToHumanReadable, hashNumbersRemover } from "./PlanParserUtils";

function extractNestedParenthesesContent(input: string): string {
  let depth = 0;
  let startIndex = -1;

  for (let i = 0; i < input.length; i++) {
    if (input[i] === '(') {
      if (depth === 0) {
        startIndex = i + 1;
      }
      depth++;
    } else if (input[i] === ')') {
      depth--;
      if (depth === 0 && startIndex !== -1) {
        return input.substring(startIndex, i);
      }
    }
  }

  return "";
}

function parseDeltaOptimizeWrite(input: string): DeltaOptimizeWrite | undefined {
  // Extract configuration values from the nested parentheses
  const configRegex = /\(([^,]+),(\d+)\)/g;
  const configs: { [key: string]: number } = {};

  let match;
  while ((match = configRegex.exec(input)) !== null) {
    const key = match[1];
    const value = parseInt(match[2], 10);
    configs[key] = value;
  }

  if (Object.keys(configs).length === 0) {
    return undefined;
  }

  return {
    minFileSize: bytesToHumanReadable(configs['spark.databricks.delta.optimize.minFileSize'] || 0),
    maxFileSize: bytesToHumanReadable(configs['spark.databricks.delta.optimize.maxFileSize'] || 0),
    autoCompactMinFileSize: bytesToHumanReadable(configs['spark.databricks.delta.autoCompact.minFileSize'] || 0),
    autoCompactMaxFileSize: bytesToHumanReadable(configs['spark.databricks.delta.autoCompact.maxFileSize'] || 0),
  };
}

export function parseExchange(input: string): ParsedExchangePlan {
  const typeRegex = /Exchange (\w+)/;
  const typeMatch = input.match(typeRegex);
  const type = typeMatch ? typeMatch[1] : "";

  const isBroadcast = input.includes("EXECUTOR_BROADCAST");

  // Check if this is a delta optimized write partitioning
  if (type === "deltaoptimizedwritepartitioning") {
    const content = extractNestedParenthesesContent(input);
    const deltaOptimizeWrite = parseDeltaOptimizeWrite(content);

    return {
      type: "deltaoptimizedwrites",
      fields: [],
      isBroadcast: false,
      deltaOptimizeWrite,
    };
  }

  // Standard exchange parsing
  const parenthesisContent = input.match(/\(([^)]+)\)/)?.[1] ?? "";
  const allFields = bracedSplit(parenthesisContent).map((field) =>
    hashNumbersRemover(field.trim()),
  );
  // Remove the last element if it is a number (partition number)
  if (allFields.length > 0 && !isNaN(Number(allFields[allFields.length - 1]))) {
    allFields.pop();
  }

  return { type, fields: allFields, isBroadcast: isBroadcast };
}
