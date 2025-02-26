import { ParsedWindowPlan } from "../../interfaces/AppStore";
import {
  bracedSplit,
  hashNumbersRemover
} from "./PlanParserUtils";

export function parseWindow(input: string): ParsedWindowPlan {
  // Improved regex to correctly capture each part of the window specification
  const regex = /Window \[(.*?)\](?:,\s*\[(.*?)\])?(?:,\s*\[(.*?)\])?/;

  // Remove any unwanted hash numbers
  const sanitizedInput = hashNumbersRemover(input);

  // Match the input string with the regex
  const match = sanitizedInput.match(regex);

  if (!match) {
    return { partitionFields: [], selectFields: [], sortFields: [] };
  }

  // Extract the matched groups (select, partition, sort)
  const selectFields = bracedSplit(match[1]);

  // Handle case when there are no partition or sort fields
  const partitionFields = match[2] ? bracedSplit(match[2]) : [];
  const sortFields = match[3] ? bracedSplit(match[3]) : [];

  return { partitionFields, selectFields, sortFields };
}
