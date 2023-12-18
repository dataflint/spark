import { ParsedJoinPlan } from "../../interfaces/AppStore";
import {
  bracedSplit,
  hashNumbersRemover,
  removeFromEnd,
  removeFromStart,
} from "./PlanParserUtils";

export function parseJoin(input: string): ParsedJoinPlan {
  if (input.startsWith("BroadcastNestedLoopJoin")) {
    return { joinType: "BroadcastNestedLoopJoin", joinSideType: "Cross" };
  }

  const regex = /^(\w+)\s+\[(.*?)\], \[(.*?)\], (\w+)(?:,\s+(.*))?$/;
  const match = hashNumbersRemover(input).match(regex);

  if (!match) {
    throw new Error("Invalid input format");
  }

  const [, joinType, leftKeysStr, rightKeysStr, joinSideType, conditionStr] =
    match;
  let leftKeys: string[] | undefined;
  let rightKeys: string[] | undefined;
  let joinCondition: string | undefined;

  if (leftKeysStr) {
    leftKeys = bracedSplit(leftKeysStr);
  }

  if (rightKeysStr) {
    rightKeys = bracedSplit(rightKeysStr);
  }

  if (conditionStr) {
    joinCondition = conditionStr;
    joinCondition = removeFromEnd(joinCondition, ", false");
    joinCondition = removeFromStart(joinCondition, "BuildRight, ");
    joinCondition = removeFromStart(joinCondition, "BuildLeft, ");
  }

  if (joinCondition === "BuildRight") {
    joinCondition = undefined;
  }
  if (joinCondition === "BuildLeft") {
    joinCondition = undefined;
  }

  return {
    joinType,
    leftKeys,
    rightKeys,
    joinCondition,
    joinSideType,
  };
}
