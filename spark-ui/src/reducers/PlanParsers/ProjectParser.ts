import { ParsedProjectPlan } from "../../interfaces/AppStore";
import { bracedSplit, hashNumbersRemover } from "./PlanParserUtils";

export function parseProject(input: string): ParsedProjectPlan {
  if (input === "Project" || input === "PhotonProject" || input === "GpuProject" || input === "ProjectExecTransformer") {
    return { fields: [] };
  }

  let fieldsStr = input;
  if (fieldsStr.startsWith("ProjectExecTransformer [")) {
    fieldsStr = fieldsStr.replace("ProjectExecTransformer [", "");
  } else if (fieldsStr.startsWith("PhotonProject [")) {
    fieldsStr = fieldsStr.replace("PhotonProject [", "");
  } else if (fieldsStr.startsWith("GpuProject [")) {
    fieldsStr = fieldsStr.replace("GpuProject [", "");
  } else if (fieldsStr.startsWith("Project [")) {
    fieldsStr = fieldsStr.replace("Project [", "");
  }

  // Remove the closing bracket and hash numbers
  fieldsStr = hashNumbersRemover(fieldsStr.slice(0, -1));

  const fields = bracedSplit(fieldsStr).map((field) => field.trim()).filter((field) => field.length > 0);
  return { fields: fields };
}
