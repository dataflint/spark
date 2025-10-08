import { ParseFileScanPlan } from "../../interfaces/AppStore";
import { hashNumbersRemover } from "./PlanParserUtils";

export function parseFileScan(
  input: string,
  nodeName: string,
): ParseFileScanPlan {
  input = hashNumbersRemover(input);
  const result: ParseFileScanPlan = {};

  // Try different location patterns for different file index types
  // InMemoryFileIndex for regular tables
  // TahoeBatchFileIndex/TahoeLogFileIndex/PreparedDeltaFileIndex for Delta Lake
  const locationPatterns = [
    /Location: InMemoryFileIndex\([\w\s]+\)\[(.*?)\]/.exec(input),
    /Location: TahoeBatchFileIndex\([\w\s]+\)\[(.*?)\]/.exec(input),
    /Location: TahoeLogFileIndex\([\w\s]+\)\[(.*?)\]/.exec(input),
    /Location: PreparedDeltaFileIndex\([\w\s]+\)\[(.*?)\]/.exec(input),
  ];

  const matches = {
    format: /Format: (\w+),/.exec(input),
    Location: locationPatterns.find(match => match !== null),
    PartitionFilters: /PartitionFilters: \[(.*?)\]/.exec(input),
    PushedFilters: /PushedFilters: \[(.*?)\]/.exec(input),
    ReadSchema: /ReadSchema: struct<([\w\W]+)>/.exec(input),
  };

  if (matches.format) result.format = matches.format[1];
  if (matches.Location && matches.Location[1].includes("...")) {
    const paths = matches.Location[1].split(",");
    result.Location = paths.length ? paths[0] : undefined;
  } else if (matches.Location) {
    result.Location = matches.Location[1];
  }

  if (matches.PartitionFilters) {
    if (matches.PartitionFilters[1].includes("...")) {
      result.PartitionFilters = undefined;
    } else {
      result.PartitionFilters = matches.PartitionFilters[1]
        .split(",")
        .map((filter) => filter.trim())
        .filter(Boolean);
    }
  }

  if (matches.PushedFilters) {
    if (matches.PushedFilters[1].includes("...")) {
      result.ReadSchema = undefined;
    } else {
      result.PushedFilters = matches.PushedFilters[1]
        .split(",")
        .map((filter) => filter.trim())
        .filter(Boolean);
    }
  }

  if (matches.ReadSchema) {
    if (matches.ReadSchema[1].includes("...")) {
      result.ReadSchema = undefined;
    } else {
      const fields = matches.ReadSchema[1].split(/,(?![^()]*\))/);
      const schema: { [key: string]: string } = {};
      fields.forEach((field) => {
        const [name, type] = field.split(":");
        if (name !== undefined && type !== undefined) {
          schema[name.trim()] = type.trim();
        }
      });
      result.ReadSchema = schema;
    }
  }
  if (nodeName.split(" ").length === 3) {
    result.tableName = nodeName.split(" ")[2];
  }

  return result;
}
