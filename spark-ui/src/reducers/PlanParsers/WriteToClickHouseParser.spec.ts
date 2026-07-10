import { parseWriteToClickHouse } from "./WriteToClickHouseParser";

describe("parseWriteToClickHouse", () => {
  // What the deployed connector actually renders in the physical plan: the write
  // object's default toString (class@hashcode), which carries no table info.
  it("should return empty fields for the runtime class@hash form (detection still relies on the marker)", () => {
    const input =
      "AppendData Arguments: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$2639@21e38dcb, com.clickhouse.spark.write.ClickHouseWrite@671b3ff1";
    expect(input.includes("ClickHouseWrite")).toBe(true);
    expect(parseWriteToClickHouse(input)).toEqual({ database: "", tableName: "" });
  });

  // Forward-compatible: if a connector version prints Write.description instead.
  it("should parse database and table when the description form is present", () => {
    const input =
      "AppendData Arguments: ClickHouseWrite(database=default, table=sales_by_store)})";
    expect(parseWriteToClickHouse(input)).toEqual({
      database: "default",
      tableName: "sales_by_store",
    });
  });

  it("should return empty strings when the description does not match", () => {
    expect(parseWriteToClickHouse("AppendData something else")).toEqual({
      database: "",
      tableName: "",
    });
  });
});
