import { parseWriteToIceberg } from "./WriteToIcebergParser";

describe("parseWriteToIceberg", () => {
  it("should parse OverwritePartitionsDynamic IcebergWrite description", () => {
    const input =
      "OverwritePartitionsDynamic org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$6931/1927447599@6911641e, IcebergWrite(table=glue_catalog.dlk_visitor_funnel_dwh_production.full_fact_events, format=PARQUET)";
    const result = parseWriteToIceberg(input);
    expect(result.tableName).toBe("glue_catalog.dlk_visitor_funnel_dwh_production.full_fact_events");
    expect(result.format).toBe("PARQUET");
    expect(result.tableType).toBe("Iceberg");
  });

  it("should parse OverwriteByExpression IcebergWrite description", () => {
    const input =
      "OverwriteByExpression org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$4431/0x0000000801ed6840@47016b3a, IcebergWrite(table=blms.trm_entity_flow_transfer_iceberg_prod.entity_flow_transfer_high_volume_monthly, format=PARQUET)";
    const result = parseWriteToIceberg(input);
    expect(result.tableName).toBe("blms.trm_entity_flow_transfer_iceberg_prod.entity_flow_transfer_high_volume_monthly");
    expect(result.format).toBe("PARQUET");
    expect(result.tableType).toBe("Iceberg");
  });

  it("should parse AppendData IcebergWrite description", () => {
    const input =
      "AppendData org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy$$Lambda$6931/1927447599@6911641e, IcebergWrite(table=catalog.db.events, format=ORC)";
    const result = parseWriteToIceberg(input);
    expect(result.tableName).toBe("catalog.db.events");
    expect(result.format).toBe("ORC");
    expect(result.tableType).toBe("Iceberg");
  });
});