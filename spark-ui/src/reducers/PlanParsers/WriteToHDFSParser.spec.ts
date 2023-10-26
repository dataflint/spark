import { parseWriteToHDFS } from "./WriteToHDFSParser"; // Ensure to export functions from parser.ts

const testData = [
  {
    input:
      "Execute InsertIntoHadoopFsRelationCommand file:/Users/menishmueli/Documents/GitHub/tpch-spark/dbgen/output/Q01, false, CSV, [header=true, path=file:///Users/menishmueli/Documents/GitHub/tpch-spark/dbgen/output/Q01], Overwrite, [l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice), sum(UDF(l_extendedprice, l_discount)), sum(UDF(UDF(l_extendedprice, l_discount), l_tax)), avg(l_quantity), avg(l_extendedprice), avg(l_discount), count(l_quantity)]",
    expected: {
      location:
        "file:/Users/menishmueli/Documents/GitHub/tpch-spark/dbgen/output/Q01",
      format: "CSV",
      mode: "Overwrite",
    },
  },
  {
    input:
      'Execute InsertIntoHadoopFsRelationCommand file:/tmp/output/partitiondata, false, [speaker#76], Parquet, [__partition_columns=["speaker"], path=/tmp/output/partitiondata], Append, [line_id, play_name, speech_number, line_number, speaker, text_entry]',
    expected: {
      location: "file:/tmp/output/partitiondata",
      format: "Parquet",
      partitionKeys: ["speaker"],
      mode: "Append",
    },
  },
  {
    input:
      "Execute InsertIntoHadoopFsRelationCommand file:/tmp/data, false, [speaker#94], Parquet, [path=file:/tmp/data], Append, `spark_catalog`.`local_catalog`.`my_table`, org.apache.spark.sql.execution.datasources.CatalogFileIndex(file:/tmp/data), [line_id, play_name, speech_number, line_number, text_entry, speaker]",
    expected: {
      location: "file:/tmp/data",
      format: "Parquet",
      partitionKeys: ["speaker"],
      mode: "Append",
      tableName: "`spark_catalog`.`local_catalog`.`my_table`",
    },
  },
  {
    input:
      "Execute InsertIntoHadoopFsRelationCommand file:/tmp/data2, false, Parquet, [path=file:/tmp/data2], Append, `spark_catalog`.`local_catalog`.`my_table`, org.apache.spark.sql.execution.datasources.InMemoryFileIndex(file:/tmp/data2), [line_id, play_name, speech_number, line_number, text_entry, speaker]",
    expected: {
      location: "file:/tmp/data2",
      format: "Parquet",
      mode: "Append",
      tableName: "`spark_catalog`.`local_catalog`.`my_table`",
    },
  },
];

describe("parseWriteToHDFS", () => {
  testData.forEach((data, idx) => {
    it(`parses string ${idx + 1} correctly`, () => {
      const result = parseWriteToHDFS(data.input);
      expect(result).toEqual(data.expected);
    });
  });
});
