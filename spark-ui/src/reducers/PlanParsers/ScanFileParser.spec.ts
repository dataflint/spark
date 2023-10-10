import { parseFileScan } from "./ScanFileParser";

describe('parseFileScan', () => {
    const testCases = [
        {
            input: 'FileScan parquet spark_catalog.tpcds.web_sales[...] Format: Parquet, Location: InMemoryFileIndex(1823 paths)[file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451895, ...], PartitionFilters: [isnotnull(ws_sold_date_sk#1274), dynamicpruningexpression(ws_sold_date_sk#1274 IN dynamicpruning#25981)], PushedFilters: [IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_web_site_sk:int,ws_ext_sales_price:decimal(7,2),ws_net_profit:decimal(7,2)>',
            nodeName: "Scan Text",
            expected: {
                format: 'Parquet',
                Location: 'file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451895',
                PartitionFilters: ['isnotnull(ws_sold_date_sk)', 'dynamicpruningexpression(ws_sold_date_sk IN dynamicpruning)'],
                PushedFilters: ['IsNotNull(ws_web_site_sk)'],
                ReadSchema: {
                    ws_web_site_sk: 'int',
                    ws_ext_sales_price: 'decimal(7,2)',
                    ws_net_profit: 'decimal(7,2)'
                }
            }
        },
        {
            input: 'FileScan parquet spark_catalog.tpcds.item[i_item_sk#896,...] Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/item], PartitionFilters: [], PushedFilters: [IsNotNull(i_manufact_id)], ReadSchema: struct<i_item_sk:int,i_brand_id:int,i_brand:string,i_manufact_id:int>',
            nodeName: "Scan Text",
            expected: {
                format: 'Parquet',
                Location: 'file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/item',
                PartitionFilters: [],
                PushedFilters: ['IsNotNull(i_manufact_id)'],
                ReadSchema: {
                    i_item_sk: 'int',
                    i_brand_id: 'int',
                    i_brand: 'string',
                    i_manufact_id: 'int'
                }
            }
        },
        {
            input: 'FileScan parquet spark_catalog.tpcds.web_sales[ws_web_site_sk#1253,ws_ext_sales_price#1263,ws_net_profit#1273,ws_sold_date_sk#1274] Batched: true, DataFilters: [isnotnull(ws_web_site_sk#1253)], Format: Parquet, Location: InMemoryFileIndex(1823 paths)[file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451895, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452165, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452057, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451750, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452314, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451789, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2450984, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451439, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451581, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451701, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451009, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452304, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451175, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451669, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452012, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452095, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452289, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451630, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452222, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452430, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451255, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451824, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452398, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451253, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452000, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451205, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451507, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2450967, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452408, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451409, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452130, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452293, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451124, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452502, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452500, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451020, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452110, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451783, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451237, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452067, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451914, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451520, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452479, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451568, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452059, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2450823, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451236, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451193, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452635, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451932, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452597, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451027, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452587, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451926, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452478, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451448, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451422, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451082, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451680, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452209, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451922, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451886, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452631, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451583, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451815, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451541, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452438, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452389, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451110, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451295, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2450916, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452533, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451290, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452302, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451631, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2450981, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452276, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452311, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451920, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452636, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452218, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452549, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452364, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451556, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451607, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451964, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451951, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451791, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452514, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452440, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451390, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451109, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451638, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451398, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452188, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451055, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451952, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451440, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451012, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451248, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451054, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2452141, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451178, file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_..., PartitionFilters: [isnotnull(ws_sold_date_sk#1274), dynamicpruningexpression(ws_sold_date_sk#1274 IN dynamicpruning#25981)], PushedFilters: [IsNotNull(ws_web_site_sk)], ReadSchema: struct<ws_web_site_sk:int,ws_ext_sales_price:decimal(7,2),ws_net_profit:decimal(7,2)>',
            nodeName: "Scan Text",
            expected: {
                Location: "file:/Users/menishmueli/Documents/GitHub/spark-sql-perf/data/web_sales/ws_sold_date_sk=2451895",
                PartitionFilters: ["isnotnull(ws_sold_date_sk)", "dynamicpruningexpression(ws_sold_date_sk IN dynamicpruning)"],
                PushedFilters: ["IsNotNull(ws_web_site_sk)"],
                ReadSchema: { "ws_ext_sales_price": "decimal(7,2)", "ws_net_profit": "decimal(7,2)", "ws_web_site_sk": "int" },
                format: "Parquet",
                tableName: undefined,
            }
        },
        {
            input: 'FileScan text [value#0] Batched: false, DataFilters: [(length(trim(value#0, None)) > 0)], Format: Text, Location: InMemoryFileIndex(1 paths)[file:/Users/menishmueli/Documents/GitHub/dataflint/spark/spark-plugin/..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<value:string>',
            nodeName: "Scan Text",
            expected: {
                Location: "file:/Users/menishmueli/Documents/GitHub/dataflint/spark/spark-plugin/...",
                PartitionFilters: [],
                PushedFilters: [],
                ReadSchema: {
                    "value": "string",
                },
                format: "Text",
                tableName: undefined
            }
        },
        {
            input: 'FileScan text [value#0] Batched: false, DataFilters: [(length(trim(value#0, None)) > 0)], Format: Text, Location: InMemoryFileIndex(1 paths)[file:/Users/menishmueli/Documents/GitHub/dataflint/spark/spark-plugin/..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<value:string,sds...>',
            nodeName: "Scan Parquet some.table.name",
            expected: {
                Location: "file:/Users/menishmueli/Documents/GitHub/dataflint/spark/spark-plugin/...",
                PartitionFilters: [],
                PushedFilters: [],
                ReadSchema: undefined,
                format: "Text",
                tableName: "some.table.name"
            }
        }
    ];

    testCases.forEach((testCase, idx) => {
        it(`Test Case ${idx + 1}`, () => {
            const result = parseFileScan(testCase.input, testCase.nodeName);
            expect(result).toEqual(testCase.expected);
        });
    });
});
