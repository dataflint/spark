import { Alert, SparkSQLStore, StatusStore } from "../interfaces/AppStore";
import { reduceSavingsOpportunity } from "./SavingsOpportunityReducer";

// totalDCU = 10 core-hours * 0.05 + 20 GiB-hours * 0.005 = 0.6
const statusStore = {
  executors: {
    totalCoreHour: 10,
    totalMemoryGibHour: 20,
    totalExecutorMemoryGibHour: 16,
    totalDCU: 0.6,
    idleCoresRate: 50,
    maxExecutorMemoryPercentage: 25,
  },
} as unknown as StatusStore;

// query 1 holds half of the app DCU -> 0.3 DCU
const sqlStore = {
  sqls: [{ id: "1", resourceMetrics: { dcuPercentage: 50 } }],
} as unknown as SparkSQLStore;

function alert(name: string, source: Alert["source"]): Alert {
  return {
    id: name,
    name,
    title: name,
    message: "",
    location: "",
    suggestion: "",
    type: "warning",
    source,
  };
}

const sqlSource = { type: "sql", sqlId: "1", sqlNodeId: 0 } as const;

describe("reduceSavingsOpportunity", () => {
  it("fixes queries first, then right-sizes idle cores and memory on what is left", () => {
    const alerts = [
      alert("idleCoresTooHigh", { type: "status", metric: "idleCores" }),
      alert("partitionSkew", sqlSource),
      alert("readSmallFiles", sqlSource),
      alert("executorMemoryTooLow", { type: "status", metric: "memory" }),
    ];

    const savings = reduceSavingsOpportunity(sqlStore, statusStore, alerts);

    // query: 0.3 * (1 - 0.4 * 0.9) = 0.192, of which skew 0.18 is reclaimed idle cores
    // idle: (0.25 - 0.18) * (1 - 0.012 / 0.6) = 0.0686
    // memory: 16 * 0.005 * 0.75 * 0.5 * (1 - 0.192 / 0.6) = 0.0204
    expect(savings?.queriesPercentage).toBeCloseTo(32, 1);
    expect(savings?.idleCoresPercentage).toBeCloseTo(11.43, 1);
    expect(savings?.memoryPercentage).toBeCloseTo(3.4, 1);
    expect(savings?.totalPercentage).toBeCloseTo(46.83, 1);

    expect(alerts.map((a) => a.savingsPercentage?.toFixed(1))).toEqual([
      "11.4",
      "30.0",
      "5.0",
      "3.4",
    ]);
  });

  it("still reports idle cores when there are no alerts", () => {
    const savings = reduceSavingsOpportunity(sqlStore, statusStore, []);
    expect(savings?.totalPercentage).toBeCloseTo(41.67, 1);
  });

  it("leaves alerts it can't price without an estimate", () => {
    const alerts = [
      alert("driverMemoryTooHigh", { type: "status", metric: "driverMemory" }),
    ];
    reduceSavingsOpportunity(sqlStore, statusStore, alerts);
    expect(alerts[0].savingsPercentage).toBeUndefined();
  });

  it("returns undefined before executor status is available", () => {
    expect(
      reduceSavingsOpportunity(
        sqlStore,
        { executors: undefined } as unknown as StatusStore,
        [],
      ),
    ).toBeUndefined();
  });
});
