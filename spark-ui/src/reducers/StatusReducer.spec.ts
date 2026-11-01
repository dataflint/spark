import { ConfigStore, SparkExecutorStore } from "../interfaces/AppStore";
import { calculateSparkExecutorsStatus } from "./StatusReducer";

const config = {
  executorContainerMemoryBytes: 0,
  driverMemoryBytes: 1024 * 1024 * 1024,
} as unknown as ConfigStore;

// local[8] driver that ran for 36s; Spark reports its totalDuration as its uptime, not its task time
const localDriver = {
  id: "driver",
  isActive: true,
  isDriver: true,
  duration: 36000,
  totalTaskDuration: 36000,
  totalCores: 8,
  maxTasks: 8,
  memoryUsagePercentage: 0,
  HeapMemoryUsageBytes: 0,
  totalInputBytes: 0,
  totalShuffleRead: 0,
  totalShuffleWrite: 0,
} as unknown as SparkExecutorStore;

describe("calculateSparkExecutorsStatus", () => {
  it("uses the stages task time for idle cores in local mode", () => {
    // 144s of tasks out of 36s * 8 cores = 50% busy
    const status = calculateSparkExecutorsStatus([localDriver], config, 144000);
    expect(status.idleCoresRate).toBeCloseTo(50, 5);
  });

  it("falls back to the driver task time before stages are loaded", () => {
    const status = calculateSparkExecutorsStatus([localDriver], config);
    expect(status.idleCoresRate).toBeCloseTo(87.5, 5);
  });
});
