export interface SparkConfiguration {
    runtime: Runtime
    sparkProperties: string[][]
    hadoopProperties: string[][]
    systemProperties: string[][]
    metricsProperties: string[][]
    classpathEntries: string[][]
    resourceProfiles: ResourceProfile[]
  }
  
  export interface Runtime {
    javaVersion: string
    javaHome: string
    scalaVersion: string
  }
  
  export interface ResourceProfile {
    id: number
    executorResources: ExecutorResources
    taskResources: TaskResources
  }
  
  export interface ExecutorResources {
    cores: Cores
    memory: Memory
    offHeap: OffHeap
  }
  
  export interface Cores {
    resourceName: string
    amount: number
    discoveryScript: string
    vendor: string
  }
  
  export interface Memory {
    resourceName: string
    amount: number
    discoveryScript: string
    vendor: string
  }
  
  export interface OffHeap {
    resourceName: string
    amount: number
    discoveryScript: string
    vendor: string
  }
  
  export interface TaskResources {
    cpus: Cpus
  }
  
  export interface Cpus {
    resourceName: string
    amount: number
  }
  