// Utility to parse the spark.dataflint.alert.disabled config
export function parseAlertDisabledConfig(config: string | undefined): Set<string> {
  if (!config) return new Set();
  return new Set(config.split(',').map(x => x.trim()).filter(Boolean));
}
