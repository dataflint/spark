export function onlyUnique(value: string, index: number, array: string[]) {
  return array.indexOf(value) === index;
}

export function hashNumbersRemover(input: string): string {
  return input.replace(/#\d+L/g, "").replace(/#\d+/g, "");
}

export function truncateString(str: string, num: number): string {
  if (str.length <= num) {
    return str;
  }
  return str.slice(0, num) + "...";
}

export function truncateMiddle(str: string, maxLength: number): string {
  if (str.length <= maxLength) {
    return str;
  }

  const prefixLength = Math.ceil(maxLength / 2) - 1; // Subtract 1 for the '...'
  const suffixLength = Math.floor(maxLength / 2);

  const prefix = str.substring(0, prefixLength);
  const suffix = str.substring(str.length - suffixLength);

  return `${prefix}...${suffix}`;
}

export function removeFromStart(str: string, strToRemove: string): string {
  if (str.startsWith(strToRemove)) {
    return str.slice(strToRemove.length);
  }
  return str;
}

export function removeFromEnd(str: string, strToRemove: string) {
  if (str.endsWith(strToRemove)) {
    return str.slice(0, -strToRemove.length);
  }
  return str;
}

export function bracedSplit(input: string): string[] {
  const result: string[] = [];
  let buffer = "";
  let bracketCount = 0;
  let inQuotes = false;

  for (let i = 0; i < input.length; i++) {
    const char = input[i];

    if (char === "(") bracketCount++;
    if (char === ")") bracketCount--;
    if (char === '"') inQuotes = !inQuotes;

    if (char === "," && bracketCount === 0 && !inQuotes) {
      result.push(buffer.trim());
      buffer = "";
    } else {
      buffer += char;
    }
  }
  if (buffer) result.push(buffer.trim());
  return result;
}

export function bytesToHumanReadable(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = bytes;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(2)} ${units[unitIndex]}`;
}
