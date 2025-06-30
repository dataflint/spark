import { format, parse } from "bytes";
import { Duration, duration } from "moment";

export function humanFileSize(bytes: number): string {
  if (Number.isNaN(bytes)) return "NaN";
  const formatted = format(bytes, { unitSeparator: " " });
  return formatted
    .replace("KB", "KiB")
    .replace("MB", "MiB")
    .replace("GB", "GiB")
    .replace("TB", "TiB");
}

export function capitalizeWords(text: string): string {
  return text
    .split(' ')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
}

export function parseBytesString(str: string): number {
  return parse(
    str
      .replace("KiB", "KB")
      .replace("MiB", "MB")
      .replace("GiB", "GB")
      .replace("TiB", "TB"),
  );
}

export function humanFileSizeSparkConfigFormat(bytes: number): string {
  if (Number.isNaN(bytes)) return "NaN";
  const formatted = format(bytes);
  return formatted
    .replace("KB", "k")
    .replace("MB", "m")
    .replace("GB", "g")
    .replace("TB", "t");
}

export function humanizeTimeDiff(
  duration: Duration,
  roundSeconds: boolean = false,
): string {
  if (duration.asDays() >= 1) {
    return duration.asDays().toFixed(1) + "d";
  }
  if (duration.asHours() >= 1) {
    return duration.asHours().toFixed(1) + "h";
  }
  if (duration.asMinutes() >= 1) {
    return duration.asMinutes().toFixed(1) + "m";
  }
  if (duration.asSeconds() >= 1 || roundSeconds) {
    return roundSeconds
      ? duration.asSeconds().toFixed(0) + "s"
      : duration.asSeconds().toFixed(1) + "s";
  }
  return duration.asMilliseconds().toFixed(0) + "ms";
}

export function msToHours(ms: number): number {
  return ms / 1000 / 60 / 60;
}

export function hoursToMS(ms: number): number {
  return ms * 1000 * 60 * 60;
}

export function timeStrToEpocTime(time: string): number {
  const addTimeMoment = new Date(time.replace("GMT", "Z"));
  return addTimeMoment.getTime();
}

export function timeStringToMilliseconds(
  timeString: string | undefined,
): number | undefined {
  if (timeString === undefined) {
    return undefined;
  }
  const unit = timeString.slice(-2).trim();
  const value = parseFloat(timeString.slice(0, -2).trim());

  switch (unit) {
    case "ms":
      return value;
    case "s":
      return duration(value, "seconds").asMilliseconds();
    case "m":
      return duration(value, "minutes").asMilliseconds();
    case "h":
      return duration(value, "hours").asMilliseconds();
    default:
      throw new Error(`Unsupported time unit: ${unit}`);
  }
}

export function calculatePercentage(value: number, total: number): number {
  if (total === undefined || value === undefined || total === 0) {
    return 0;
  }
  const percentage = (value / total) * 100;
  return Math.min(Math.max(percentage, 0), 100);
}
