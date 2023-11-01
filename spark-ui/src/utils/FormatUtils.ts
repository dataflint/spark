import { format } from "bytes";
import { Duration } from "moment";

export function humanFileSize(bytes: number): string {
  if (Number.isNaN(bytes)) return "NaN";
  const formatted = format(bytes, { unitSeparator: " " });
  return formatted.replace("KB", "KiB").replace("MB", "MiB").replace("GB", "GiB").replace("TB", "TiB");
}

export function humanizeTimeDiff(duration: Duration, roundSeconds: boolean = false): string {
  if (duration.asDays() >= 1) {
    return duration.asDays().toFixed(1) + "d";
  }
  if (duration.asHours() >= 1) {
    return duration.asHours().toFixed(1) + "h";
  }
  if (duration.asMinutes() >= 1) {
    return duration.asMinutes().toFixed(1) + "m";
  }
  return roundSeconds ? duration.asSeconds().toFixed(0) + "s" : duration.asSeconds().toFixed(1) + "s";
}

export function msToHours(ms: number): number {
  return ms / 1000 / 60 / 60;
}

export function timeStrToEpocTime(time: string): number {
  const addTimeMoment = new Date(time.replace("GMT", "Z"));
  return addTimeMoment.getTime();
}
