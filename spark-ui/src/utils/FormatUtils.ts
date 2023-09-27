import { Duration } from 'moment'

export function humanFileSize(bytes: number, si = true): string {
    let thresh = si ? 1000 : 1024;
    if (bytes < thresh) return bytes + ' B';
    let units = si ? ['kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'] : ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
    let u = -1;
    do {
      bytes /= thresh;
      ++u;
    } while (bytes >= thresh);
    return bytes.toFixed(1) + ' ' + units[u];
  };

export function humanizeTimeDiff(duration: Duration): string {
  if(duration.asDays() >= 1) {
    return duration.asDays().toFixed(1) + "d"
  }
  if(duration.asHours() >= 1) {
    return duration.asHours().toFixed(1) + "h"
  }
  if(duration.asMinutes() >= 1) {
    return duration.asMinutes().toFixed(1) + "m"
  }
  return duration.asSeconds().toFixed(1) + "s"
}

export function msToHours(ms: number): number {
  return ms / 1000 / 60 / 60;
}

export function timeStrToEpocTime(time: string): number {
  const addTimeMoment = new Date(time.replace("GMT", "Z"));
  return addTimeMoment.getTime();
}
