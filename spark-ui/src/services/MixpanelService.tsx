import mixpanel from "mixpanel-browser";
import { MixpanelEvents } from "../interfaces/Mixpanel";

const KEEP_ALIVE_INTERVAL_MS = 60 * 1000;

export class MixpanelService {
  static InitMixpanel(): void {
    if (!this.ShouldTrack()) return;

    const MIX_PANEL_TOKEN =
      process.env.NODE_ENV === "development"
        ? "114c37f7dc10c79978b850277136c232"
        : "114c37f7dc10c79978b850277136c232";

    // For debugging add debug: true to the props
    mixpanel.init(MIX_PANEL_TOKEN, {
      track_pageview: true,
      persistence: "localStorage",
    });
    this.StartKeepAlive(KEEP_ALIVE_INTERVAL_MS);
  }

  /**
   * Sends keep alive every interval if the tab is focused, in order to keep the mixpanel sessions "alive"
   * @param interval keep alive interval in ms
   */
  static StartKeepAlive(interval: number): void {
    if (!this.ShouldTrack) return;

    setInterval(() => {
      if (document.hidden) {
        // skip keep alive when tab is not in focus
        return;
      }

      this.Track(MixpanelEvents.KeepAlive);
    }, interval);
  }

  static Track(
    event: MixpanelEvents,
    properties?: { [key: string]: any },
  ): void {
    if (!this.ShouldTrack()) return;

    mixpanel.track(event, properties);
  }

  static TrackPageView(properties?: { [key: string]: any }): void {
    if (!this.ShouldTrack()) return;

    mixpanel.track_pageview(properties);
  }

  static ShouldTrack(): boolean {
    // For tracking in dev mode set the following env var - 'ENABLE_MIXPANEL_IN_DEV = true'
    return (
      process.env.NODE_ENV !== "development" ||
      process.env.ENABLE_MIXPANEL_IN_DEV === "true"
    );
  }
}
