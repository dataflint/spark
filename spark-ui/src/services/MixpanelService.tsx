import mixpanel from "mixpanel-browser";
import { MixpanelEvents } from "../interfaces/Mixpanel";


export class MixpanelService {
    static InitMixpanel(): void {
        if (!this.ShouldTrack())
            return;

        const MIX_PANEL_TOKEN = process.env.NODE_ENV === 'development' ? '4b4ba202495eacfd7b46b1147d27f930' : '5251dfa36e60af653d1c6380ccf97857';

        // For debugging add debug: true to the props
        mixpanel.init(MIX_PANEL_TOKEN, { track_pageview: true, persistence: 'localStorage' });
    }

    static Track(event: MixpanelEvents, properties?: { [key: string]: any }): void {
        if (!this.ShouldTrack())
            return;

        mixpanel.track(event, properties);
    }

    static TrackPageView(properties?: { [key: string]: any }): void {
        if (!this.ShouldTrack())
            return;

        mixpanel.track_pageview(properties);
    }

    static ShouldTrack(): boolean {
        // For tracking in dev mode set the following env var - 'ENABLE_MIXPANEL_IN_DEV = true'
        return process.env.NODE_ENV !== 'development' || process.env.ENABLE_MIXPANEL_IN_DEV === 'true';
    }
}