import { AppDispatch } from "../Store";
import { setTelemetryEnabled } from "../reducers/GeneralSlice";

export class ScarfPixelService {
    static scarfPixelTelemetryConfigDisabled = false;
    static dispatch: AppDispatch | null = null;

    static setDispatch(dispatch: AppDispatch): void {
        ScarfPixelService.dispatch = dispatch;
    }

    static setScarfPixelTelemetryConfigDisabled(): void {
        ScarfPixelService.scarfPixelTelemetryConfigDisabled = true;
    }

    static InitScarfPixel(): void {
        if (!this.ShouldTrack()) return;

        // Enable telemetry in Redux state
        if (ScarfPixelService.dispatch) {
            ScarfPixelService.dispatch(setTelemetryEnabled({ enabled: true }));
        }
    }

    static ShouldTrack(): boolean {
        return (
            process.env.NODE_ENV !== "development" &&
            localStorage.getItem("SKIP_MIXPANEL") !== "true" &&
            !ScarfPixelService.scarfPixelTelemetryConfigDisabled
        );
    }
}

