import * as React from "react";
import { useAppSelector } from "../Hooks";

/**
 * Scarf tracking pixel component that respects telemetry settings.
 * Only renders when telemetry is explicitly enabled (after Spark config is loaded).
 */
export const ScarfPixel: React.FC = () => {
    const telemetryEnabled = useAppSelector((state) => state.general.telemetryEnabled);

    if (!telemetryEnabled) {
        return null;
    }

    return (
        <img
            referrerPolicy="unsafe-url"
            src="https://static.scarf.sh/a.png?x-pxid=785f95be-53f1-48f6-bedb-a36d2732ec8d"
            alt=""
            style={{ display: "none" }}
        />
    );
};

