import { MixpanelEvents } from "../interfaces/Mixpanel";
import { MixpanelService } from "../services/MixpanelService";
import { isDataFlintSaaSUI } from "./UrlUtils";

const BOOK_DEMO_URL = "https://www.dataflint.io/book-demo";

export const SAVINGS_COLOR = "#2e7d32";
export const SAVINGS_TEXT_COLOR = "#66bb6a";

// shared copy of the savings CTAs: lead with the outcome, MCP as the "how", and "see it live" rather than
// "book a demo", which reads as a sales call to engineers
export const SAVINGS_CTA_LABEL = "Get the code fix · See it live";
export const SAVINGS_AGENT_PITCH =
  "DataFlint's agent finds the code changes and opens the PR, right from your IDE via MCP.";

// shared look of the savings CTAs, so the tile, sidebar card and alerts read as one feature
export const SAVINGS_BUTTON_SX = {
  textTransform: "none",
  fontWeight: 600,
  color: "#fff",
  backgroundColor: SAVINGS_COLOR,
  "&:hover": { backgroundColor: "#1b5e20" },
} as const;

export const SAVINGS_CHIP_SX = {
  color: SAVINGS_TEXT_COLOR,
  borderColor: SAVINGS_COLOR,
  backgroundColor: "rgba(46,125,50,0.12)",
  fontWeight: 600,
  "& .MuiChip-icon": { color: SAVINGS_TEXT_COLOR },
} as const;

// the SaaS UI shows the engine's own opportunity figures, so the OSS savings CTA is OSS-only
export const shouldShowSavingsCta = (): boolean => !isDataFlintSaaSUI();

export const formatSavingsPercentage = (percentage: number): string =>
  percentage >= 10 ? percentage.toFixed(0) : percentage.toFixed(1);

export function openBookDemo(
  placement: string,
  savingsPercentage: number,
): void {
  const roundedSavings = Math.round(savingsPercentage);
  MixpanelService.Track(MixpanelEvents.SavingsBookDemoClicked, {
    placement,
    savingsPercentage: roundedSavings,
  });
  const params = new URLSearchParams({
    utm_source: "dataflint-oss",
    utm_medium: "spark-ui",
    utm_campaign: "savings-opportunity",
    utm_content: placement,
    utm_term: `savings-${roundedSavings}`,
  });
  window.open(`${BOOK_DEMO_URL}?${params.toString()}`, "_blank", "noopener");
}
