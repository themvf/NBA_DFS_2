export const dynamic = "force-dynamic";
export const maxDuration = 60;

import type { ReactNode } from "react";
import type { OwnershipDetailSort, Sport } from "@/db/queries";
import AnalyticsContent from "./analytics-content";
import MlbBlowupCandidatePanel from "./mlb-blowup-candidate-panel";
import MlbHealthCard from "./mlb-health-card";
import MlbOwnershipModelPanel from "./mlb-ownership-model-panel";
import MlbPitcherLineupPanel from "./mlb-pitcher-lineup-panel";
import MlbPostmortemPanel from "./mlb-postmortem-panel";
import MlbRunEnvironmentPanel from "./mlb-run-environment-panel";
import MlbSignalCard from "./mlb-signal-card";
import PerfectLineupPanel from "./perfect-lineup-panel";

async function safeSection(render: () => Promise<ReactNode>): Promise<ReactNode | null> {
  try {
    return await render();
  } catch (error) {
    console.error("Analytics section failed", error);
    return null;
  }
}

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string; ownershipSlate?: string; ownershipSort?: string }>;
}) {
  const { sport: rawSport, ownershipSlate: rawOwnershipSlate, ownershipSort: rawOwnershipSort } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";
  const ownershipSlateId = rawOwnershipSlate && /^\d+$/.test(rawOwnershipSlate) ? Number(rawOwnershipSlate) : null;
  const ownershipSort: OwnershipDetailSort = rawOwnershipSort === "gain"
    || rawOwnershipSort === "actual"
    || rawOwnershipSort === "field-own"
    || rawOwnershipSort === "field-error"
    ? rawOwnershipSort
    : "field-error";

  // Run all sections in parallel — page.tsx has no inter-section dependencies.
  if (sport === "mlb") {
    const [
      calibration,
      healthCard,
      signalCard,
      postmortem,
      ownership,
      blowup,
      runEnvironment,
      pitcherLineup,
      perfectLineup,
    ] = await Promise.all([
      safeSection(() => AnalyticsContent({ sport })),
      safeSection(() => MlbHealthCard()),
      safeSection(() => MlbSignalCard()),
      safeSection(() => MlbPostmortemPanel()),
      safeSection(() => MlbOwnershipModelPanel({ selectedSlateId: ownershipSlateId, sortBy: ownershipSort })),
      safeSection(() => MlbBlowupCandidatePanel()),
      safeSection(() => MlbRunEnvironmentPanel()),
      safeSection(() => MlbPitcherLineupPanel()),
      safeSection(() => PerfectLineupPanel({ sport })),
    ]);

    return (
      <>
        {calibration}
        {healthCard}
        {signalCard}
        {postmortem}
        {ownership}
        {blowup}
        {runEnvironment}
        {pitcherLineup}
        {perfectLineup}
      </>
    );
  }

  const [calibration, perfectLineup] = await Promise.all([
    safeSection(() => AnalyticsContent({ sport })),
    safeSection(() => PerfectLineupPanel({ sport })),
  ]);

  return (
    <>
      {calibration}
      {perfectLineup}
    </>
  );
}
