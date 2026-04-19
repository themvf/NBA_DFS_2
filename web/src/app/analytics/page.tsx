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

  const calibration = await safeSection(() => AnalyticsContent({ sport }));

  if (sport === "mlb") {
    const healthCard = await safeSection(() => MlbHealthCard());
    const signalCard = await safeSection(() => MlbSignalCard());
    const postmortem = await safeSection(() => MlbPostmortemPanel());
    const ownership = await safeSection(() => MlbOwnershipModelPanel({ selectedSlateId: ownershipSlateId, sortBy: ownershipSort }));
    const blowup = await safeSection(() => MlbBlowupCandidatePanel());
    const runEnvironment = await safeSection(() => MlbRunEnvironmentPanel());
    const pitcherLineup = await safeSection(() => MlbPitcherLineupPanel());
    const perfectLineup = await safeSection(() => PerfectLineupPanel({ sport }));

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

  const perfectLineup = await safeSection(() => PerfectLineupPanel({ sport }));

  return (
    <>
      {calibration}
      {perfectLineup}
    </>
  );
}
