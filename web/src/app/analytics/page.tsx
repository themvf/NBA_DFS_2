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

// Per-section timeout — keeps individual slow sections from killing the whole page.
// The LP-solver in PerfectLineupPanel can take 20–30 s on cache miss; cap it so
// the rest of the sections still render within the 60 s function budget.
const SECTION_TIMEOUT_MS = 25_000;
const PERFECT_LINEUP_TIMEOUT_MS = 30_000;

async function safeSection(
  label: string,
  render: () => Promise<ReactNode>,
  timeoutMs = SECTION_TIMEOUT_MS,
): Promise<ReactNode> {
  const start = Date.now();
  try {
    const result = await Promise.race([
      render(),
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error(`Timed out after ${timeoutMs / 1000}s`)),
          timeoutMs,
        )
      ),
    ]);
    console.log(`[analytics] ${label} OK ${Date.now() - start}ms`);
    return result ?? null;
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    const stack = error instanceof Error ? (error.stack ?? "") : "";
    console.error(`[analytics] ${label} FAILED ${Date.now() - start}ms — ${msg}\n${stack}`);
    // Render inline instead of propagating to error.tsx
    return (
      <div className="mx-auto mt-4 max-w-5xl rounded border border-amber-200 bg-amber-50 px-4 py-2 text-xs text-amber-700">
        <span className="font-semibold">{label}</span>
        {": "}
        <span className="font-mono">{msg}</span>
      </div>
    );
  }
}

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string; ownershipSlate?: string; ownershipSort?: string }>;
}) {
  try {
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
        safeSection("Calibration", () => AnalyticsContent({ sport })),
        safeSection("HealthCard", () => MlbHealthCard()),
        safeSection("SignalCard", () => MlbSignalCard()),
        safeSection("Postmortem", () => MlbPostmortemPanel()),
        safeSection("Ownership", () => MlbOwnershipModelPanel({ selectedSlateId: ownershipSlateId, sortBy: ownershipSort })),
        safeSection("Blowup", () => MlbBlowupCandidatePanel()),
        safeSection("RunEnvironment", () => MlbRunEnvironmentPanel()),
        safeSection("PitcherLineup", () => MlbPitcherLineupPanel()),
        safeSection("PerfectLineup", () => PerfectLineupPanel({ sport }), PERFECT_LINEUP_TIMEOUT_MS),
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
      safeSection("Calibration", () => AnalyticsContent({ sport })),
      safeSection("PerfectLineup", () => PerfectLineupPanel({ sport }), PERFECT_LINEUP_TIMEOUT_MS),
    ]);

    return (
      <>
        {calibration}
        {perfectLineup}
      </>
    );
  } catch (fatal) {
    // safeSection already catches section-level errors; this only fires if
    // something throws above the Promise.all (e.g. searchParams resolution).
    const msg = fatal instanceof Error ? fatal.message : String(fatal);
    console.error("[analytics] Page-level fatal error:", fatal);
    return (
      <div className="mx-auto mt-8 max-w-5xl space-y-4 p-6">
        <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
        <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-800">
          <p className="font-medium">Analytics failed to load</p>
          <p className="mt-1 font-mono text-xs">{msg}</p>
        </div>
      </div>
    );
  }
}
