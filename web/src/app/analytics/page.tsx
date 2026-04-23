export const dynamic = "force-dynamic";
export const maxDuration = 60;

import { Suspense } from "react";
import { redirect } from "next/navigation";
import type { Sport } from "@/db/queries";
import AnalyticsContent from "./analytics-content";
import AnalyticsEntryCards from "./analytics-entry-cards";
import AnalyticsSubnav from "./analytics-subnav";
import { AnalyticsSection, SectionFallback } from "./analytics-sections";
import MlbBlowupCandidatePanel from "./mlb-blowup-candidate-panel";
import MlbHealthCard from "./mlb-health-card";
import MlbPitcherLineupPanel from "./mlb-pitcher-lineup-panel";
import MlbRunEnvironmentPanel from "./mlb-run-environment-panel";
import MlbSignalCard from "./mlb-signal-card";

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string; ownershipSlate?: string; ownershipSort?: string }>;
}) {
  try {
    const { sport: rawSport, ownershipSlate, ownershipSort } = await searchParams;
    const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

    if (sport === "mlb" && (ownershipSlate || ownershipSort)) {
      const params = new URLSearchParams({ sport: "mlb" });
      if (ownershipSlate) params.set("ownershipSlate", ownershipSlate);
      if (ownershipSort) params.set("ownershipSort", ownershipSort);
      redirect(`/analytics/ownership?${params.toString()}`);
    }

    if (sport === "mlb") {
      return (
        <>
          <AnalyticsEntryCards sport={sport} />
          <AnalyticsSubnav sport={sport} active="overview" />

          <Suspense fallback={<SectionFallback label="Calibration" />}>
            <AnalyticsSection label="Calibration" render={() => AnalyticsContent({ sport, showHeader: false })} />
          </Suspense>
          <Suspense fallback={<SectionFallback label="HealthCard" />}>
            <AnalyticsSection label="HealthCard" render={() => MlbHealthCard()} />
          </Suspense>
          <Suspense fallback={<SectionFallback label="SignalCard" />}>
            <AnalyticsSection label="SignalCard" render={() => MlbSignalCard()} />
          </Suspense>
          <Suspense fallback={<SectionFallback label="Blowup" />}>
            <AnalyticsSection label="Blowup" render={() => MlbBlowupCandidatePanel()} />
          </Suspense>
          <Suspense fallback={<SectionFallback label="RunEnvironment" />}>
            <AnalyticsSection label="RunEnvironment" render={() => MlbRunEnvironmentPanel()} />
          </Suspense>
          <Suspense fallback={<SectionFallback label="PitcherLineup" />}>
            <AnalyticsSection label="PitcherLineup" render={() => MlbPitcherLineupPanel()} />
          </Suspense>
        </>
      );
    }

    return (
      <>
        <AnalyticsEntryCards sport={sport} />
        <AnalyticsSubnav sport={sport} active="overview" />
        <Suspense fallback={<SectionFallback label="Calibration" />}>
          <AnalyticsSection label="Calibration" render={() => AnalyticsContent({ sport, showHeader: false })} />
        </Suspense>
      </>
    );
  } catch (fatal) {
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
