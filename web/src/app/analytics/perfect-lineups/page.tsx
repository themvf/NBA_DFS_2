export const dynamic = "force-dynamic";
export const maxDuration = 60;

import { Suspense } from "react";
import type { Sport } from "@/db/queries";
import AnalyticsPageHeader from "../analytics-page-header";
import AnalyticsSubnav from "../analytics-subnav";
import {
  AnalyticsSection,
  PERFECT_LINEUP_TIMEOUT_MS,
  SectionFallback,
} from "../analytics-sections";
import PerfectLineupPanel from "../perfect-lineup-panel";

export default async function AnalyticsPerfectLineupsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport: rawSport } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

  return (
    <>
      <AnalyticsPageHeader
        title={sport === "mlb" ? "MLB Perfect Lineups" : "NBA Perfect Lineups"}
        description={
          sport === "mlb"
            ? "Historical optimal lineup construction by slate size, stack shape, salary left, and opponent context."
            : "Historical optimal lineup construction by slate size, shape frequency, and team concentration."
        }
      />
      <AnalyticsSubnav sport={sport} active="perfect-lineups" />
      <Suspense fallback={<SectionFallback label="PerfectLineup" />}>
        <AnalyticsSection
          label="PerfectLineup"
          render={() => PerfectLineupPanel({ sport })}
          timeoutMs={PERFECT_LINEUP_TIMEOUT_MS}
        />
      </Suspense>
    </>
  );
}
