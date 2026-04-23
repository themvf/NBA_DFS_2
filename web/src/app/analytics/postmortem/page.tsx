export const dynamic = "force-dynamic";
export const maxDuration = 60;

import { Suspense } from "react";
import { redirect } from "next/navigation";
import AnalyticsPageHeader from "../analytics-page-header";
import AnalyticsSubnav from "../analytics-subnav";
import { AnalyticsSection, SectionFallback } from "../analytics-sections";
import MlbPostmortemPanel from "../mlb-postmortem-panel";

export default async function AnalyticsPostmortemPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport } = await searchParams;
  if (sport === "nba") {
    redirect("/analytics?sport=nba");
  }

  return (
    <>
      <AnalyticsPageHeader
        title="MLB Postmortem"
        description="Projection independence, ownership gaps, signal follow-through, and latest model misses."
      />
      <AnalyticsSubnav sport="mlb" active="postmortem" />
      <Suspense fallback={<SectionFallback label="Postmortem" />}>
        <AnalyticsSection label="Postmortem" render={() => MlbPostmortemPanel()} />
      </Suspense>
    </>
  );
}
