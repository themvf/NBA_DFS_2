export const dynamic = "force-dynamic";
export const maxDuration = 60;

import { Suspense } from "react";
import { redirect } from "next/navigation";
import type { OwnershipDetailSort } from "@/db/queries";
import AnalyticsPageHeader from "../analytics-page-header";
import AnalyticsSubnav from "../analytics-subnav";
import { AnalyticsSection, SectionFallback } from "../analytics-sections";
import MlbOwnershipModelPanel from "../mlb-ownership-model-panel";

export default async function AnalyticsOwnershipPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string; ownershipSlate?: string; ownershipSort?: string }>;
}) {
  const {
    sport: rawSport,
    ownershipSlate: rawOwnershipSlate,
    ownershipSort: rawOwnershipSort,
  } = await searchParams;

  if (rawSport === "nba") {
    redirect("/analytics?sport=nba");
  }

  const ownershipSlateId = rawOwnershipSlate && /^\d+$/.test(rawOwnershipSlate) ? Number(rawOwnershipSlate) : null;
  const ownershipSort: OwnershipDetailSort = rawOwnershipSort === "gain"
    || rawOwnershipSort === "actual"
    || rawOwnershipSort === "field-own"
    || rawOwnershipSort === "field-error"
    ? rawOwnershipSort
    : "field-error";

  return (
    <>
      <AnalyticsPageHeader
        title="MLB Ownership Tracking"
        description="Field model vs LineStar, with slate detail, bucket bias, and biggest ownership misses."
      />
      <AnalyticsSubnav sport="mlb" active="ownership" />
      <Suspense fallback={<SectionFallback label="Ownership" />}>
        <AnalyticsSection
          label="Ownership"
          render={() => (
            MlbOwnershipModelPanel({
              selectedSlateId: ownershipSlateId,
              sortBy: ownershipSort,
              basePath: "/analytics/ownership",
            })
          )}
        />
      </Suspense>
    </>
  );
}
