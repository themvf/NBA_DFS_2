export const dynamic = "force-dynamic";
export const maxDuration = 60;

import { Suspense } from "react";
import type { OwnershipDetailSort, Sport } from "@/db/queries";
import AnalyticsContent from "./analytics-content";
import MlbOwnershipModelPanel from "./mlb-ownership-model-panel";
import MlbPitcherLineupPanel from "./mlb-pitcher-lineup-panel";
import MlbRunEnvironmentPanel from "./mlb-run-environment-panel";
import PerfectLineupPanel from "./perfect-lineup-panel";

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

  return (
    <>
      <Suspense
        fallback={
          <div className="mx-auto max-w-5xl space-y-8 p-6 text-slate-900">
            <div>
              <h1 className="text-xl font-bold">Model Calibration Analytics</h1>
              <p className="mt-1 text-sm text-slate-700">
                Loading {sport.toUpperCase()} analytics...
              </p>
            </div>
            <div className="rounded-lg border bg-card p-6 text-sm text-slate-700">
              Loading accuracy trends, position breakdowns, and leverage calibration...
            </div>
          </div>
        }
      >
        <AnalyticsContent sport={sport} />
      </Suspense>
      {sport === "mlb" ? (
        <Suspense
          fallback={
            <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
              <h2 className="mb-1 text-sm font-semibold">MLB Ownership Model Tracking</h2>
              <p className="text-xs text-slate-700">
                Loading ownership tracking report...
              </p>
            </div>
          }
        >
          <MlbOwnershipModelPanel selectedSlateId={ownershipSlateId} sortBy={ownershipSort} />
        </Suspense>
      ) : null}
      {sport === "mlb" ? (
        <Suspense
          fallback={
            <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
              <h2 className="mb-1 text-sm font-semibold">MLB Pitcher And Park Environment</h2>
              <p className="text-xs text-slate-700">
                Loading pitcher and park environment report...
              </p>
            </div>
          }
        >
          <MlbRunEnvironmentPanel />
        </Suspense>
      ) : null}
      {sport === "mlb" ? (
        <Suspense
          fallback={
            <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
              <h2 className="mb-1 text-sm font-semibold">MLB Pitcher Lineup Signals</h2>
              <p className="text-xs text-slate-700">
                Loading MLB pitcher cohort report...
              </p>
            </div>
          }
        >
          <MlbPitcherLineupPanel />
        </Suspense>
      ) : null}
      <Suspense
        fallback={
          <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
            <h2 className="mb-1 text-sm font-semibold">Perfect Lineup Structure</h2>
            <p className="text-xs text-slate-700">
              Loading {sport.toUpperCase()} perfect-lineup analytics...
            </p>
          </div>
        }
      >
        <PerfectLineupPanel sport={sport} />
      </Suspense>
    </>
  );
}
