export const dynamic = "force-dynamic";
export const maxDuration = 60;

import { Suspense } from "react";
import type { Sport } from "@/db/queries";
import AnalyticsContent from "./analytics-content";
import MlbOwnershipModelPanel from "./mlb-ownership-model-panel";
import MlbPitcherLineupPanel from "./mlb-pitcher-lineup-panel";
import PerfectLineupPanel from "./perfect-lineup-panel";

export default async function AnalyticsPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string }>;
}) {
  const { sport: rawSport } = await searchParams;
  const sport: Sport = rawSport === "mlb" ? "mlb" : "nba";

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
          <MlbOwnershipModelPanel />
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
