export const dynamic = "force-dynamic";

import { Suspense } from "react";
import VegasContent from "./vegas-content";
import type { Sport } from "@/db/queries";

export default async function VegasPage({
  searchParams,
}: {
  searchParams: Promise<{ date?: string; sport?: string }>;
}) {
  const { date, sport } = await searchParams;
  const resolvedSport: Sport = sport === "mlb" ? "mlb" : "nba";

  return (
    <Suspense
      fallback={
        <div className="space-y-6 p-6 max-w-5xl mx-auto">
          <h1 className="text-xl font-bold">Vegas Analysis — {resolvedSport.toUpperCase()}</h1>
          <div className="rounded-lg border bg-card p-6 text-sm text-gray-400">
            Loading matchups and historical Vegas data…
          </div>
        </div>
      }
    >
      <VegasContent date={date} sport={resolvedSport} />
    </Suspense>
  );
}
