import { Suspense } from "react";
import {
  getSoccerEloRankings,
  getSoccerCompletedResults,
  getSoccerFuturesBets,
  getSoccerGroupStandings,
  getSoccerGroupFixtures,
} from "@/db/queries";
import EloClient from "./elo-client";

export const dynamic = "force-dynamic";
export const revalidate = 0;

async function EloContent() {
  const [teams, results, futures, standings, fixtures] = await Promise.all([
    getSoccerEloRankings(),
    getSoccerCompletedResults(),
    getSoccerFuturesBets(),
    getSoccerGroupStandings(),
    getSoccerGroupFixtures(),
  ]);
  return <EloClient teams={teams} results={results} futures={futures} standings={standings} fixtures={fixtures} />;
}

export default function EloPage() {
  return (
    <Suspense
      fallback={
        <div className="mx-auto max-w-6xl p-6 text-muted-foreground text-sm">
          Loading power rankings…
        </div>
      }
    >
      <EloContent />
    </Suspense>
  );
}
