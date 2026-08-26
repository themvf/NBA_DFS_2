"use client";

import { useMemo, useState } from "react";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import { formatPriorSeasonFantasyPoints } from "@/lib/fantasy-football/prior-season-finish";
import ProjectionNotation from "../rankings/projection-notation";
import InjuryMarker from "@/components/fantasy-football/injury-marker";

const POSITION_GROUPS = [
  { position: "QB", label: "Quarterbacks" },
  { position: "RB", label: "Running Backs" },
  { position: "WR", label: "Wide Receivers" },
  { position: "TE", label: "Tight Ends" },
  { position: "K", label: "Kickers" },
  { position: "DST", label: "Defense / Special Teams" },
] as const;

function number(value: number | null, digits = 1): string {
  return value === null ? "—" : value.toFixed(digits);
}

function PlayerSignals({ player }: { player: FantasyRankingRow }) {
  const hasInjuryMarker = Boolean(player.injuryStatus || player.injuryDetails);
  const visibleIndicators = player.indicators.filter((badge) => badge.code !== "INJURY");
  return <div className="flex flex-wrap gap-1">
    <InjuryMarker injuryStatus={player.injuryStatus} details={player.injuryDetails ?? null} />
    {visibleIndicators.slice(0, hasInjuryMarker ? 2 : 3).map((badge) => <span key={badge.code} title={JSON.stringify(badge.evidence)} className={`rounded-full px-2 py-1 text-[10px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}
  </div>;
}

function PositionTable({
  label,
  players,
  season,
  scoring,
}: {
  label: string;
  players: FantasyRankingRow[];
  season: number;
  scoring: string;
}) {
  return <section className="overflow-hidden rounded-2xl border bg-card shadow-sm">
    <div className="flex items-center justify-between border-b bg-muted/60 px-4 py-3">
      <h2 className="text-lg font-black">{label}</h2>
      <span className="rounded-full bg-background px-2.5 py-1 text-xs font-bold text-muted-foreground">{players.length} player{players.length === 1 ? "" : "s"}</span>
    </div>
    {players.length === 0 ? <p className="p-5 text-sm text-muted-foreground">No players from this position are in the current ranking snapshot.</p> : <div className="overflow-x-auto">
      <table className="w-full min-w-[1180px] text-sm">
        <thead className="bg-muted/35 text-left text-[11px] uppercase tracking-wide text-muted-foreground"><tr>
          <th className="p-3">Our rank</th><th className="p-3">Player</th><th className="p-3">Role & signals</th>
          <th className="p-3">Our {season} {scoring}</th><th className="p-3">FantasyPros</th><th className="p-3">{season - 1} FPTS</th>
          <th className="p-3">ADP</th><th className="p-3">{season - 1} GP</th><th className="p-3">{season} GP</th><th className="p-3">Confidence</th>
        </tr></thead>
        <tbody>
          {players.map((player) => <tr key={player.playerId} className="border-t align-top hover:bg-muted/35">
            <td className="p-3 text-lg font-black">{player.ourRank ?? player.ecr ?? "—"}<span className="block text-xs font-semibold text-muted-foreground">{player.position}{player.positionRank ?? ""} · T{player.tier ?? "—"}</span></td>
            <td className="p-3"><p className="font-bold">{player.name}</p><p className="text-xs text-muted-foreground">Bye {player.byeWeek ?? "—"}</p></td>
            <td className="max-w-[300px] p-3"><PlayerSignals player={player} /></td>
            <td className="p-3 font-black">{number(player.ourProjectedPoints)}<ProjectionNotation details={player.projectionDetails} label="Projection details" /></td>
            <td className="p-3 font-semibold">{number(player.fantasyProsProjectedPoints)}</td>
            <td className="p-3 font-semibold">{formatPriorSeasonFantasyPoints(player.fantasyPoints2025, player.positionFinish2025, player.positionFinishTieCount2025)}</td>
            <td className="p-3">{number(player.adp)}{player.dkBestBallAdp !== null && <span className="block text-xs font-semibold text-blue-700">DK {player.dkBestBallAdp.toFixed(1)}</span>}</td>
            <td className="p-3">{player.games2025 ?? "—"}</td>
            <td className="p-3">{number(player.expectedGames)}</td>
            <td className="p-3">{player.confidence === null ? "—" : `${Math.round(player.confidence * 100)}%`}</td>
          </tr>)}
        </tbody>
      </table>
    </div>}
  </section>;
}

export default function NflTeamOverview({
  rankings,
  season,
  scoring,
}: {
  rankings: FantasyRankingRow[];
  season: number;
  scoring: string;
}) {
  const teams = useMemo(
    () => [...new Set(rankings.flatMap((player) => player.team ? [player.team] : []))].sort(),
    [rankings],
  );
  const [selectedTeam, setSelectedTeam] = useState(teams[0] ?? "");
  const teamPlayers = useMemo(
    () => rankings.filter((player) => player.team === selectedTeam),
    [rankings, selectedTeam],
  );


  return <div className="space-y-5">
    <section className="rounded-2xl border bg-card p-4 shadow-sm">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <label className="w-full max-w-xs space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">
          Choose an NFL team
          <select
            value={selectedTeam}
            onChange={(event) => setSelectedTeam(event.target.value)}
            disabled={teams.length === 0}
            className="block w-full rounded-lg border bg-background px-3 py-2.5 text-sm font-semibold normal-case tracking-normal text-foreground disabled:cursor-not-allowed disabled:opacity-50"
          >
            {teams.length === 0 && <option value="">No teams available</option>}
            {teams.map((team) => <option key={team} value={team}>{team}</option>)}
          </select>
        </label>
        <div className="text-left sm:text-right" aria-live="polite">
          <p className="text-2xl font-black">{selectedTeam || "NFL roster"}</p>
          <p className="text-sm text-muted-foreground">
            {teamPlayers.length} ranked player{teamPlayers.length === 1 ? "" : "s"} across {POSITION_GROUPS.length} position groups
          </p>
        </div>
      </div>
    </section>

    {teams.length === 0 ? (
      <div className="rounded-2xl border border-dashed bg-card p-8 text-center text-sm text-muted-foreground">
        No team-assigned players are available in the current ranking snapshot.
      </div>
    ) : (
      <div className="space-y-5">
        {POSITION_GROUPS.map(({ position, label }) => (
          <PositionTable
            key={position}
            label={label}
            players={teamPlayers.filter((player) => player.position === position)}
            season={season}
            scoring={scoring}
          />
        ))}
      </div>
    )}
  </div>;
}
