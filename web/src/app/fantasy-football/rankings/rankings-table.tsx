"use client";

import { useMemo, useState } from "react";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import { filterFantasyRankings } from "@/lib/fantasy-football/ranking-filters";

const POSITIONS = ["QB", "RB", "WR", "TE", "K", "DST"];

export default function RankingsTable({ rankings }: { rankings: FantasyRankingRow[] }) {
  const [name, setName] = useState("");
  const [position, setPosition] = useState("");
  const [team, setTeam] = useState("");
  const teams = useMemo(
    () => [...new Set(rankings.flatMap((player) => player.team ? [player.team] : []))].sort(),
    [rankings],
  );
  const filtered = useMemo(
    () => filterFantasyRankings(rankings, { name, position, team }),
    [rankings, name, position, team],
  );
  const hasFilters = Boolean(name || position || team);

  return <div className="space-y-3">
    <div className="rounded-2xl border bg-card p-3">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-[minmax(220px,1fr)_180px_180px_auto] lg:items-end">
        <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">
          Name
          <input value={name} onChange={(event) => setName(event.target.value)} placeholder="Search player" className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground" />
        </label>
        <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">
          Position
          <select value={position} onChange={(event) => setPosition(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground">
            <option value="">All positions</option>
            {POSITIONS.map((value) => <option key={value} value={value}>{value}</option>)}
          </select>
        </label>
        <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">
          Team
          <select value={team} onChange={(event) => setTeam(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground">
            <option value="">All teams</option>
            {teams.map((value) => <option key={value} value={value}>{value}</option>)}
          </select>
        </label>
        <button type="button" disabled={!hasFilters} onClick={() => { setName(""); setPosition(""); setTeam(""); }} className="rounded-lg border px-3 py-2 text-sm font-semibold disabled:cursor-not-allowed disabled:opacity-40">Clear filters</button>
      </div>
      <p className="mt-2 text-xs text-muted-foreground">Showing {filtered.length} of {rankings.length} players</p>
    </div>

    <div className="overflow-x-auto rounded-2xl border bg-card"><table className="w-full min-w-[1120px] text-sm"><thead className="bg-muted text-left text-xs uppercase text-muted-foreground"><tr><th className="p-3">Our rank</th><th className="p-3">Player</th><th className="p-3">Role & signals</th><th className="p-3">Current ADP</th><th className="p-3">Value</th><th className="p-3">2025 GP</th><th className="p-3">2025 FPTS</th><th className="p-3">2026 proj</th><th className="p-3">2026 GP</th><th className="p-3">Conf.</th></tr></thead><tbody>{filtered.map((player) => {
      const rank=player.ourRank??player.ecr; const delta=player.adp!==null&&rank!==null?player.adp-rank:null;
      return <tr key={player.playerId} className="border-t align-top hover:bg-muted/40"><td className="p-3 text-lg font-black">{rank??"—"}<span className="block text-xs font-medium text-muted-foreground">{player.position}{player.positionRank??""} · T{player.tier??"—"}</span></td><td className="p-3"><p className="font-bold">{player.name}</p><p className="text-xs text-muted-foreground">{player.position} · {player.team??"FA"} · Bye {player.byeWeek??"—"}</p></td><td className="max-w-[330px] p-3"><div className="flex flex-wrap gap-1">{player.indicators.slice(0,3).map((badge) => <span key={badge.code} title={JSON.stringify(badge.evidence)} className={`rounded-full px-2 py-1 text-[10px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}</div></td><td className="p-3">{player.adp?.toFixed(1)??"—"}</td><td className={`p-3 font-bold ${delta===null?"text-muted-foreground":delta>=0?"text-emerald-700":"text-red-700"}`}>{delta===null?"—":`${delta>=0?"+":""}${delta.toFixed(1)}`}</td><td className="p-3">{player.games2025??"—"}</td><td className="p-3 font-semibold">{player.fantasyPoints2025?.toFixed(1)??"—"}</td><td className="p-3 font-semibold">{player.ourProjectedPoints?.toFixed(1)??"—"}</td><td className="p-3">{player.expectedGames?.toFixed(1)??"—"}</td><td className="p-3">{player.confidence!==null?`${Math.round(player.confidence*100)}%`:"—"}</td></tr>})}</tbody></table>
      {filtered.length === 0 && <p className="border-t p-8 text-center text-sm text-muted-foreground">No players match these filters.</p>}
    </div>
  </div>;
}
