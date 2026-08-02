"use client";

import { memo, useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import { BEST_BALL_POSITIONS, type BestBallPosition } from "@/lib/fantasy-football/best-ball";
import ProjectionNotation from "../rankings/projection-notation";

const COLUMN_GRID = "grid-cols-[76px_104px_minmax(220px,1fr)_minmax(280px,1.35fr)_78px_86px_96px_126px_150px_76px]";

type BestBallPlayerBoardProps = {
  rankings: FantasyRankingRow[];
  draftedPlayerIds: number[];
  canDraftPosition: Record<BestBallPosition, boolean>;
  onDraft: (playerId: number) => void;
};

type PlayerRowProps = {
  player: FantasyRankingRow;
  skillRank: number;
  canDraft: boolean;
  onDraft: (playerId: number) => void;
};

const BestBallPlayerRow = memo(function BestBallPlayerRow({ player, skillRank, canDraft, onDraft }: PlayerRowProps) {
  const overallRank = player.ourRank ?? player.ecr ?? skillRank;
  return <>
    <div role="cell" className="p-3 text-lg font-black">{skillRank}</div>
    <div role="cell" className="p-3"><p className="text-base font-black">#{overallRank}</p><p className="text-xs font-semibold text-muted-foreground">{player.position}{player.positionRank ?? "—"}</p></div>
    <div role="cell" className="p-3"><p className="font-bold">{player.name}</p><p className="text-xs text-muted-foreground">{player.position} · {player.team ?? "FA"} · Bye {player.byeWeek ?? "—"}</p></div>
    <div role="cell" className="max-w-[310px] p-3"><div className="flex flex-wrap gap-1">{player.indicators.slice(0, 3).map((badge) => <span key={badge.code} className={`rounded-full px-2 py-1 text-[10px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}</div></div>
    <div role="cell" className="p-3">{player.adp?.toFixed(1) ?? "—"}</div>
    <div role="cell" className="p-3">{player.games2025 ?? "—"}</div>
    <div role="cell" className="p-3">{player.fantasyPoints2025?.toFixed(1) ?? "—"}</div>
    <div role="cell" className="p-3 font-semibold" title={player.fantasyProsProjectionUpdatedAt ? `FantasyPros source updated ${new Date(player.fantasyProsProjectionUpdatedAt).toLocaleString()}` : "No matched FantasyPros PPR projection"}>{player.fantasyProsProjectedPoints?.toFixed(1) ?? "—"}</div>
    <div role="cell" className="p-3 font-semibold">{player.ourProjectedPoints?.toFixed(1) ?? "—"}<ProjectionNotation details={player.projectionDetails} /></div>
    <div role="cell" className="p-3"><button disabled={!canDraft} onClick={() => onDraft(player.playerId)} className="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-bold text-white disabled:opacity-35">Add</button></div>
  </>;
});

export default function BestBallPlayerBoard({ rankings, draftedPlayerIds, canDraftPosition, onDraft }: BestBallPlayerBoardProps) {
  const [name, setName] = useState("");
  const [position, setPosition] = useState("");
  const [team, setTeam] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);
  const draftedIds = useMemo(() => new Set(draftedPlayerIds), [draftedPlayerIds]);
  const skillRankById = useMemo(() => new Map(rankings.map((player, index) => [player.playerId, index + 1])), [rankings]);
  const teams = useMemo(() => [...new Set(rankings.flatMap((player) => player.team ? [player.team] : []))].sort(), [rankings]);
  const filtered = useMemo(() => {
    const search = name.trim().toLocaleLowerCase();
    return rankings.filter((player) => (
      !draftedIds.has(player.playerId)
      && (!search || player.name.toLocaleLowerCase().includes(search))
      && (!position || player.position === position)
      && (!team || player.team === team)
    ));
  }, [rankings, draftedIds, name, position, team]);

  // eslint-disable-next-line react-hooks/incompatible-library -- TanStack Virtual owns row windowing for this component.
  const rowVirtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 78,
    overscan: 6,
    getItemKey: (index) => filtered[index]?.playerId ?? index,
  });

  useEffect(() => {
    rowVirtualizer.scrollToOffset(0);
  }, [name, position, team, rowVirtualizer]);

  return <section className="space-y-3">
    <div className="grid gap-3 rounded-2xl border bg-card p-3 sm:grid-cols-2 lg:grid-cols-[minmax(220px,1fr)_180px_180px_auto] lg:items-end">
      <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Name<input value={name} onChange={(event) => setName(event.target.value)} placeholder="Search player" className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground" /></label>
      <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Position<select value={position} onChange={(event) => setPosition(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All positions</option>{BEST_BALL_POSITIONS.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
      <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Team<select value={team} onChange={(event) => setTeam(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All teams</option>{teams.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
      <button onClick={() => { setName(""); setPosition(""); setTeam(""); }} className="rounded-lg border px-3 py-2 text-sm font-semibold">Clear filters</button>
    </div>
    <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground"><p>{filtered.length} available players · drafted players are removed from the board</p><p>Fast list · only visible rows are rendered</p></div>
    <div ref={scrollRef} role="table" aria-rowcount={filtered.length + 1} className="h-[min(68vh,680px)] overflow-auto rounded-2xl border bg-card text-sm [contain:strict]">
      <div role="rowgroup" className="sticky top-0 z-20 min-w-[1350px] bg-muted text-left text-xs uppercase text-muted-foreground">
        <div role="row" className={`grid ${COLUMN_GRID}`}><div role="columnheader" className="p-3">Skill rank</div><div role="columnheader" className="p-3">Overall / Pos.</div><div role="columnheader" className="p-3">Player</div><div role="columnheader" className="p-3">Signals</div><div role="columnheader" className="p-3">ADP</div><div role="columnheader" className="p-3">2025 GP</div><div role="columnheader" className="p-3">2025 FPTS</div><div role="columnheader" className="p-3">FantasyPros PPR Proj.</div><div role="columnheader" className="p-3">Our 2026 PPR Base</div><div role="columnheader" className="p-3">Draft</div></div>
      </div>
      <div role="rowgroup" className="relative min-w-[1350px]" style={{ height: `${rowVirtualizer.getTotalSize()}px` }}>
        {rowVirtualizer.getVirtualItems().map((virtualRow) => {
          const player = filtered[virtualRow.index];
          return <div key={player.playerId} ref={rowVirtualizer.measureElement} data-index={virtualRow.index} role="row" aria-rowindex={virtualRow.index + 2} className={`absolute left-0 top-0 grid w-full border-t align-top hover:bg-muted/40 ${COLUMN_GRID}`} style={{ transform: `translateY(${virtualRow.start}px)` }}><BestBallPlayerRow player={player} skillRank={skillRankById.get(player.playerId) ?? 999} canDraft={canDraftPosition[player.position as BestBallPosition]} onDraft={onDraft} /></div>;
        })}
      </div>
      {filtered.length === 0 && <p className="min-w-[1350px] border-t p-8 text-center text-sm text-muted-foreground">No available players match these filters.</p>}
    </div>
  </section>;
}
