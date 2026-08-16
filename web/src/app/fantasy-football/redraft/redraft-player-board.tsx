"use client";

import { memo, useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import { fantasyBadgeClass } from "@/lib/fantasy-football/badge-style";
import { REDRAFT_FLEX_POSITIONS, REDRAFT_POSITIONS, REDRAFT_POSITION_LABEL, type RedraftPosition } from "@/lib/fantasy-football/redraft";
import type { AvailabilityOdds } from "@/lib/fantasy-football/availability-odds";
import ProjectionNotation from "../rankings/projection-notation";

const COLUMN_GRID = "grid-cols-[76px_104px_minmax(220px,1fr)_76px_minmax(260px,1.3fr)_78px_92px_86px_96px_126px_150px_100px]";
const TABLE_MIN_WIDTH = "min-w-[1500px]";
const FLEX_POSITIONS = new Set<string>(REDRAFT_FLEX_POSITIONS);

type SortKey = "skillRank" | "overallRank" | "name" | "adp" | "avail" | "gp2025" | "fpts2025" | "fpProj" | "ourProj" | "projDelta";
type SortDir = "asc" | "desc";

// Lower-is-better columns (rank/ADP-shaped) default to ascending on first
// click; higher-is-better columns (points/probability) default to descending,
// so the first click on any header surfaces the "best" players first.
const SORT_HEADERS: Array<{ key: SortKey; label: string; defaultDir: SortDir; title?: string }> = [
  { key: "skillRank", label: "Rank", defaultDir: "asc" },
  { key: "overallRank", label: "Overall / Pos.", defaultDir: "asc" },
  { key: "name", label: "Player", defaultDir: "asc" },
  { key: "adp", label: "ADP", defaultDir: "asc", title: "12-team ADP from Fantasy Football Calculator. Comparison data only -- never changes our rank or projection." },
  { key: "avail", label: "Avail.", defaultDir: "desc", title: "P(still available at your next pick), from FFC's observed ADP mean/variance/sample size" },
  { key: "gp2025", label: "2025 GP", defaultDir: "desc" },
  { key: "fpts2025", label: "2025 FPTS", defaultDir: "desc", title: "Actual 2025 PPR fantasy points. For DEF this is real Yahoo-scored team-defense scoring." },
  { key: "fpProj", label: "FantasyPros PPR Proj.", defaultDir: "desc" },
  { key: "ourProj", label: "Our 2026 PPR Base", defaultDir: "desc" },
  { key: "projDelta", label: "Our Δ FP", defaultDir: "desc", title: "Our projection minus FantasyPros'. Positive = we project this player higher. Comparison only -- never blended into our board." },
];

type RedraftPlayerBoardProps = {
  rankings: FantasyRankingRow[];
  draftedPlayerIds: number[];
  canDraft: boolean;
  onDraft: (playerId: number) => void;
  availabilityByPlayerId: Map<number, AvailabilityOdds | null>;
};

type PlayerRowProps = {
  player: FantasyRankingRow;
  skillRank: number;
  canDraft: boolean;
  onDraft: (playerId: number) => void;
  odds: AvailabilityOdds | null | undefined;
};

function AvailabilityCell({ odds }: { odds: AvailabilityOdds | null | undefined }) {
  if (!odds) return <span className="text-muted-foreground">—</span>;
  const pct = Math.round(odds.probability * 100);
  const tone = pct >= 66 ? "text-emerald-700" : pct >= 33 ? "text-amber-700" : "text-red-700";
  return (
    <span className={`font-semibold ${tone}`} title={`FFC ADP ${odds.adjustedAdp.toFixed(1)} ± ${odds.adjustedStdev.toFixed(1)} picks · ${odds.sampleSize ?? "few"} drafts sampled (${odds.confidence} confidence)`}>
      {pct}%
    </span>
  );
}

function displayPosition(position: string): string {
  return REDRAFT_POSITION_LABEL[position as RedraftPosition] ?? position;
}

const RedraftPlayerRow = memo(function RedraftPlayerRow({ player, skillRank, canDraft, onDraft, odds }: PlayerRowProps) {
  const overallRank = player.ourRank ?? player.ecr ?? skillRank;
  const projDelta = player.ourProjectedPoints !== null && player.fantasyProsProjectedPoints !== null
    ? player.ourProjectedPoints - player.fantasyProsProjectedPoints
    : null;
  const isDefense = player.position === "DST";
  return <>
    <div role="cell" className="p-3 text-lg font-black">{skillRank}</div>
    <div role="cell" className="p-3"><p className="text-base font-black">#{overallRank}</p><p className="text-xs font-semibold text-muted-foreground">{displayPosition(player.position)}{player.positionRank ?? "—"}</p></div>
    <div role="cell" className="sticky left-0 z-10 border-r bg-card p-3 shadow-[4px_0_8px_-6px_rgba(15,23,42,0.45)]"><p className="font-bold">{player.name}</p><p className="text-xs text-muted-foreground">{displayPosition(player.position)} · {player.team ?? "FA"} · Bye {player.byeWeek ?? "—"}</p></div>
    <div role="cell" className="p-3"><button disabled={!canDraft} onClick={() => onDraft(player.playerId)} className="rounded-lg bg-emerald-600 px-3 py-1.5 text-xs font-bold text-white disabled:opacity-35">Draft</button></div>
    <div role="cell" className="max-w-[290px] p-3"><div className="flex flex-wrap gap-1">
      {player.indicators.slice(0, 3).map((badge) => <span key={badge.code} className={`rounded-full px-2 py-1 text-[10px] font-bold ring-1 ring-inset ${fantasyBadgeClass(badge)}`}>{badge.label}</span>)}
    </div></div>
    <div role="cell" className="p-3">{player.adp?.toFixed(1) ?? "—"}</div>
    <div role="cell" className="p-3"><AvailabilityCell odds={odds} /></div>
    <div role="cell" className="p-3">{player.games2025 ?? "—"}</div>
    <div role="cell" className="p-3 font-semibold">{player.fantasyPoints2025?.toFixed(1) ?? "—"}</div>
    <div role="cell" className="p-3 font-semibold" title={player.fantasyProsProjectionUpdatedAt ? `FantasyPros source updated ${new Date(player.fantasyProsProjectionUpdatedAt).toLocaleString()}` : "No matched FantasyPros PPR projection"}>{player.fantasyProsProjectedPoints?.toFixed(1) ?? "—"}</div>
    <div role="cell" className="p-3 font-semibold">
      {player.ourProjectedPoints?.toFixed(1) ?? "—"}
      {/* Defenses are deliberately compressed into a narrow band -- prior-season
          carry-forward sets their ORDER, but the magnitude is shrunk hard toward
          the league prior because DST year-over-year signal is very weak. The
          2025 FPTS column shows the real uncompressed number. */}
      {isDefense
        ? <span className="ml-1 text-[10px] font-semibold text-amber-700" title="DEF ordering comes from 2025 results, but the projected total is shrunk toward the league average: year-over-year defensive signal is weak (Spearman 0.18). See the 2025 FPTS column for the real number.">ranked on 2025</span>
        : <ProjectionNotation details={player.projectionDetails} label="How projected" />}
    </div>
    <div role="cell" className={`p-3 font-bold ${projDelta === null ? "text-muted-foreground" : projDelta >= 0 ? "text-emerald-700" : "text-red-700"}`}>{projDelta === null ? "—" : `${projDelta >= 0 ? "+" : ""}${projDelta.toFixed(1)}`}</div>
  </>;
});

function sortValue(player: FantasyRankingRow, key: SortKey, skillRank: number, availProbability: number | null): number | string | null {
  switch (key) {
    case "skillRank": return skillRank;
    case "overallRank": return player.ourRank ?? player.ecr ?? skillRank;
    case "name": return player.name;
    case "adp": return player.adp;
    case "avail": return availProbability;
    case "gp2025": return player.games2025;
    case "fpts2025": return player.fantasyPoints2025;
    case "fpProj": return player.fantasyProsProjectedPoints;
    case "ourProj": return player.ourProjectedPoints;
    case "projDelta": return player.ourProjectedPoints !== null && player.fantasyProsProjectedPoints !== null
      ? player.ourProjectedPoints - player.fantasyProsProjectedPoints
      : null;
  }
}

function SortHeader({ config, active, dir, onSort }: { config: (typeof SORT_HEADERS)[number]; active: boolean; dir: SortDir; onSort: (key: SortKey, defaultDir: SortDir) => void }) {
  return (
    <button
      type="button"
      onClick={() => onSort(config.key, config.defaultDir)}
      title={config.title}
      className={`flex w-full items-center gap-1 p-3 text-left hover:text-foreground ${active ? "text-foreground" : ""}`}
    >
      {config.label}
      <span className="text-[10px]">{active ? (dir === "asc" ? "▲" : "▼") : ""}</span>
    </button>
  );
}

export default function RedraftPlayerBoard({ rankings, draftedPlayerIds, canDraft, onDraft, availabilityByPlayerId }: RedraftPlayerBoardProps) {
  const [name, setName] = useState("");
  const [position, setPosition] = useState("");
  const [team, setTeam] = useState("");
  const [sortKey, setSortKey] = useState<SortKey | null>(null);
  const [sortDir, setSortDir] = useState<SortDir>("asc");
  const scrollRef = useRef<HTMLDivElement>(null);
  const draftedIds = useMemo(() => new Set(draftedPlayerIds), [draftedPlayerIds]);
  const skillRankById = useMemo(() => new Map(rankings.map((player, index) => [player.playerId, index + 1])), [rankings]);
  const teams = useMemo(() => [...new Set(rankings.flatMap((player) => player.team ? [player.team] : []))].sort(), [rankings]);

  const handleSort = (key: SortKey, defaultDir: SortDir) => {
    if (sortKey === key) setSortDir((current) => (current === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir(defaultDir); }
  };

  const filtered = useMemo(() => {
    const search = name.trim().toLocaleLowerCase();
    const rows = rankings.filter((player) => (
      !draftedIds.has(player.playerId)
      && (!search || player.name.toLocaleLowerCase().includes(search))
      && (!position || (position === "FLEX" ? FLEX_POSITIONS.has(player.position) : player.position === position))
      && (!team || player.team === team)
    ));
    if (!sortKey) return rows;
    const dir = sortDir === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const aValue = sortValue(a, sortKey, skillRankById.get(a.playerId) ?? 999, availabilityByPlayerId.get(a.playerId)?.probability ?? null);
      const bValue = sortValue(b, sortKey, skillRankById.get(b.playerId) ?? 999, availabilityByPlayerId.get(b.playerId)?.probability ?? null);
      // Nulls always sort last regardless of direction -- missing data must not
      // claim the "best" slot just because null compares as smaller.
      if (aValue === null && bValue === null) return 0;
      if (aValue === null) return 1;
      if (bValue === null) return -1;
      if (typeof aValue === "string" || typeof bValue === "string") return String(aValue).localeCompare(String(bValue)) * dir;
      return (aValue - bValue) * dir;
    });
  }, [rankings, draftedIds, name, position, team, sortKey, sortDir, skillRankById, availabilityByPlayerId]);

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
  }, [name, position, team, sortKey, sortDir, rowVirtualizer]);

  return <section className="space-y-3">
    <div className="grid gap-3 rounded-2xl border bg-card p-3 sm:grid-cols-2 lg:grid-cols-[minmax(220px,1fr)_180px_180px_auto] lg:items-end">
      <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Name<input value={name} onChange={(event) => setName(event.target.value)} placeholder="Search player" className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground" /></label>
      <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Position<select value={position} onChange={(event) => setPosition(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All positions</option><option value="FLEX">FLEX (RB/WR/TE)</option>{REDRAFT_POSITIONS.map((value) => <option key={value} value={value}>{REDRAFT_POSITION_LABEL[value]}</option>)}</select></label>
      <label className="space-y-1 text-xs font-bold uppercase tracking-wide text-muted-foreground">Team<select value={team} onChange={(event) => setTeam(event.target.value)} className="block w-full rounded-lg border bg-background px-3 py-2 text-sm font-normal normal-case tracking-normal text-foreground"><option value="">All teams</option>{teams.map((value) => <option key={value} value={value}>{value}</option>)}</select></label>
      <button onClick={() => { setName(""); setPosition(""); setTeam(""); setSortKey(null); }} className="rounded-lg border px-3 py-2 text-sm font-semibold">Clear filters</button>
    </div>
    <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
      <p>{filtered.length} available players · drafted players are removed from the board</p>
      <p>Click any column header to sort · fast list, only visible rows are rendered</p>
    </div>
    <div ref={scrollRef} role="table" aria-rowcount={filtered.length + 1} className={`h-[min(68vh,680px)] overflow-auto rounded-2xl border bg-card text-sm [contain:strict]`}>
      <div role="rowgroup" className={`sticky top-0 z-20 ${TABLE_MIN_WIDTH} bg-muted text-left text-xs uppercase text-muted-foreground`}>
        <div role="row" className={`grid ${COLUMN_GRID}`}>
          {SORT_HEADERS.slice(0, 3).map((config) => <div key={config.key} role="columnheader" className={config.key === "name" ? "sticky left-0 z-30 border-r bg-muted shadow-[4px_0_8px_-6px_rgba(15,23,42,0.45)]" : undefined}><SortHeader config={config} active={sortKey === config.key} dir={sortDir} onSort={handleSort} /></div>)}
          <div role="columnheader" className="p-3">Draft</div>
          <div role="columnheader" className="p-3">Signals</div>
          {SORT_HEADERS.slice(3).map((config) => <div key={config.key} role="columnheader"><SortHeader config={config} active={sortKey === config.key} dir={sortDir} onSort={handleSort} /></div>)}
        </div>
      </div>
      <div role="rowgroup" className={`relative ${TABLE_MIN_WIDTH}`} style={{ height: `${rowVirtualizer.getTotalSize()}px` }}>
        {rowVirtualizer.getVirtualItems().map((virtualRow) => {
          const player = filtered[virtualRow.index];
          return <div key={player.playerId} ref={rowVirtualizer.measureElement} data-index={virtualRow.index} role="row" aria-rowindex={virtualRow.index + 2} className={`absolute left-0 top-0 grid w-full border-t align-top hover:bg-muted/40 ${COLUMN_GRID}`} style={{ transform: `translateY(${virtualRow.start}px)` }}><RedraftPlayerRow player={player} skillRank={skillRankById.get(player.playerId) ?? 999} canDraft={canDraft} onDraft={onDraft} odds={availabilityByPlayerId.get(player.playerId)} /></div>;
        })}
      </div>
      {filtered.length === 0 && <p className={`${TABLE_MIN_WIDTH} border-t p-8 text-center text-sm text-muted-foreground`}>No available players match these filters.</p>}
    </div>
  </section>;
}
