"use client";

import { useMemo, useState } from "react";
import type { YoutubePickRow } from "./queries";

const SPORT_ICON: Record<string, string> = {
  nba: "🏀",
  mlb: "⚾",
  nfl: "🏈",
  nhl: "🏒",
  wnba: "🏀",
  soccer: "⚽",
  tennis: "🎾",
  f1: "🏎️",
  other: "🎲",
};

const BET_TYPE_LABEL: Record<string, string> = {
  moneyline: "Moneyline",
  spread: "Spread",
  total: "Total",
  prop: "Prop",
  futures: "Futures",
  other: "Other",
};

function fmtOdds(odds: number | null): string {
  if (odds == null) return "—";
  return odds > 0 ? `+${odds}` : `${odds}`;
}

function fmtDate(d: Date | null): string {
  if (!d) return "—";
  return new Date(d).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

export function YoutubePicksClient({ picks }: { picks: YoutubePickRow[] }) {
  const [sport, setSport] = useState("all");
  const [betType, setBetType] = useState("all");
  const [search, setSearch] = useState("");
  const [expanded, setExpanded] = useState<Set<number>>(new Set());

  const sports = useMemo(
    () => Array.from(new Set(picks.map((p) => p.sport))).sort(),
    [picks],
  );
  const betTypes = useMemo(
    () => Array.from(new Set(picks.map((p) => p.betType))).sort(),
    [picks],
  );

  const filtered = picks.filter((p) => {
    if (sport !== "all" && p.sport !== sport) return false;
    if (betType !== "all" && p.betType !== betType) return false;
    if (search) {
      const q = search.toLowerCase();
      const haystack = `${p.subject} ${p.opponent ?? ""} ${p.selection}`.toLowerCase();
      if (!haystack.includes(q)) return false;
    }
    return true;
  });

  function toggleExpanded(id: number) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  return (
    <main className="mx-auto max-w-5xl space-y-4 p-4">
      <div>
        <h1 className="text-xl font-bold">🎯 YouTube Picks Tracker</h1>
        <p className="text-sm text-muted-foreground">
          Betting picks extracted from tracked YouTube channels (currently: BettingPros).
          Every pick links back to its source quote and video.
        </p>
      </div>

      <div className="rounded-lg border border-amber-500/40 bg-amber-500/10 p-3 text-sm text-amber-700 dark:text-amber-300">
        ⚠ Settlement isn&apos;t built yet — every pick shows as{" "}
        <span className="font-medium">pending</span>. There&apos;s no win/loss or accuracy
        data here yet; this page is extraction visibility only.
      </div>

      <div className="flex flex-wrap items-center gap-2 text-sm">
        <input
          type="text"
          placeholder="Search team / player..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="rounded border bg-background px-2 py-1.5 text-sm placeholder:text-muted-foreground w-52"
        />
        <label className="flex items-center gap-1">
          <span className="text-muted-foreground">Sport</span>
          <select value={sport} onChange={(e) => setSport(e.target.value)}
            className="rounded border bg-background px-1.5 py-1.5">
            <option value="all">All</option>
            {sports.map((s) => (
              <option key={s} value={s}>
                {SPORT_ICON[s] ?? ""} {s.toUpperCase()}
              </option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-1">
          <span className="text-muted-foreground">Bet Type</span>
          <select value={betType} onChange={(e) => setBetType(e.target.value)}
            className="rounded border bg-background px-1.5 py-1.5">
            <option value="all">All</option>
            {betTypes.map((b) => (
              <option key={b} value={b}>{BET_TYPE_LABEL[b] ?? b}</option>
            ))}
          </select>
        </label>
        <span className="text-xs text-muted-foreground">
          {filtered.length} of {picks.length} picks
        </span>
      </div>

      {filtered.length === 0 ? (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No picks match these filters.
        </div>
      ) : (
        <div className="space-y-2">
          {filtered.map((p) => (
            <div key={p.id} className="rounded-lg border bg-card p-3">
              <div className="flex flex-wrap items-start justify-between gap-2">
                <div className="flex items-center gap-2">
                  <span aria-hidden="true">{SPORT_ICON[p.sport] ?? "🎲"}</span>
                  <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] uppercase text-muted-foreground">
                    {BET_TYPE_LABEL[p.betType] ?? p.betType}
                  </span>
                  <span className="font-medium">
                    {p.subject}
                    {p.opponent ? ` vs ${p.opponent}` : ""}
                  </span>
                  {p.confidenceLabel && (
                    <span className="rounded-full bg-blue-500/10 px-2 py-0.5 text-[10px] font-medium text-blue-600 dark:text-blue-400">
                      {p.confidenceLabel}
                    </span>
                  )}
                </div>
                <span className="rounded-full bg-muted px-2 py-0.5 text-[10px] font-medium uppercase text-muted-foreground">
                  {p.status}
                </span>
              </div>

              <div className="mt-1 flex flex-wrap items-baseline gap-2">
                <span className="text-sm">{p.selection}</span>
                {p.oddsAmerican != null && (
                  <span className="text-sm font-medium tabular-nums text-emerald-600 dark:text-emerald-400">
                    {fmtOdds(p.oddsAmerican)}
                  </span>
                )}
              </div>

              <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                <span>{fmtDate(p.publishedAt)}</span>
                <span>·</span>
                <a
                  href={`https://www.youtube.com/watch?v=${p.youtubeVideoId}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="underline hover:text-foreground"
                >
                  {p.videoTitle}
                </a>
                {p.gameContext && (
                  <>
                    <span>·</span>
                    <span>{p.gameContext}</span>
                  </>
                )}
                <button
                  onClick={() => toggleExpanded(p.id)}
                  className="ml-auto underline hover:text-foreground"
                >
                  {expanded.has(p.id) ? "hide quote" : "show quote"}
                </button>
              </div>

              {expanded.has(p.id) && (
                <blockquote className="mt-2 border-l-2 border-muted-foreground/30 pl-3 text-xs italic text-muted-foreground">
                  &quot;{p.quote}&quot;
                </blockquote>
              )}
            </div>
          ))}
        </div>
      )}
    </main>
  );
}
