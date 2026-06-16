"use client";

import { useState } from "react";
import type {
  SoccerEloTeamRow,
  SoccerCompletedResultRow,
  SoccerFuturesBetRow,
} from "@/db/queries";

// ── Formatting helpers ────────────────────────────────────────────────────────
const fmtPct = (v: number | null) =>
  v == null ? "—" : `${(v * 100).toFixed(0)}%`;
const fmtPct1 = (v: number | null) =>
  v == null ? "—" : `${(v * 100).toFixed(1)}%`;
const fmtMl = (ml: number | null) =>
  ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml);
const fmtSigned = (v: number) =>
  `${v >= 0 ? "+" : ""}${(v * 100).toFixed(0)}pp`;

// ── Elo math (neutral venue, K=60, FIFA World Cup) ───────────────────────────
function eloExpected(homeElo: number, awayElo: number) {
  return 1 / (1 + Math.pow(10, (awayElo - homeElo) / 400));
}

function goalDiffMult(margin: number) {
  if (margin <= 1) return 1.0;
  if (margin === 2) return 1.5;
  return (11 + margin) / 8;
}

function computeEloDelta(
  homeElo: number,
  awayElo: number,
  homeScore: number,
  awayScore: number
): { expectedHome: number; delta: number; result: "home" | "draw" | "away" } {
  const expected = eloExpected(homeElo, awayElo);
  const margin = Math.abs(homeScore - awayScore);
  const K = 60 * goalDiffMult(margin);
  const actual =
    homeScore > awayScore ? 1.0 : homeScore < awayScore ? 0.0 : 0.5;
  return {
    expectedHome: expected,
    delta: Math.round(K * (actual - expected)),
    result:
      homeScore > awayScore ? "home" : homeScore < awayScore ? "away" : "draw",
  };
}

function fmtDate(d: string) {
  return new Date(`${d}T00:00:00`).toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function Stars({ n }: { n: number }) {
  return (
    <span className="tabular-nums text-amber-400">
      {"★".repeat(n)}
      <span className="text-muted-foreground/40">{"★".repeat(5 - n)}</span>
    </span>
  );
}

// ── Rankings tab ──────────────────────────────────────────────────────────────
function RankingsTab({ teams }: { teams: SoccerEloTeamRow[] }) {
  // Overall sorted by Elo for global rank
  const byElo = [...teams].sort((a, b) => b.elo - a.elo);
  const globalRank = new Map(byElo.map((t, i) => [t.teamId, i + 1]));

  // Group into WC groups (null = no group assigned yet)
  const grouped = new Map<string, SoccerEloTeamRow[]>();
  const ungrouped: SoccerEloTeamRow[] = [];
  for (const t of teams) {
    if (t.groupLabel) {
      const list = grouped.get(t.groupLabel) ?? [];
      list.push(t);
      grouped.set(t.groupLabel, list);
    } else {
      ungrouped.push(t);
    }
  }
  const groups = Array.from(grouped.entries()).sort(([a], [b]) =>
    a.localeCompare(b)
  );

  const ratingDate = teams[0]?.ratingDate ?? null;

  return (
    <div className="space-y-6">
      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        Elo ratings trained from 49k+ international results (1872–present) plus
        live WC 2026 scores, retrained every 3 hours.{" "}
        <strong className="text-foreground">Attack</strong> and{" "}
        <strong className="text-foreground">Defense</strong> are Poisson GLM
        log-coefficients — positive attack = above average scorer, negative
        defense = good (concedes fewer goals).{" "}
        {ratingDate && (
          <span>
            Last retrained:{" "}
            <span className="text-foreground">{ratingDate}</span>.
          </span>
        )}
      </p>

      {/* Global top-16 */}
      <div>
        <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
          Global Power Rankings
        </h2>
        <div className="overflow-x-auto rounded-lg border bg-card">
          <table className="w-full min-w-[540px] text-sm">
            <thead>
              <tr className="border-b text-xs text-muted-foreground">
                <th className="px-3 py-2 text-left font-medium">Rank</th>
                <th className="px-3 py-2 text-left font-medium">Team</th>
                <th className="px-2 py-2 text-center font-medium">Group</th>
                <th className="px-2 py-2 text-center font-medium">Elo</th>
                <th className="px-2 py-2 text-center font-medium">Attack</th>
                <th className="px-2 py-2 text-center font-medium">Defense</th>
                <th className="px-2 py-2 text-center font-medium">Games</th>
              </tr>
            </thead>
            <tbody>
              {byElo.slice(0, 20).map((t, i) => (
                <tr
                  key={t.teamId}
                  className="border-b last:border-0 hover:bg-accent/40"
                >
                  <td className="px-3 py-2 tabular-nums text-muted-foreground">
                    {i + 1}
                  </td>
                  <td className="px-3 py-2 font-medium">{t.name}</td>
                  <td className="px-2 py-2 text-center text-xs text-muted-foreground">
                    {t.groupLabel ?? "—"}
                  </td>
                  <td className="px-2 py-2 text-center tabular-nums font-semibold">
                    {Math.round(t.elo)}
                  </td>
                  <td
                    className={`px-2 py-2 text-center tabular-nums text-xs ${t.attack > 0 ? "text-emerald-400" : "text-rose-400"}`}
                  >
                    {t.attack >= 0 ? "+" : ""}
                    {t.attack.toFixed(3)}
                  </td>
                  <td
                    className={`px-2 py-2 text-center tabular-nums text-xs ${t.defense < 0 ? "text-emerald-400" : "text-rose-400"}`}
                  >
                    {t.defense >= 0 ? "+" : ""}
                    {t.defense.toFixed(3)}
                  </td>
                  <td className="px-2 py-2 text-center tabular-nums text-xs text-muted-foreground">
                    {t.matches}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="mt-1 text-[11px] text-muted-foreground">
          Defense: negative = elite (concedes fewer goals than average). Lower
          is better.
        </p>
      </div>

      {/* Per-group tables */}
      {groups.length > 0 && (
        <div>
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            By Group
          </h2>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {groups.map(([label, groupTeams]) => (
              <div key={label} className="rounded-lg border bg-card">
                <div className="border-b px-3 py-2 font-semibold text-sm">
                  Group {label}
                </div>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b text-[10px] text-muted-foreground">
                      <th className="px-3 py-1.5 text-left font-medium">
                        #
                      </th>
                      <th className="px-3 py-1.5 text-left font-medium">
                        Team
                      </th>
                      <th className="px-2 py-1.5 text-center font-medium">
                        Elo
                      </th>
                      <th className="px-2 py-1.5 text-center font-medium">
                        Atk
                      </th>
                      <th className="px-2 py-1.5 text-center font-medium">
                        Def
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {[...groupTeams]
                      .sort((a, b) => b.elo - a.elo)
                      .map((t) => (
                        <tr
                          key={t.teamId}
                          className="border-b last:border-0 hover:bg-accent/40"
                        >
                          <td className="px-3 py-1.5 text-xs tabular-nums text-muted-foreground">
                            #{globalRank.get(t.teamId)}
                          </td>
                          <td className="px-3 py-1.5 font-medium text-sm">
                            {t.name}
                          </td>
                          <td className="px-2 py-1.5 text-center tabular-nums font-semibold">
                            {Math.round(t.elo)}
                          </td>
                          <td
                            className={`px-2 py-1.5 text-center tabular-nums text-xs ${t.attack > 0 ? "text-emerald-400" : "text-rose-400"}`}
                          >
                            {t.attack >= 0 ? "+" : ""}
                            {t.attack.toFixed(2)}
                          </td>
                          <td
                            className={`px-2 py-1.5 text-center tabular-nums text-xs ${t.defense < 0 ? "text-emerald-400" : "text-rose-400"}`}
                          >
                            {t.defense >= 0 ? "+" : ""}
                            {t.defense.toFixed(2)}
                          </td>
                        </tr>
                      ))}
                  </tbody>
                </table>
              </div>
            ))}
          </div>
        </div>
      )}

      {ungrouped.length > 0 && (
        <p className="text-xs text-muted-foreground">
          {ungrouped.length} team{ungrouped.length !== 1 ? "s" : ""} not yet
          assigned to a group (groups derive from loaded fixtures).
        </p>
      )}
    </div>
  );
}

// ── Results tab ───────────────────────────────────────────────────────────────
function ResultsTab({ results }: { results: SoccerCompletedResultRow[] }) {
  if (results.length === 0) {
    return (
      <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
        No completed WC 2026 results yet. Scores populate after each game
        settles.
      </div>
    );
  }

  // Group by date
  const byDate = new Map<string, SoccerCompletedResultRow[]>();
  for (const r of results) {
    const list = byDate.get(r.gameDate) ?? [];
    list.push(r);
    byDate.set(r.gameDate, list);
  }
  const days = Array.from(byDate.keys()).sort();

  return (
    <div className="space-y-5">
      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        <strong className="text-foreground">Elo Δ</strong> is computed from
        current ratings using the FIFA World Cup formula (K=60, neutral venue,
        margin multiplier). Positive = home team gained Elo.{" "}
        <strong className="text-foreground">Expected</strong> shows our
        model&apos;s pre-game win probability for the home side.
      </p>

      {days.map((day) => (
        <div key={day} className="space-y-2">
          <h3 className="text-sm font-semibold text-muted-foreground">
            {new Date(`${day}T00:00:00`).toLocaleDateString(undefined, {
              weekday: "long",
              month: "long",
              day: "numeric",
            })}
          </h3>
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[680px] text-sm">
              <thead>
                <tr className="border-b text-xs text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Match</th>
                  <th className="px-2 py-2 text-center font-medium">Score</th>
                  <th className="px-2 py-2 text-center font-medium">
                    Elo (H / A)
                  </th>
                  <th className="px-2 py-2 text-center font-medium">
                    Vegas %
                  </th>
                  <th className="px-2 py-2 text-center font-medium">
                    Model %
                  </th>
                  <th className="px-2 py-2 text-center font-medium">Elo Δ</th>
                  <th className="px-2 py-2 text-center font-medium">
                    Outcome
                  </th>
                </tr>
              </thead>
              <tbody>
                {(byDate.get(day) ?? []).map((r) => {
                  const hasElo = r.homeElo != null && r.awayElo != null;
                  const elo = hasElo
                    ? computeEloDelta(
                        r.homeElo!,
                        r.awayElo!,
                        r.homeScore,
                        r.awayScore
                      )
                    : null;

                  // Determine if result was an upset
                  const favored =
                    elo != null
                      ? elo.expectedHome > 0.5
                        ? "home"
                        : elo.expectedHome < 0.5
                          ? "away"
                          : "draw"
                      : null;
                  const isUpset =
                    elo != null &&
                    favored !== null &&
                    elo.result !== favored &&
                    Math.abs(elo.expectedHome - 0.5) > 0.1;

                  return (
                    <tr
                      key={r.matchupId}
                      className="border-b last:border-0 hover:bg-accent/40"
                    >
                      <td className="px-3 py-2">
                        <div className="font-medium">
                          {r.homeTeam}{" "}
                          <span className="text-muted-foreground">vs</span>{" "}
                          {r.awayTeam}
                        </div>
                        <div className="text-[10px] text-muted-foreground">
                          {fmtDate(r.gameDate)}
                        </div>
                      </td>
                      <td className="px-2 py-2 text-center font-bold tabular-nums">
                        {r.homeScore}–{r.awayScore}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-xs text-muted-foreground">
                        {r.homeElo != null ? Math.round(r.homeElo) : "—"} /{" "}
                        {r.awayElo != null ? Math.round(r.awayElo) : "—"}
                      </td>
                      <td className="px-2 py-2 text-center text-xs tabular-nums">
                        {r.vegasProbHome != null ? (
                          <span>
                            <span className="text-blue-400">
                              {fmtPct(r.vegasProbHome)}
                            </span>
                            {" / "}
                            <span className="text-gray-400">
                              {fmtPct(r.vegasProbDraw)}
                            </span>
                            {" / "}
                            <span className="text-rose-400">
                              {fmtPct(r.vegasProbAway)}
                            </span>
                          </span>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-2 py-2 text-center text-xs tabular-nums">
                        {r.ourProbHome != null ? (
                          <span>
                            <span className="text-blue-400">
                              {fmtPct(r.ourProbHome)}
                            </span>
                            {" / "}
                            <span className="text-gray-400">
                              {fmtPct(r.ourProbDraw)}
                            </span>
                            {" / "}
                            <span className="text-rose-400">
                              {fmtPct(r.ourProbAway)}
                            </span>
                          </span>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums text-xs">
                        {elo != null ? (
                          <span
                            className={
                              elo.delta > 0
                                ? "text-emerald-400"
                                : elo.delta < 0
                                  ? "text-rose-400"
                                  : "text-muted-foreground"
                            }
                          >
                            {elo.delta > 0 ? "+" : ""}
                            {elo.delta} home
                          </span>
                        ) : (
                          "—"
                        )}
                      </td>
                      <td className="px-2 py-2 text-center">
                        {elo != null ? (
                          isUpset ? (
                            <span className="rounded bg-amber-500/20 px-1.5 py-0.5 text-[10px] font-medium text-amber-400">
                              UPSET
                            </span>
                          ) : (
                            <span className="text-[10px] text-muted-foreground">
                              {elo.result === "home"
                                ? `${r.homeAbbrev ?? r.homeTeam} win`
                                : elo.result === "away"
                                  ? `${r.awayAbbrev ?? r.awayTeam} win`
                                  : "Draw"}
                            </span>
                          )
                        ) : null}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Futures tab ───────────────────────────────────────────────────────────────
function FuturesTab({ futures }: { futures: SoccerFuturesBetRow[] }) {
  const outright = futures.filter((f) => f.betType === "outright_winner");
  const groupWinner = futures.filter((f) => f.betType === "group_winner");

  // Group winner bets by group label
  const byGroup = new Map<string, SoccerFuturesBetRow[]>();
  for (const f of groupWinner) {
    const key = f.groupLabel ?? f.scope;
    const list = byGroup.get(key) ?? [];
    list.push(f);
    byGroup.set(key, list);
  }
  const groups = Array.from(byGroup.entries()).sort(([a], [b]) =>
    a.localeCompare(b)
  );

  return (
    <div className="space-y-6">
      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        <strong className="text-foreground">Our %</strong> is from the Monte
        Carlo tournament simulation (20k runs, Elo-driven bivariate Poisson
        match model).{" "}
        <strong className="text-foreground">Mkt %</strong> is the vig-removed
        consensus from The Odds API (4–5 books for outright; no market for group
        winner — stars use edge over 1/4 baseline). Green edge = we rate this
        team higher than the market.
      </p>

      {/* Outright winner */}
      {outright.length > 0 && (
        <div>
          <h2 className="mb-2 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Outright Winner
          </h2>
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[600px] text-sm">
              <thead>
                <tr className="border-b text-xs text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Team</th>
                  <th className="px-2 py-2 text-center font-medium">
                    Our %
                  </th>
                  <th className="px-2 py-2 text-center font-medium">
                    Market %
                  </th>
                  <th className="px-2 py-2 text-center font-medium">
                    Best Odds
                  </th>
                  <th className="px-2 py-2 text-center font-medium">Edge</th>
                  <th className="px-2 py-2 text-center font-medium">EV</th>
                  <th className="px-2 py-2 text-center font-medium">
                    Rating
                  </th>
                  <th className="px-2 py-2 text-center font-medium">
                    Status
                  </th>
                </tr>
              </thead>
              <tbody>
                {outright.map((f) => (
                  <tr
                    key={f.id}
                    className="border-b last:border-0 hover:bg-accent/40"
                  >
                    <td className="px-3 py-2 font-medium">
                      {f.selectionLabel}
                    </td>
                    <td className="px-2 py-2 text-center tabular-nums font-semibold">
                      {fmtPct1(f.ourProb)}
                    </td>
                    <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">
                      {f.marketProb != null ? fmtPct1(f.marketProb) : "—"}
                    </td>
                    <td className="px-2 py-2 text-center tabular-nums">
                      {fmtMl(f.marketOdds)}
                    </td>
                    <td className="px-2 py-2 text-center tabular-nums text-xs">
                      {f.edge != null ? (
                        <span
                          className={
                            f.edge > 0 ? "text-emerald-400" : "text-rose-400"
                          }
                        >
                          {fmtSigned(f.edge)}
                        </span>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="px-2 py-2 text-center tabular-nums text-xs">
                      {f.ev != null ? (
                        <span
                          className={
                            f.ev > 0 ? "text-emerald-400" : "text-muted-foreground"
                          }
                        >
                          {fmtSigned(f.ev)}
                        </span>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="px-2 py-2 text-center">
                      <Stars n={f.stars} />
                    </td>
                    <td className="px-2 py-2 text-center">
                      <span
                        className={`rounded px-1.5 py-0.5 text-[10px] font-medium uppercase ${
                          f.status === "won"
                            ? "bg-emerald-500/15 text-emerald-400"
                            : f.status === "lost"
                              ? "bg-rose-500/15 text-rose-400"
                              : "bg-muted text-muted-foreground"
                        }`}
                      >
                        {f.status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Group winner — per group */}
      {groups.length > 0 && (
        <div>
          <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Group Winner
          </h2>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {groups.map(([key, rows]) => {
              const sorted = [...rows].sort((a, b) => b.ourProb - a.ourProb);
              return (
                <div key={key} className="rounded-lg border bg-card">
                  <div className="border-b px-3 py-2 text-sm font-semibold">
                    Group {key}
                  </div>
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b text-[10px] text-muted-foreground">
                        <th className="px-3 py-1.5 text-left font-medium">
                          Team
                        </th>
                        <th className="px-2 py-1.5 text-center font-medium">
                          Our %
                        </th>
                        <th className="px-2 py-1.5 text-center font-medium">
                          Edge
                        </th>
                        <th className="px-2 py-1.5 text-center font-medium">
                          ★
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      {sorted.map((f) => (
                        <tr
                          key={f.id}
                          className="border-b last:border-0 hover:bg-accent/40"
                        >
                          <td className="px-3 py-1.5 font-medium text-sm">
                            {f.selectionLabel}
                          </td>
                          <td className="px-2 py-1.5 text-center tabular-nums font-semibold">
                            {fmtPct1(f.ourProb)}
                          </td>
                          <td className="px-2 py-1.5 text-center tabular-nums text-xs">
                            {f.edge != null ? (
                              <span
                                className={
                                  f.edge > 0
                                    ? "text-emerald-400"
                                    : "text-rose-400"
                                }
                              >
                                {fmtSigned(f.edge)}
                              </span>
                            ) : (
                              "—"
                            )}
                          </td>
                          <td className="px-2 py-1.5 text-center">
                            <Stars n={f.stars} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              );
            })}
          </div>
          <p className="mt-1 text-[11px] text-muted-foreground">
            Group winner bets have no market line — stars use edge vs the 25%
            baseline (each team has equal 1/4 prior). Settled from final
            standings.
          </p>
        </div>
      )}

      {futures.length === 0 && (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No futures bets rated yet. Run{" "}
          <code className="rounded bg-muted px-1 py-0.5">
            python -m model.soccer_futures
          </code>{" "}
          to generate outright and group winner probabilities.
        </div>
      )}
    </div>
  );
}

// ── Root component ────────────────────────────────────────────────────────────
type Tab = "rankings" | "results" | "futures";

const TABS: { id: Tab; label: string }[] = [
  { id: "rankings", label: "📊 Power Rankings" },
  { id: "results", label: "📋 Results & Elo Impact" },
  { id: "futures", label: "🏆 Futures" },
];

export default function EloClient({
  teams,
  results,
  futures,
}: {
  teams: SoccerEloTeamRow[];
  results: SoccerCompletedResultRow[];
  futures: SoccerFuturesBetRow[];
}) {
  const [tab, setTab] = useState<Tab>("rankings");

  const topElo = [...teams].sort((a, b) => b.elo - a.elo)[0];
  const completedGames = results.length;
  const highStarFutures = futures.filter((f) => f.stars >= 4).length;

  return (
    <div className="mx-auto max-w-6xl space-y-4 p-6">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-bold">
            ⚽ WC 2026 — Elo Power Rankings
          </h1>
          <p className="text-xs text-muted-foreground mt-0.5">
            Bivariate Poisson model · Retrained every 3h from live scores
          </p>
        </div>
        <div className="flex gap-4 text-xs text-muted-foreground">
          {topElo && (
            <span>
              Top ranked:{" "}
              <span className="font-medium text-foreground">{topElo.name}</span>{" "}
              ({Math.round(topElo.elo)} Elo)
            </span>
          )}
          <span>
            {completedGames} results in
          </span>
          {highStarFutures > 0 && (
            <span className="text-amber-400 font-medium">
              {highStarFutures} 4-5★ futures
            </span>
          )}
        </div>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b">
        {TABS.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`px-4 py-2 text-sm font-medium transition-colors ${
              tab === t.id
                ? "border-b-2 border-primary text-foreground"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === "rankings" && <RankingsTab teams={teams} />}
      {tab === "results" && <ResultsTab results={results} />}
      {tab === "futures" && <FuturesTab futures={futures} />}
    </div>
  );
}
