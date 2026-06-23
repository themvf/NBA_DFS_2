"use client";

import { useState } from "react";
import type {
  SoccerEloTeamRow,
  SoccerCompletedResultRow,
  SoccerFuturesBetRow,
  SoccerGroupStandingRow,
  SoccerGroupFixtureRow,
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

// ── Group clinch computation ──────────────────────────────────────────────────
// Enumerates all 3^N outcomes of remaining intra-group fixtures.
// Tiebreaker: pts → GD → GF (conservative; no scoreline data for future games).

type ResultKind = 'always' | 'sometimes' | 'never';

type ClinchStatus = {
  // R16 (top-2) advancement
  advanceIn: number;
  total: number;
  // "To win the group" — per-result-in-their-next-game
  nextFxOpponentAbbr: string | null;
  nextFxOpponentName: string | null;
  winFirst: ResultKind;
  drawFirst: ResultKind;
  lossFirst: ResultKind;
  // For N=2 (final matchday): plain-English condition for each 'sometimes' result
  // e.g. "if Morocco doesn't win" — what the other game needs to be
  winCondition: string | null;
  drawCondition: string | null;
  lossCondition: string | null;
};

// Describes which outcome of the OTHER game a team needs, given N=2 simultaneous games.
function _describeOtherCondition(
  worksWhen: Set<number>, // raw other-game outcomes (0=home win,1=draw,2=away win) that allow group win
  otherFx: SoccerGroupFixtureRow,
): string | null {
  if (worksWhen.size === 0 || worksWhen.size === 3) return null;
  const h = otherFx.homeAbbr ?? otherFx.homeName;
  const a = otherFx.awayAbbr ?? otherFx.awayName;
  const hw = worksWhen.has(0), d = worksWhen.has(1), aw = worksWhen.has(2);
  if (hw && d && !aw) return `if ${a} doesn't win`;
  if (!hw && d && aw) return `if ${h} doesn't win`;
  if (hw && !d && !aw) return `if ${h} wins`;
  if (!hw && !d && aw) return `if ${a} wins`;
  if (!hw && d && !aw) return `if ${h} draws ${a}`;
  if (hw && !d && aw)  return `if ${h} vs ${a} isn't a draw`;
  return null;
}

function _rankTeams(
  rows: SoccerGroupStandingRow[],
  pts: Map<number, number>,
  gd: Map<number, number>,
  gf: Map<number, number>,
): number[] {
  return [...rows]
    .sort((a, b) => {
      const dp = (pts.get(b.teamId) ?? 0) - (pts.get(a.teamId) ?? 0);
      if (dp !== 0) return dp;
      const dg = (gd.get(b.teamId) ?? 0) - (gd.get(a.teamId) ?? 0);
      if (dg !== 0) return dg;
      return (gf.get(b.teamId) ?? 0) - (gf.get(a.teamId) ?? 0);
    })
    .map((r) => r.teamId);
}

function computeGroupClinch(
  rows: SoccerGroupStandingRow[],
  fxs: SoccerGroupFixtureRow[],
): Map<number, ClinchStatus> {
  const N = fxs.length;
  const total = Math.pow(3, N);

  // Per-team accumulators
  const advanceIn = new Map(rows.map((s) => [s.teamId, 0]));
  type Bucket = [number, number];
  const winB  = new Map<number, Bucket>(rows.map((s) => [s.teamId, [0, 0]]));
  const drawB = new Map<number, Bucket>(rows.map((s) => [s.teamId, [0, 0]]));
  const lossB = new Map<number, Bucket>(rows.map((s) => [s.teamId, [0, 0]]));
  // For N=2: track which other-game outcomes allow group win, per my result
  const winWhen  = new Map<number, Set<number>>(rows.map((s) => [s.teamId, new Set()]));
  const drawWhen = new Map<number, Set<number>>(rows.map((s) => [s.teamId, new Set()]));
  const lossWhen = new Map<number, Set<number>>(rows.map((s) => [s.teamId, new Set()]));

  // For each team, find their next fixture index
  const nextFxIdx = new Map<number, number>();
  for (const s of rows) {
    const idx = fxs.findIndex((f) => f.homeTeamId === s.teamId || f.awayTeamId === s.teamId);
    if (idx !== -1) nextFxIdx.set(s.teamId, idx);
  }

  for (let mask = 0; mask < total; mask++) {
    const pts = new Map(rows.map((s) => [s.teamId, s.pts]));
    const gd  = new Map(rows.map((s) => [s.teamId, s.gd]));
    const gf  = new Map(rows.map((s) => [s.teamId, s.gf]));
    const outcomes: number[] = [];
    let m = mask;
    for (let i = 0; i < N; i++) {
      const o = m % 3; m = Math.floor(m / 3); outcomes.push(o);
      const fx = fxs[i];
      if (o === 0)      { pts.set(fx.homeTeamId, (pts.get(fx.homeTeamId) ?? 0) + 3); }
      else if (o === 1) { pts.set(fx.homeTeamId, (pts.get(fx.homeTeamId) ?? 0) + 1); pts.set(fx.awayTeamId, (pts.get(fx.awayTeamId) ?? 0) + 1); }
      else              { pts.set(fx.awayTeamId, (pts.get(fx.awayTeamId) ?? 0) + 3); }
    }
    const ranked = _rankTeams(rows, pts, gd, gf);
    const topId = ranked[0];

    for (const s of rows) {
      const rank = ranked.indexOf(s.teamId);
      // Same tie logic: if pts-tied with the 2nd place team, count as potentially advancing.
      const rank1Id = ranked[1];
      const tiedWithSecond = rank >= 2 && rank1Id != null &&
        (pts.get(s.teamId) ?? 0) === (pts.get(rank1Id) ?? 0);
      if (rank < 2 || tiedWithSecond) advanceIn.set(s.teamId, (advanceIn.get(s.teamId) ?? 0) + 1);
      // Also count as "wins group" when tied with the leader on pts: GD and GF are
      // frozen at current values in this simulation (future scorelines not modeled),
      // so a pts tie is sufficient — the team could pull ahead on GD/GF or win via
      // H2H tiebreakers depending on how the actual games play out.
      const tiedWithTop = rank > 0 &&
        (pts.get(s.teamId) ?? 0) === (pts.get(topId) ?? 0);
      const winsGroup = rank === 0 || tiedWithTop;

      const fi = nextFxIdx.get(s.teamId);
      if (fi !== undefined) {
        const o = outcomes[fi];
        const fx = fxs[fi];
        const isHome = fx.homeTeamId === s.teamId;
        const myResult: 'win' | 'draw' | 'loss' =
          o === 1 ? 'draw' : (o === 0) === isHome ? 'win' : 'loss';
        const b = myResult === 'win' ? winB : myResult === 'draw' ? drawB : lossB;
        const bucket = b.get(s.teamId)!;
        bucket[0]++;
        if (winsGroup) {
          bucket[1]++;
          // Track other-game outcome for condition description (N=2 only)
          if (N === 2) {
            const otherFxIdx = 1 - fi;
            const when = myResult === 'win' ? winWhen : myResult === 'draw' ? drawWhen : lossWhen;
            when.get(s.teamId)!.add(outcomes[otherFxIdx]);
          }
        }
      }
    }
  }

  const classify = ([n, w]: Bucket): ResultKind =>
    n === 0 ? 'never' : w === n ? 'always' : w > 0 ? 'sometimes' : 'never';

  const result = new Map<number, ClinchStatus>();
  for (const s of rows) {
    const fi = nextFxIdx.get(s.teamId);
    const fx = fi !== undefined ? fxs[fi] : undefined;
    const isHome = fx ? fx.homeTeamId === s.teamId : false;
    const otherFx = (N === 2 && fi !== undefined) ? fxs[1 - fi] : undefined;
    const wF = classify(winB.get(s.teamId)  ?? [0, 0]);
    const dF = classify(drawB.get(s.teamId) ?? [0, 0]);
    const lF = classify(lossB.get(s.teamId) ?? [0, 0]);
    result.set(s.teamId, {
      advanceIn: advanceIn.get(s.teamId) ?? 0,
      total,
      nextFxOpponentAbbr: fx ? (isHome ? fx.awayAbbr : fx.homeAbbr) : null,
      nextFxOpponentName: fx ? (isHome ? fx.awayName : fx.homeName) : null,
      winFirst:  wF,
      drawFirst: dF,
      lossFirst: lF,
      winCondition:  (wF  === 'sometimes' && otherFx) ? _describeOtherCondition(winWhen.get(s.teamId)!,  otherFx) : null,
      drawCondition: (dF  === 'sometimes' && otherFx) ? _describeOtherCondition(drawWhen.get(s.teamId)!, otherFx) : null,
      lossCondition: (lF  === 'sometimes' && otherFx) ? _describeOtherCondition(lossWhen.get(s.teamId)!, otherFx) : null,
    });
  }
  return result;
}

// ── Futures tab ───────────────────────────────────────────────────────────────
function FuturesTab({
  futures,
  standings,
  fixtures,
  teams,
}: {
  futures: SoccerFuturesBetRow[];
  standings: SoccerGroupStandingRow[];
  fixtures: SoccerGroupFixtureRow[];
  teams: SoccerEloTeamRow[];
}) {
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

  // Standings lookup: groupLabel → rows already sorted pts/gd/gf
  const standingsByGroup = new Map<string, SoccerGroupStandingRow[]>();
  for (const s of standings) {
    const list = standingsByGroup.get(s.groupLabel) ?? [];
    list.push(s);
    standingsByGroup.set(s.groupLabel, list);
  }

  // Remaining fixtures lookup: groupLabel → upcoming intra-group games
  const fixturesByGroup = new Map<string, SoccerGroupFixtureRow[]>();
  for (const f of fixtures) {
    const list = fixturesByGroup.get(f.groupLabel) ?? [];
    list.push(f);
    fixturesByGroup.set(f.groupLabel, list);
  }

  // Elo lookup: teamId → elo rating
  const eloByTeam = new Map<number, number>();
  for (const t of teams) eloByTeam.set(t.teamId, Math.round(t.elo));

  return (
    <div className="space-y-6">
      <p className="rounded-lg border bg-card p-3 text-xs text-muted-foreground">
        <strong className="text-foreground">Our %</strong> is from the Monte
        Carlo tournament simulation (20k runs, Elo-driven bivariate Poisson
        match model), anchored 30% model / 70% market for group winner and 35%
        model / 65% market for outright.{" "}
        <strong className="text-foreground">Mkt %</strong> is vig-free: outright
        from The Odds API consensus (4–5 books); group winner from{" "}
        <strong className="text-foreground">Pinnacle</strong> (sharpest line,
        ~3% vig removed multiplicatively). Green edge = we like this team more
        than the market.
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
          <h2 className="mb-1 text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Group Winner
          </h2>
          <p className="mb-3 text-[11px] text-muted-foreground">
            <span className="text-emerald-500">▲</span> = current group leader · 🏆 = group won ·{" "}
            <span className="text-amber-400 font-semibold">≈ CLOSE</span> = top two teams within 10pp (too close to call).
            No group is mathematically decided until the final round is played.
          </p>
          <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
            {groups.map(([key, rows]) => {
              const sorted = [...rows].sort((a, b) => b.ourProb - a.ourProb);
              const top2Gap =
                sorted.length >= 2
                  ? sorted[0].ourProb - sorted[1].ourProb
                  : 1;
              const contested = !sorted[0]?.wonGroup && top2Gap <= 0.10;
              const grpStandings = standingsByGroup.get(key) ?? [];
              const grpFixtures  = fixturesByGroup.get(key) ?? [];
              const clinch = grpFixtures.length > 0
                ? computeGroupClinch(grpStandings, grpFixtures)
                : new Map<number, ClinchStatus>();
              return (
                <div key={key} className="rounded-lg border bg-card">
                  <div className="border-b px-3 py-2 text-sm font-semibold flex items-center gap-2">
                    Group {key}
                    {contested && (
                      <span
                        className="rounded px-1.5 py-0.5 text-[10px] font-semibold bg-amber-500/15 text-amber-400"
                        title={`Top two teams within ${(top2Gap * 100).toFixed(0)}pp — group too close to call`}
                      >
                        ≈ CLOSE
                      </span>
                    )}
                  </div>
                  <table className="w-full table-fixed text-sm">
                    <thead>
                      <tr className="border-b text-[10px] text-muted-foreground">
                        <th className="w-[28%] px-3 py-1.5 text-left font-medium">
                          Team
                        </th>
                        <th className="px-1 py-1.5 text-center font-medium">
                          Our %
                        </th>
                        <th className="px-1 py-1.5 text-center font-medium">
                          Mkt %
                        </th>
                        <th className="px-1 py-1.5 text-center font-medium">
                          Pinnacle
                        </th>
                        <th className="px-1 py-1.5 text-center font-medium">
                          Edge
                        </th>
                        <th className="px-1 py-1.5 text-center font-medium">
                          EV
                        </th>
                        <th className="w-[12%] px-1 py-1.5 text-center font-medium">
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
                          <td className="px-3 py-1.5 font-medium text-sm truncate">
                            {f.wonGroup ? (
                              <span className="mr-1" title="Won the group">🏆</span>
                            ) : f.isLeader ? (
                              <span
                                className="mr-1 text-emerald-500"
                                title={`Current group leader${f.groupPts != null ? ` — ${f.groupPts} pts` : ""}`}
                              >
                                ▲
                              </span>
                            ) : null}
                            {f.selectionLabel}
                          </td>
                          <td className="px-1 py-1.5 text-center tabular-nums font-semibold">
                            {fmtPct1(f.ourProb)}
                          </td>
                          <td className="px-1 py-1.5 text-center tabular-nums text-xs text-muted-foreground">
                            {f.marketProb != null ? fmtPct1(f.marketProb) : "—"}
                          </td>
                          <td className="px-1 py-1.5 text-center tabular-nums text-xs">
                            {fmtMl(f.marketOdds)}
                          </td>
                          <td className="px-1 py-1.5 text-center tabular-nums text-xs">
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
                          <td className="px-1 py-1.5 text-center tabular-nums text-xs">
                            {f.ev != null ? (
                              <span
                                className={
                                  f.ev > 0
                                    ? "text-emerald-400"
                                    : "text-muted-foreground"
                                }
                              >
                                {fmtSigned(f.ev)}
                              </span>
                            ) : (
                              "—"
                            )}
                          </td>
                          <td className="px-1 py-1.5 text-center">
                            <Stars n={f.stars} />
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {/* Remaining fixtures */}
                  {(fixturesByGroup.get(key) ?? []).length > 0 && (
                    <div className="border-t px-3 py-2 space-y-1">
                      <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide mb-1">
                        Still to play
                      </p>
                      {(fixturesByGroup.get(key) ?? []).map((f) => {
                        const home = f.homeAbbr ?? f.homeName;
                        const away = f.awayAbbr ?? f.awayName;
                        // Format date as "Jun 25" in local time from commence_time if available
                        const dateStr = f.commenceTime
                          ? new Date(f.commenceTime).toLocaleDateString("en-US", { month: "short", day: "numeric" })
                          : f.gameDate.slice(5).replace("-", "/");
                        // Points for each team from standings
                        const homePts = grpStandings.find((s) => (s.abbreviation ?? s.name) === home)?.pts;
                        const awayPts = grpStandings.find((s) => (s.abbreviation ?? s.name) === away)?.pts;
                        return (
                          <div key={f.matchupId} className="flex items-center justify-between text-[11px]">
                            <span className="text-muted-foreground">{dateStr}</span>
                            <span className="font-medium">
                              {home}
                              {homePts != null && (
                                <span className="text-muted-foreground ml-0.5">({homePts})</span>
                              )}
                              {" vs "}
                              {away}
                              {awayPts != null && (
                                <span className="text-muted-foreground ml-0.5">({awayPts})</span>
                              )}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  )}

                  {/* Standings mini-table */}
                  {grpStandings.length > 0 && (
                    <div className="border-t">
                      <table className="w-full table-fixed text-[11px]">
                        <thead>
                          <tr className="border-b text-[10px] text-muted-foreground bg-muted/30">
                            <th className="px-3 py-1 text-left font-medium w-[34%]">Standings</th>
                            <th className="px-1 py-1 text-center font-medium">GP</th>
                            <th className="px-1 py-1 text-center font-medium">W</th>
                            <th className="px-1 py-1 text-center font-medium">D</th>
                            <th className="px-1 py-1 text-center font-medium">L</th>
                            <th className="px-1 py-1 text-center font-medium">GD</th>
                            <th className="px-1 py-1 text-center font-medium font-semibold">P</th>
                            <th className="px-1 py-1 text-center font-medium text-violet-400">Elo</th>
                          </tr>
                        </thead>
                        <tbody>
                          {grpStandings.map((s, i) => {
                            const elo = eloByTeam.get(s.teamId);
                            const c = clinch.get(s.teamId);
                            const advClinched = c && c.advanceIn === c.total;
                            const eliminated  = c && c.advanceIn === 0;
                            return (
                              <tr key={s.teamId} className="border-b last:border-0">
                                <td className="px-3 py-1 truncate text-muted-foreground">
                                  <span className="mr-1 text-muted-foreground/50">{i + 1}</span>
                                  {s.abbreviation ?? s.name}
                                  {advClinched && (
                                    <span className="ml-1 text-[9px] font-semibold text-emerald-400">✓R16</span>
                                  )}
                                  {eliminated && (
                                    <span className="ml-1 text-[9px] font-semibold text-rose-400">OUT</span>
                                  )}
                                </td>
                                <td className="px-1 py-1 text-center tabular-nums text-muted-foreground">{s.gp}</td>
                                <td className="px-1 py-1 text-center tabular-nums text-muted-foreground">{s.w}</td>
                                <td className="px-1 py-1 text-center tabular-nums text-muted-foreground">{s.d}</td>
                                <td className="px-1 py-1 text-center tabular-nums text-muted-foreground">{s.l}</td>
                                <td className={`px-1 py-1 text-center tabular-nums ${s.gd > 0 ? "text-emerald-400" : s.gd < 0 ? "text-rose-400" : "text-muted-foreground"}`}>
                                  {s.gd > 0 ? `+${s.gd}` : s.gd}
                                </td>
                                <td className="px-1 py-1 text-center tabular-nums font-semibold">{s.pts}</td>
                                <td className="px-1 py-1 text-center tabular-nums text-violet-400 text-[10px]">
                                  {elo ?? "—"}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                      {/* To win the group — only guaranteed paths + eliminated teams */}
                      {(() => {
                        type RowData = { s: SoccerGroupStandingRow; c: ClinchStatus; guaranteed: string[]; conditional: { result: string; condition: string | null }[]; impossible: boolean };
                        const rows = grpStandings.map((s): RowData | null => {
                          const c = clinch.get(s.teamId);
                          if (!c) return null;
                          const guaranteed: string[] = [];
                          if (c.winFirst  === 'always') guaranteed.push('Win');
                          if (c.drawFirst === 'always') guaranteed.push('Draw');
                          if (c.lossFirst === 'always') guaranteed.push('Loss');
                          const conditional: { result: string; condition: string | null }[] = [];
                          if (c.winFirst  === 'sometimes') conditional.push({ result: 'Win',  condition: c.winCondition });
                          if (c.drawFirst === 'sometimes') conditional.push({ result: 'Draw', condition: c.drawCondition });
                          if (c.lossFirst === 'sometimes') conditional.push({ result: 'Loss', condition: c.lossCondition });
                          const impossible = c.winFirst === 'never' && c.drawFirst === 'never' && c.lossFirst === 'never';
                          return { s, c, guaranteed, conditional, impossible };
                        }).filter(Boolean) as RowData[];

                        // Only show when there's something meaningful: guaranteed path, conditional path with known condition, or eliminated
                        const hasContent = rows.some((r) =>
                          r.guaranteed.length > 0 ||
                          r.conditional.some((p) => p.condition !== null) ||
                          r.impossible
                        );
                        const tooEarly = !hasContent && grpFixtures.length > 2;

                        if (!hasContent && !tooEarly) return null;

                        return (
                          <div className="border-t px-3 py-2 space-y-1.5">
                            <p className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide mb-1.5">
                              To win the group
                            </p>
                            {tooEarly && (
                              <p className="text-[11px] text-muted-foreground/50 italic">Too early to call — check back on the final matchday.</p>
                            )}
                            {rows.map(({ s, c, guaranteed, conditional, impossible }) => {
                              const name = s.abbreviation ?? s.name;
                              const opp  = c.nextFxOpponentAbbr ?? c.nextFxOpponentName ?? "?";
                              const showConditional = conditional.filter((p) => p.condition !== null);
                              if (!guaranteed.length && !showConditional.length && !impossible) return null;
                              return (
                                <div key={s.teamId} className="flex gap-2 text-[11px] items-start">
                                  <span className="font-medium w-8 shrink-0 text-foreground pt-px">{name}</span>
                                  {impossible ? (
                                    <span className="text-muted-foreground/50">Cannot win group</span>
                                  ) : (
                                    <span className="flex flex-col gap-0.5">
                                      {guaranteed.length > 0 && (
                                        <span>
                                          <span className="text-emerald-400 font-semibold">{guaranteed.join(' or ')} vs {opp}</span>
                                          <span className="text-muted-foreground"> → clinches 1st</span>
                                        </span>
                                      )}
                                      {showConditional.map((p) => (
                                        <span key={p.result}>
                                          <span className="text-amber-400 font-semibold">{p.result} vs {opp}</span>
                                          <span className="text-muted-foreground"> → 1st {p.condition}</span>
                                        </span>
                                      ))}
                                    </span>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        );
                      })()}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
          <p className="mt-1 text-[11px] text-muted-foreground">
            Group winner market from Pinnacle (vig-free). EV uses Pinnacle
            price; DraftKings prices may differ. Settled from final group
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
  standings,
  fixtures,
}: {
  teams: SoccerEloTeamRow[];
  results: SoccerCompletedResultRow[];
  futures: SoccerFuturesBetRow[];
  standings: SoccerGroupStandingRow[];
  fixtures: SoccerGroupFixtureRow[];
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
      {tab === "futures" && <FuturesTab futures={futures} standings={standings} fixtures={fixtures} teams={teams} />}
    </div>
  );
}
