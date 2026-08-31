"use client";

/**
 * NFL survivor pool grid and path optimizer.
 *
 * Two things this page does that the popular free grids do not, both
 * deliberate:
 *
 *  1. It marks every cell MARKET or MODEL. Most of the season has no quoted
 *     line yet, so most of any full-season grid is model output; rendering
 *     those cells identically to quoted ones is the central dishonesty of the
 *     genre.
 *  2. It shows PATH SURVIVAL -- the product of the chosen path's
 *     probabilities -- next to the average. Eighteen 75% picks average 75%
 *     and survive 0.6% of the time. The average alone is the flattering
 *     number and the useless one.
 *
 * Picks are stored in this browser only. There is no pool ledger yet, and the
 * page says so rather than implying your picks are recorded anywhere.
 */

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Ban,
  Check,
  Download,
  Info,
  Lock,
  RotateCcw,
  Search,
  Sparkles,
} from "lucide-react";
import type { SurvivorCell, SurvivorGrid } from "@/db/queries";
import { evaluateWeek, futureSurvivorValue, solveSurvivor } from "@/lib/nfl/survivor-assignment";

type Props = { grid: SurvivorGrid; loadedAt: string };

type SortMode = "week" | "fsv" | "alpha";
type TieRule = "tie_loses" | "tie_survives";

const STORAGE_KEY = "nfl-survivor-v1";

function pct(value: number | null | undefined, digits = 0): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${(value * 100).toFixed(digits)}%`;
}

function spreadLabel(cell: SurvivorCell): string {
  if (cell.spread == null) return "—";
  // Stored home-perspective-positive; a survivor player reads their own team's
  // number book-style, so a favorite shows negative.
  const own = -cell.spread;
  return own > 0 ? `+${own.toFixed(1)}` : own.toFixed(1);
}

/** Green ramp for win probability. Quoted and modeled cells use different families. */
function cellTone(cell: SurvivorCell | undefined): string {
  if (!cell) return "bg-muted/40 text-muted-foreground";
  const p = cell.pWin;
  if (p == null) return "bg-muted/40 text-muted-foreground";
  const modeled = cell.provenance === "model_spread";
  const steps: Array<[number, string, string]> = [
    [0.80, "bg-emerald-500/45", "bg-sky-500/30"],
    [0.72, "bg-emerald-500/32", "bg-sky-500/22"],
    [0.65, "bg-emerald-500/22", "bg-sky-500/15"],
    [0.58, "bg-emerald-500/12", "bg-sky-500/9"],
    [0.50, "bg-transparent", "bg-transparent"],
  ];
  for (const [floor, market, model] of steps) {
    if (p >= floor) return modeled ? model : market;
  }
  return "bg-rose-500/10";
}

export default function SurvivorClient({ grid, loadedAt }: Props) {
  const { weeks, teams, anchorWeek } = grid;

  const [used, setUsed] = useState<Record<number, number>>({}); // week -> teamId
  const [banned, setBanned] = useState<number[]>([]);
  const [startWeek, setStartWeek] = useState<number>(weeks[0] ?? 1);
  const [endWeek, setEndWeek] = useState<number>(weeks[weeks.length - 1] ?? 18);
  const [tieRule, setTieRule] = useState<TieRule>("tie_loses");
  const [sortMode, setSortMode] = useState<SortMode>("week");
  const [query, setQuery] = useState("");
  const [optimized, setOptimized] = useState<Record<number, number> | null>(null);
  const [hydrated, setHydrated] = useState(false);

  // ── local persistence ─────────────────────────────────────────────
  // Restored after first paint (same deferred pattern as the Best Ball room)
  // so the server-rendered markup and the first client render agree, and so
  // no state is set synchronously inside the effect body.
  useEffect(() => {
    const storageKey = `${STORAGE_KEY}:${grid.season}`;
    const restore = (raw: string | null) => {
      try {
        if (raw) {
          const saved = JSON.parse(raw) as {
            used?: Record<string, number>;
            banned?: number[];
            startWeek?: number;
            endWeek?: number;
            tieRule?: TieRule;
          };
          if (saved.used) {
            setUsed(
              Object.fromEntries(Object.entries(saved.used).map(([week, id]) => [Number(week), id])),
            );
          }
          if (Array.isArray(saved.banned)) setBanned(saved.banned);
          if (saved.startWeek) setStartWeek(saved.startWeek);
          if (saved.endWeek) setEndWeek(saved.endWeek);
          if (saved.tieRule) setTieRule(saved.tieRule);
        }
      } catch {
        // A blocked, empty, or corrupt store is a normal state, not an error.
      }
      setHydrated(true);
    };

    const initialLoad = window.setTimeout(() => {
      restore(window.localStorage.getItem(storageKey));
    }, 0);
    const syncFromAnotherTab = (event: StorageEvent) => {
      if (event.key === storageKey) restore(event.newValue);
    };
    window.addEventListener("storage", syncFromAnotherTab);
    return () => {
      window.clearTimeout(initialLoad);
      window.removeEventListener("storage", syncFromAnotherTab);
    };
  }, [grid.season]);

  useEffect(() => {
    if (!hydrated) return;
    try {
      window.localStorage.setItem(
        `${STORAGE_KEY}:${grid.season}`,
        JSON.stringify({ used, banned, startWeek, endWeek, tieRule }),
      );
    } catch {
      // Ignore: persistence is a convenience, never a requirement.
    }
  }, [hydrated, used, banned, startWeek, endWeek, tieRule, grid.season]);

  // ── derived state ─────────────────────────────────────────────────
  const teamIndex = useMemo(
    () => new Map(teams.map((team, index) => [team.teamId, index])),
    [teams],
  );

  const planWeeks = useMemo(
    () => weeks.filter((week) => week >= startWeek && week <= endWeek && !(week in used)),
    [weeks, startWeek, endWeek, used],
  );

  const advanceProb = useCallback(
    (cell: SurvivorCell | undefined): number | null => {
      if (!cell || cell.pWin == null) return null;
      return tieRule === "tie_survives" ? Math.min(1, cell.pWin + (cell.pTie ?? 0)) : cell.pWin;
    },
    [tieRule],
  );

  /** probs[planWeekIndex][teamIndex] */
  const probMatrix = useMemo(
    () =>
      planWeeks.map((week) =>
        teams.map((team) => advanceProb(team.cells[week])),
      ),
    [planWeeks, teams, advanceProb],
  );

  const consumed = useMemo(() => {
    const set = new Set<number>();
    for (const teamId of Object.values(used)) {
      const index = teamIndex.get(teamId);
      if (index !== undefined) set.add(index);
    }
    for (const teamId of banned) {
      const index = teamIndex.get(teamId);
      if (index !== undefined) set.add(index);
    }
    return set;
  }, [used, banned, teamIndex]);

  const solution = useMemo(
    () => solveSurvivor(probMatrix, { bannedTeams: consumed }),
    [probMatrix, consumed],
  );

  const fsv = useMemo(
    () => futureSurvivorValue(probMatrix, { bannedTeams: consumed }).fsv,
    [probMatrix, consumed],
  );

  const currentWeek = planWeeks[0] ?? null;

  const weekPicks = useMemo(() => {
    if (currentWeek === null) return [];
    return evaluateWeek(probMatrix, 0, { bannedTeams: consumed }).picks.slice(0, 6);
  }, [probMatrix, consumed, currentWeek]);

  // Picks already committed, plus whatever the optimizer proposed.
  const pathByWeek = useMemo(() => {
    const map: Record<number, { teamId: number; committed: boolean }> = {};
    for (const [week, teamId] of Object.entries(used)) {
      map[Number(week)] = { teamId, committed: true };
    }
    if (optimized) {
      for (const [week, teamId] of Object.entries(optimized)) {
        if (!(Number(week) in map)) map[Number(week)] = { teamId, committed: false };
      }
    }
    return map;
  }, [used, optimized]);

  const committedProbs = useMemo(
    () =>
      Object.entries(used)
        .map(([week, teamId]) => {
          const team = teams.find((entry) => entry.teamId === teamId);
          return advanceProb(team?.cells[Number(week)]);
        })
        .filter((value): value is number => value != null),
    [used, teams, advanceProb],
  );

  const plannedProbs = useMemo(() => {
    if (!optimized) return [];
    return Object.entries(optimized)
      .map(([week, teamId]) => {
        const team = teams.find((entry) => entry.teamId === teamId);
        return advanceProb(team?.cells[Number(week)]);
      })
      .filter((value): value is number => value != null);
  }, [optimized, teams, advanceProb]);

  const allProbs = [...committedProbs, ...plannedProbs];
  const avgWin = allProbs.length
    ? allProbs.reduce((sum, value) => sum + value, 0) / allProbs.length
    : null;
  const pathSurvival = allProbs.length
    ? allProbs.reduce((product, value) => product * value, 1)
    : null;

  const runOptimize = useCallback(() => {
    if (!solution.feasible) {
      setOptimized(null);
      return;
    }
    const next: Record<number, number> = {};
    solution.assignment.forEach((index, position) => {
      if (index == null) return;
      next[planWeeks[position]] = teams[index].teamId;
    });
    setOptimized(next);
  }, [solution, planWeeks, teams]);

  const commitWeek = (week: number, teamId: number) => {
    setUsed((prev) => ({ ...prev, [week]: teamId }));
    setOptimized(null);
  };

  const clearWeek = (week: number) => {
    setUsed((prev) => {
      const next = { ...prev };
      delete next[week];
      return next;
    });
    setOptimized(null);
  };

  const toggleBan = (teamId: number) => {
    setBanned((prev) =>
      prev.includes(teamId) ? prev.filter((id) => id !== teamId) : [...prev, teamId],
    );
    setOptimized(null);
  };

  const resetAll = () => {
    setUsed({});
    setBanned([]);
    setOptimized(null);
  };

  const exportCsv = () => {
    const header = ["team", ...weeks.map((week) => `W${week}`)].join(",");
    const lines = teams.map((team) =>
      [
        team.abbrev,
        ...weeks.map((week) => {
          const cell = team.cells[week];
          if (!cell) return "BYE";
          const probability = advanceProb(cell);
          return `${cell.isHome ? "" : "@"}${cell.opponent} ${
            probability == null ? "" : (probability * 100).toFixed(1)
          }${cell.provenance === "model_spread" ? "M" : ""}`;
        }),
      ].join(","),
    );
    const blob = new Blob([[header, ...lines].join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `nfl-survivor-${grid.season}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  };

  // ── ordering ──────────────────────────────────────────────────────
  const visibleTeams = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const filtered = needle
      ? teams.filter(
          (team) =>
            team.abbrev.toLowerCase().includes(needle) ||
            team.name.toLowerCase().includes(needle),
        )
      : teams;
    const ordered = [...filtered];
    if (sortMode === "alpha") {
      ordered.sort((a, b) => a.abbrev.localeCompare(b.abbrev));
    } else if (sortMode === "fsv") {
      ordered.sort((a, b) => (fsv[teamIndex.get(b.teamId)!] ?? 0) - (fsv[teamIndex.get(a.teamId)!] ?? 0));
    } else {
      ordered.sort((a, b) => {
        const left = currentWeek === null ? null : advanceProb(a.cells[currentWeek]);
        const right = currentWeek === null ? null : advanceProb(b.cells[currentWeek]);
        return (right ?? -1) - (left ?? -1);
      });
    }
    return ordered;
  }, [teams, query, sortMode, fsv, teamIndex, currentWeek, advanceProb]);

  const modelCells = grid.provenanceCounts.model_spread ?? 0;
  const marketCells =
    (grid.provenanceCounts.market_ml_novig ?? 0) + (grid.provenanceCounts.market_spread ?? 0);
  const far = grid.calibration.find((row) => row.horizon === 10) ?? grid.calibration.at(-1);

  return (
    <div className="mx-auto max-w-[1600px] space-y-4 p-4">
      {/* ── header ─────────────────────────────────────────────── */}
      <header className="space-y-2">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-bold tracking-tight">Survivor Pool</h1>
          <span className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
            {grid.season} season
          </span>
          <span className="rounded border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-amber-600 dark:text-amber-400">
            Picks saved in this browser only
          </span>
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Every team&rsquo;s win probability for every week, and an{" "}
          <strong className="text-foreground">exact</strong> optimizer that plans the whole
          season under the use-each-team-once rule. Probabilities come from the market where
          the market has priced the game, and from market-implied power ratings where it has
          not — those cells are marked, never blended in silently.
        </p>
      </header>

      {/* ── controls ───────────────────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-2 rounded-lg border bg-card p-3">
        <div className="relative">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search teams"
            className="h-8 w-52 rounded border bg-background pl-7 pr-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-emerald-500"
          />
        </div>

        <div className="flex overflow-hidden rounded border">
          {([
            ["week", currentWeek ? `W${currentWeek}` : "Week"],
            ["fsv", "FSV"],
            ["alpha", "A–Z"],
          ] as Array<[SortMode, string]>).map(([mode, label]) => (
            <button
              key={mode}
              type="button"
              onClick={() => setSortMode(mode)}
              className={`px-3 py-1.5 text-xs font-medium transition-colors ${
                sortMode === mode
                  ? "bg-emerald-600 text-white"
                  : "bg-background text-muted-foreground hover:bg-accent"
              }`}
            >
              {label}
            </button>
          ))}
        </div>

        <button
          type="button"
          onClick={runOptimize}
          className="inline-flex items-center gap-1.5 rounded bg-emerald-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-emerald-700"
        >
          <Sparkles className="h-3.5 w-3.5" /> Optimize
        </button>

        <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
          Weeks
          <select
            value={startWeek}
            onChange={(event) => setStartWeek(Number(event.target.value))}
            className="h-8 rounded border bg-background px-1.5 text-xs"
          >
            {weeks.map((week) => (
              <option key={week} value={week}>{week}</option>
            ))}
          </select>
          –
          <select
            value={endWeek}
            onChange={(event) => setEndWeek(Number(event.target.value))}
            className="h-8 rounded border bg-background px-1.5 text-xs"
          >
            {weeks.map((week) => (
              <option key={week} value={week}>{week}</option>
            ))}
          </select>
        </label>

        <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
          Tie
          <select
            value={tieRule}
            onChange={(event) => setTieRule(event.target.value as TieRule)}
            className="h-8 rounded border bg-background px-1.5 text-xs"
          >
            <option value="tie_loses">counts as a loss</option>
            <option value="tie_survives">counts as a survive</option>
          </select>
        </label>

        <div className="ml-auto flex items-center gap-2">
          <button
            type="button"
            onClick={exportCsv}
            className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent"
          >
            <Download className="h-3.5 w-3.5" /> CSV
          </button>
          <button
            type="button"
            onClick={resetAll}
            className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent"
          >
            <RotateCcw className="h-3.5 w-3.5" /> Reset
          </button>
        </div>
      </div>

      {/* ── summary ────────────────────────────────────────────── */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <SummaryCard
          label="Picked"
          value={`${Object.keys(used).length}/${weeks.length}`}
          detail={
            currentWeek === null ? "season complete" : `next open week: W${currentWeek}`
          }
        />
        <SummaryCard label="Avg win" value={pct(avgWin, 1)} detail="mean of the path's weekly probabilities" />
        <SummaryCard
          label="Path survival"
          value={pathSurvival == null ? "—" : pct(pathSurvival, pathSurvival < 0.1 ? 2 : 1)}
          detail="product, not average — this is the real number"
          emphasis
        />
        <SummaryCard
          label="Remaining plan"
          value={solution.feasible ? pct(solution.survivalProb, solution.survivalProb < 0.1 ? 2 : 1) : "infeasible"}
          detail={
            solution.feasible
              ? `optimal over ${planWeeks.length} open weeks`
              : `week ${planWeeks[solution.unfillableWeek ?? 0] ?? "?"} cannot be filled`
          }
        />
      </div>

      {/* ── this week's decision ───────────────────────────────── */}
      {currentWeek !== null && weekPicks.length > 0 && (
        <section className="rounded-lg border bg-card">
          <header className="flex flex-wrap items-baseline justify-between gap-2 border-b px-3 py-2">
            <h2 className="text-sm font-semibold">Week {currentWeek} — best picks by true cost</h2>
            <p className="text-xs text-muted-foreground">
              Ranked by win probability <em>minus</em> what using the team costs the rest of the plan
            </p>
          </header>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
                  <th className="px-3 py-1.5">Team</th>
                  <th className="px-3 py-1.5">Game</th>
                  <th className="px-3 py-1.5 text-right">Win</th>
                  <th className="px-3 py-1.5 text-right">Cost of using</th>
                  <th className="px-3 py-1.5 text-right">Season survival if taken</th>
                  <th className="px-3 py-1.5">Source</th>
                  <th className="px-3 py-1.5"></th>
                </tr>
              </thead>
              <tbody>
                {weekPicks.map((pick) => {
                  const team = teams[pick.teamIndex];
                  const cell = team.cells[currentWeek];
                  return (
                    <tr key={team.teamId} className="border-b last:border-0 hover:bg-accent/40">
                      <td className="px-3 py-1.5 font-semibold">{team.abbrev}</td>
                      <td className="px-3 py-1.5 text-muted-foreground">
                        {cell.isHome ? "vs" : "@"} {cell.opponent}{" "}
                        <span className="font-mono text-xs">{spreadLabel(cell)}</span>
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums">
                        {pct(pick.p, 1)}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums text-muted-foreground">
                        {pick.survivalCost <= 1e-6 ? "—" : `−${(pick.survivalCost * 100).toFixed(2)}pp`}
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums">
                        {pct(pick.pathSurvival, pick.pathSurvival < 0.1 ? 2 : 1)}
                      </td>
                      <td className="px-3 py-1.5">
                        <ProvenanceBadge cell={cell} />
                      </td>
                      <td className="px-3 py-1.5 text-right">
                        <button
                          type="button"
                          onClick={() => commitWeek(currentWeek, team.teamId)}
                          className="inline-flex items-center gap-1 rounded border px-2 py-0.5 text-xs hover:bg-accent"
                        >
                          <Check className="h-3 w-3" /> Use
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* ── the grid ───────────────────────────────────────────── */}
      <section className="rounded-lg border bg-card">
        <header className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2 text-xs">
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
            <span className="font-semibold">{teams.length * weeks.length} team-weeks</span>
            <LegendSwatch className="bg-emerald-500/35" label={`Market — ${marketCells} games priced`} />
            <LegendSwatch className="bg-sky-500/25" label={`Model — ${modelCells} games not yet priced`} />
            <span className="text-muted-foreground">Click a cell to use that team that week</span>
          </div>
          <span className="font-mono text-[11px] text-muted-foreground">
            {grid.modelVersion ?? "—"} · {new Date(loadedAt).toLocaleTimeString()}
          </span>
        </header>

        <div className="max-h-[70vh] overflow-auto">
          <table className="w-full border-collapse text-xs">
            <thead className="sticky top-0 z-20">
              <tr>
                <th className="sticky left-0 z-30 border-b border-r bg-card px-2 py-1.5 text-left font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
                  Team
                </th>
                <th className="border-b bg-card px-2 py-1.5 text-right font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
                  FSV
                </th>
                {weeks.map((week) => (
                  <th
                    key={week}
                    className={`border-b bg-card px-1 py-1.5 text-center font-mono text-[10px] uppercase tracking-wider ${
                      anchorWeek != null && week > anchorWeek
                        ? "text-muted-foreground/60"
                        : "text-muted-foreground"
                    }`}
                  >
                    W{week}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {visibleTeams.map((team) => {
                const isBanned = banned.includes(team.teamId);
                const usedWeek = Object.entries(used).find(([, id]) => id === team.teamId)?.[0];
                const teamFsv = fsv[teamIndex.get(team.teamId)!] ?? 0;
                return (
                  <tr key={team.teamId} className={isBanned ? "opacity-40" : undefined}>
                    <th className="sticky left-0 z-10 border-b border-r bg-card px-2 py-1 text-left">
                      <div className="flex items-center gap-1.5">
                        <button
                          type="button"
                          onClick={() => toggleBan(team.teamId)}
                          title={isBanned ? "Allow this team" : "Exclude this team"}
                          className="text-muted-foreground hover:text-foreground"
                        >
                          <Ban className="h-3 w-3" />
                        </button>
                        <span className="font-semibold">{team.abbrev}</span>
                        {usedWeek && (
                          <span
                            className="inline-flex items-center gap-0.5 rounded bg-muted px-1 font-mono text-[9px] text-muted-foreground"
                            title={`Used in week ${usedWeek}`}
                          >
                            <Lock className="h-2.5 w-2.5" />W{usedWeek}
                          </span>
                        )}
                      </div>
                    </th>
                    <td className="border-b px-2 py-1 text-right font-mono tabular-nums text-muted-foreground">
                      {teamFsv <= 1e-6 ? "—" : `${(teamFsv * 100).toFixed(2)}`}
                    </td>
                    {weeks.map((week) => {
                      const cell = team.cells[week];
                      const planned = pathByWeek[week];
                      const onPath = planned?.teamId === team.teamId;
                      const probability = advanceProb(cell);
                      if (!cell) {
                        return (
                          <td
                            key={week}
                            className="border-b border-l bg-muted/30 px-1 py-1 text-center font-mono text-[9px] text-muted-foreground"
                          >
                            BYE
                          </td>
                        );
                      }
                      const modeled = cell.provenance === "model_spread";
                      return (
                        <td
                          key={week}
                          className={`border-b border-l px-1 py-1 text-center align-top ${cellTone(cell)} ${
                            onPath
                              ? planned.committed
                                ? "outline outline-2 -outline-offset-2 outline-emerald-500"
                                : "outline outline-2 -outline-offset-2 outline-emerald-400/60"
                              : ""
                          }`}
                        >
                          <button
                            type="button"
                            onClick={() =>
                              used[week] === team.teamId
                                ? clearWeek(week)
                                : commitWeek(week, team.teamId)
                            }
                            title={
                              `${team.abbrev} ${cell.isHome ? "vs" : "@"} ${cell.opponent}\n` +
                              `${modeled ? "Modeled" : "Market"} ${spreadLabel(cell)} · ${pct(probability, 1)}` +
                              (modeled && cell.sigmaH
                                ? `\n±${cell.sigmaH.toFixed(1)} pts at ${cell.horizonWeeks} weeks out`
                                : "") +
                              (cell.spreadSource ? `\nsource: ${cell.spreadSource}` : "")
                            }
                            className="w-full cursor-pointer leading-tight"
                          >
                            <span className="block font-mono text-[9px] text-muted-foreground">
                              {cell.isHome ? "" : "@"}
                              {cell.opponent}
                            </span>
                            <span
                              className={`block font-mono text-[11px] tabular-nums ${
                                modeled ? "italic text-sky-700 dark:text-sky-300" : "font-semibold"
                              }`}
                            >
                              {pct(probability)}
                            </span>
                          </button>
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {/* ── the honest legend ──────────────────────────────────── */}
      <section className="rounded-lg border border-sky-500/30 bg-sky-500/5 p-3 text-sm">
        <div className="mb-1.5 flex items-center gap-1.5 font-semibold">
          <Info className="h-4 w-4 text-sky-600 dark:text-sky-400" />
          How much to trust the far columns
        </div>
        <p className="max-w-4xl text-muted-foreground">
          Weeks {anchorWeek == null ? "past the priced ones" : `after W${anchorWeek}`} have no
          quoted line, so those cells are modeled from market-implied power ratings — shown in
          blue italics with a ± band.{" "}
          {far && far.topPickExactRate != null && far.topPickTop5Rate != null && (
            <>
              Measured over 2010–2025: at {far.horizon} weeks out this model names the eventual
              best play{" "}
              <strong className="text-foreground">{pct(far.topPickExactRate)}</strong> of the time
              and lands in the top five{" "}
              <strong className="text-foreground">{pct(far.topPickTop5Rate)}</strong> of the time,
              with a spread error of{" "}
              <strong className="text-foreground">±{far.rmse.toFixed(1)} points</strong>.{" "}
            </>
          )}
          <strong className="text-foreground">
            Plan with these columns; commit only the current week.
          </strong>
        </p>
        {grid.calibration.length > 0 && (
          <div className="mt-2 overflow-x-auto">
            <table className="font-mono text-[11px] tabular-nums">
              <thead>
                <tr className="text-muted-foreground">
                  <th className="pr-3 text-left font-normal">weeks out</th>
                  {grid.calibration.map((row) => (
                    <th key={row.horizon} className="px-1.5 text-right font-normal">{row.horizon}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className="pr-3 text-muted-foreground">spread error</td>
                  {grid.calibration.map((row) => (
                    <td key={row.horizon} className="px-1.5 text-right">±{row.rmse.toFixed(1)}</td>
                  ))}
                </tr>
                <tr>
                  <td className="pr-3 text-muted-foreground">top pick right</td>
                  {grid.calibration.map((row) => (
                    <td key={row.horizon} className="px-1.5 text-right">
                      {row.topPickExactRate == null ? "—" : `${(row.topPickExactRate * 100).toFixed(0)}%`}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </section>

      <p className="pb-6 text-xs text-muted-foreground">
        The optimizer is exact — a Hungarian assignment over weeks × teams, not a greedy or
        heuristic search — so the path it returns is provably the highest-survival one under your
        constraints. It optimizes survival, which is the right objective in a small pool. In a
        large pool you win by being alive when others are not, which needs pick-popularity data
        this page does not have yet.
      </p>
    </div>
  );
}

function SummaryCard({
  label,
  value,
  detail,
  emphasis,
}: {
  label: string;
  value: string;
  detail: string;
  emphasis?: boolean;
}) {
  return (
    <div className={`rounded-lg border bg-card p-3 ${emphasis ? "border-emerald-500/50" : ""}`}>
      <div className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className={`mt-0.5 font-mono text-2xl tabular-nums ${emphasis ? "text-emerald-600 dark:text-emerald-400" : ""}`}>
        {value}
      </div>
      <div className="mt-0.5 text-[11px] text-muted-foreground">{detail}</div>
    </div>
  );
}

function ProvenanceBadge({ cell }: { cell: SurvivorCell }) {
  if (cell.provenance === "model_spread") {
    return (
      <span
        className="rounded border border-sky-500/40 px-1.5 py-0.5 font-mono text-[10px] uppercase text-sky-700 dark:text-sky-300"
        title={`Modeled ${cell.horizonWeeks} weeks out, ±${cell.sigmaH?.toFixed(1) ?? "?"} pts`}
      >
        Model
      </span>
    );
  }
  if (cell.provenance === "blocked") {
    return (
      <span className="rounded border border-rose-500/40 px-1.5 py-0.5 font-mono text-[10px] uppercase text-rose-600 dark:text-rose-400">
        No data
      </span>
    );
  }
  return (
    <span
      className="rounded border border-emerald-500/40 px-1.5 py-0.5 font-mono text-[10px] uppercase text-emerald-700 dark:text-emerald-400"
      title={`${cell.provenance === "market_ml_novig" ? "No-vig moneyline" : "Quoted spread"} · ${cell.spreadSource ?? ""}`}
    >
      Market
    </span>
  );
}

function LegendSwatch({ className, label }: { className: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-muted-foreground">
      <span className={`inline-block h-3 w-3 rounded-sm border ${className}`} />
      {label}
    </span>
  );
}
