"use client";

/**
 * NFL survivor pool grid, path optimizer, and recommendation ledger.
 *
 * Three things this page does that the popular free grids do not:
 *
 *  1. It marks every cell MARKET or MODEL. Most of the season has no quoted
 *     line yet, so most of any full-season grid is model output; rendering
 *     those cells identically to quoted ones is the central dishonesty of the
 *     genre.
 *  2. It shows PATH SURVIVAL -- the product of the chosen path's
 *     probabilities -- next to the average. Eighteen 75% picks average 75%
 *     and survive 0.6% of the time. The average alone is the flattering
 *     number and the useless one.
 *  3. It freezes what it recommended, before kickoff, into an append-only
 *     ledger that settles against the real result. A tool that cannot be
 *     graded is a toy.
 *
 * Without a pool configured the page is a scratchpad and picks live in this
 * browser. With one, picks and recommendations go to the database and can be
 * settled.
 */

import { useCallback, useEffect, useMemo, useState, useTransition } from "react";
import {
  Ban,
  Check,
  Download,
  Info,
  Lock,
  Plus,
  RotateCcw,
  Search,
  Sparkles,
  Trash2,
} from "lucide-react";
import type {
  SurvivorCell,
  SurvivorGrid,
  SurvivorLedgerRow,
  SurvivorPoolRow,
} from "@/db/queries";
import { evaluateWeek, futureSurvivorValue } from "@/lib/nfl/survivor-assignment";
import {
  CONTRARIAN_TOLERANCE,
  EV_IS_VALIDATED,
  EV_MIN_POOL_SIZE,
  buildPlan,
  evAdvisory,
  type ObjectiveMode,
} from "@/lib/nfl/survivor-policy";
import { addEntry, clearPick, commitPick, createPool, deletePool, freezeRecommendation } from "./actions";

type Props = {
  grid: SurvivorGrid;
  pools: SurvivorPoolRow[];
  ledger: SurvivorLedgerRow[];
  loadedAt: string;
};

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

/** Green ramp for market cells, blue for modeled ones. */
function cellTone(cell: SurvivorCell | undefined): string {
  if (!cell || cell.pWin == null) return "bg-muted/40 text-muted-foreground";
  const modeled = cell.provenance === "model_spread";
  const steps: Array<[number, string, string]> = [
    [0.80, "bg-emerald-500/45", "bg-sky-500/30"],
    [0.72, "bg-emerald-500/32", "bg-sky-500/22"],
    [0.65, "bg-emerald-500/22", "bg-sky-500/15"],
    [0.58, "bg-emerald-500/12", "bg-sky-500/9"],
    [0.50, "bg-transparent", "bg-transparent"],
  ];
  for (const [floor, market, model] of steps) {
    if (cell.pWin >= floor) return modeled ? model : market;
  }
  return "bg-rose-500/10";
}

const RESULT_STYLE: Record<string, string> = {
  won: "text-emerald-600 dark:text-emerald-400",
  lost: "text-rose-600 dark:text-rose-400",
  push: "text-sky-600 dark:text-sky-400",
  pending: "text-muted-foreground",
  void: "text-muted-foreground",
};

export default function SurvivorClient({ grid, pools, ledger, loadedAt }: Props) {
  const { weeks, teams, anchorWeek } = grid;

  const [poolId, setPoolId] = useState<number | null>(pools[0]?.id ?? null);
  const [entryId, setEntryId] = useState<number | null>(pools[0]?.entries[0]?.id ?? null);
  const [localUsed, setLocalUsed] = useState<Record<number, number>>({});
  const [banned, setBanned] = useState<number[]>([]);
  const [startWeek, setStartWeek] = useState<number>(weeks[0] ?? 1);
  const [endWeek, setEndWeek] = useState<number>(weeks[weeks.length - 1] ?? 18);
  const [tieRule, setTieRule] = useState<TieRule>("tie_loses");
  const [objective, setObjective] = useState<ObjectiveMode>("survive");
  const [sortMode, setSortMode] = useState<SortMode>("week");
  const [query, setQuery] = useState("");
  const [showPlan, setShowPlan] = useState(false);
  const [hydrated, setHydrated] = useState(false);
  const [showPoolForm, setShowPoolForm] = useState(false);
  const [notice, setNotice] = useState<{ ok: boolean; text: string } | null>(null);
  const [pending, startTransition] = useTransition();

  const pool = useMemo(
    () => pools.find((item) => item.id === poolId) ?? null,
    [pools, poolId],
  );
  const entry = useMemo(
    () => pool?.entries.find((item) => item.id === entryId) ?? pool?.entries[0] ?? null,
    [pool, entryId],
  );

  // ── local persistence (scratchpad mode only) ──────────────────────
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
            setLocalUsed(
              Object.fromEntries(Object.entries(saved.used).map(([w, id]) => [Number(w), id])),
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
        JSON.stringify({ used: localUsed, banned, startWeek, endWeek, tieRule }),
      );
    } catch {
      // Persistence is a convenience, never a requirement.
    }
  }, [hydrated, localUsed, banned, startWeek, endWeek, tieRule, grid.season]);

  // Pool rules override the scratchpad controls once a pool exists.
  const effectiveTieRule = pool?.tieRule ?? tieRule;
  const effectiveStart = pool?.startWeek ?? startWeek;
  const effectiveEnd = pool?.endWeek ?? endWeek;

  const used: Record<number, number> = useMemo(() => {
    if (!entry) return localUsed;
    return Object.fromEntries(entry.picks.map((pick) => [pick.week, pick.teamId]));
  }, [entry, localUsed]);

  const lockedWeeks = useMemo(
    () => new Set((entry?.picks ?? []).filter((p) => p.locked || p.result !== "pending").map((p) => p.week)),
    [entry],
  );

  // ── derived grid state ────────────────────────────────────────────
  const teamIndex = useMemo(
    () => new Map(teams.map((team, index) => [team.teamId, index])),
    [teams],
  );

  const planWeeks = useMemo(
    () => weeks.filter((w) => w >= effectiveStart && w <= effectiveEnd && !(w in used)),
    [weeks, effectiveStart, effectiveEnd, used],
  );

  const advanceProb = useCallback(
    (cell: SurvivorCell | undefined): number | null => {
      if (!cell || cell.pWin == null) return null;
      return effectiveTieRule === "tie_survives"
        ? Math.min(1, cell.pWin + (cell.pTie ?? 0))
        : cell.pWin;
    },
    [effectiveTieRule],
  );

  const probMatrix = useMemo(
    () => planWeeks.map((week) => teams.map((team) => advanceProb(team.cells[week]))),
    [planWeeks, teams, advanceProb],
  );

  const pickPctMatrix = useMemo(
    () => planWeeks.map((week) => teams.map((team) => team.cells[week]?.pickPct ?? null)),
    [planWeeks, teams],
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

  const plan = useMemo(
    () => buildPlan({ probs: probMatrix, pickPct: pickPctMatrix, bannedTeams: consumed, mode: objective }),
    [probMatrix, pickPctMatrix, consumed, objective],
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

  const plannedByWeek = useMemo(() => {
    const map: Record<number, number> = {};
    if (!showPlan) return map;
    plan.path.forEach((teamIndexValue, position) => {
      if (teamIndexValue != null) map[planWeeks[position]] = teams[teamIndexValue].teamId;
    });
    return map;
  }, [showPlan, plan, planWeeks, teams]);

  const committedProbs = useMemo(
    () =>
      Object.entries(used)
        .map(([week, teamId]) =>
          advanceProb(teams.find((t) => t.teamId === teamId)?.cells[Number(week)]),
        )
        .filter((value): value is number => value != null),
    [used, teams, advanceProb],
  );

  const plannedProbs = showPlan
    ? plan.path
        .map((teamIndexValue, position) =>
          teamIndexValue == null ? null : probMatrix[position][teamIndexValue],
        )
        .filter((value): value is number => value != null)
    : [];

  const allProbs = [...committedProbs, ...plannedProbs];
  const avgWin = allProbs.length ? allProbs.reduce((a, b) => a + b, 0) / allProbs.length : null;
  const pathSurvival = allProbs.length ? allProbs.reduce((a, b) => a * b, 1) : null;

  // ── mutations ─────────────────────────────────────────────────────
  const flash = (result: { ok: boolean; message?: string; error?: string }) => {
    setNotice({ ok: result.ok, text: result.ok ? result.message ?? "Done." : result.error ?? "Failed." });
    window.setTimeout(() => setNotice(null), 6000);
  };

  const setWeekPick = (week: number, teamId: number) => {
    if (!entry) {
      setLocalUsed((prev) => ({ ...prev, [week]: teamId }));
      return;
    }
    if (lockedWeeks.has(week)) {
      flash({ ok: false, error: `Week ${week} is locked — that game has started.` });
      return;
    }
    startTransition(async () => {
      const result = await commitPick({ entryId: entry.id, season: grid.season, week, teamId });
      flash(result.ok ? { ok: true, message: result.message } : { ok: false, error: result.error });
    });
  };

  const removeWeekPick = (week: number) => {
    if (!entry) {
      setLocalUsed((prev) => {
        const next = { ...prev };
        delete next[week];
        return next;
      });
      return;
    }
    startTransition(async () => {
      const result = await clearPick(entry.id, week);
      flash(result.ok ? { ok: true, message: result.message } : { ok: false, error: result.error });
    });
  };

  const freezeCurrent = () => {
    if (currentWeek === null || plan.path[0] == null) return;
    const team = teams[plan.path[0]];
    const cell = team.cells[currentWeek];
    const top = weekPicks.find((pick) => pick.teamIndex === plan.path[0]);
    startTransition(async () => {
      const result = await freezeRecommendation({
        poolId: pool?.id ?? null,
        entryId: entry?.id ?? null,
        season: grid.season,
        week: currentWeek,
        teamId: team.teamId,
        pAdvance: advanceProb(cell),
        provenance: cell?.provenance ?? null,
        objectiveMode: objective,
        path: plan.path.flatMap((index, position) =>
          index == null ? [] : [{ week: planWeeks[position], team: teams[index].abbrev }],
        ),
        pathSurvivalProb: plan.solution.survivalProb,
        opportunityCost: top?.survivalCost ?? null,
        pickPct: cell?.pickPct ?? null,
        alternatives: weekPicks.slice(0, 5).map((pick) => ({
          team: teams[pick.teamIndex].abbrev,
          p: pick.p,
          survivalCost: pick.survivalCost,
        })),
        constraints: {
          banned: banned.map((id) => teams.find((t) => t.teamId === id)?.abbrev ?? id),
          startWeek: effectiveStart,
          endWeek: effectiveEnd,
          tieRule: effectiveTieRule,
        },
        modelVersion: grid.modelVersion ?? "unknown",
      });
      flash(result.ok ? { ok: true, message: result.message } : { ok: false, error: result.error });
    });
  };

  const toggleBan = (teamId: number) => {
    setBanned((prev) => (prev.includes(teamId) ? prev.filter((id) => id !== teamId) : [...prev, teamId]));
  };

  const resetScratchpad = () => {
    setLocalUsed({});
    setBanned([]);
  };

  const exportCsv = () => {
    const header = ["team", ...weeks.map((w) => `W${w}`)].join(",");
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

  const visibleTeams = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const filtered = needle
      ? teams.filter(
          (team) =>
            team.abbrev.toLowerCase().includes(needle) || team.name.toLowerCase().includes(needle),
        )
      : teams;
    const ordered = [...filtered];
    if (sortMode === "alpha") ordered.sort((a, b) => a.abbrev.localeCompare(b.abbrev));
    else if (sortMode === "fsv") {
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
  const hasPickPct = teams.some((team) => Object.values(team.cells).some((c) => c.pickPct != null));

  return (
    <div className="mx-auto max-w-[1600px] space-y-4 p-4">
      <header className="space-y-2">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-bold tracking-tight">Survivor Pool</h1>
          <span className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
            {grid.season} season
          </span>
          {!pool && (
            <span className="rounded border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-amber-600 dark:text-amber-400">
              Scratchpad — picks saved in this browser only
            </span>
          )}
        </div>
        <p className="max-w-3xl text-sm text-muted-foreground">
          Every team&rsquo;s win probability for every week, and an{" "}
          <strong className="text-foreground">exact</strong> optimizer that plans the whole
          season under the use-each-team-once rule. Probabilities come from the market where the
          market has priced the game, and from market-implied power ratings where it has not —
          those cells are marked, never blended in silently.
        </p>
      </header>

      {notice && (
        <div
          className={`rounded border px-3 py-2 text-sm ${
            notice.ok
              ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300"
              : "border-rose-500/40 bg-rose-500/10 text-rose-700 dark:text-rose-300"
          }`}
        >
          {notice.text}
        </div>
      )}

      {/* ── pool bar ───────────────────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-2 rounded-lg border bg-card p-3">
        <span className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
          Pool
        </span>
        <select
          value={poolId ?? ""}
          onChange={(event) => {
            const next = event.target.value ? Number(event.target.value) : null;
            setPoolId(next);
            setEntryId(pools.find((p) => p.id === next)?.entries[0]?.id ?? null);
          }}
          className="h-8 rounded border bg-background px-2 text-sm"
        >
          <option value="">Scratchpad (no pool)</option>
          {pools.map((item) => (
            <option key={item.id} value={item.id}>
              {item.name}
              {item.poolSize ? ` · ${item.poolSize} entries` : ""}
            </option>
          ))}
        </select>

        {pool && (
          <>
            <select
              value={entryId ?? ""}
              onChange={(event) => setEntryId(Number(event.target.value))}
              className="h-8 rounded border bg-background px-2 text-sm"
            >
              {pool.entries.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.label}
                  {item.status === "eliminated" ? ` (out W${item.eliminatedWeek ?? "?"})` : ""}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => {
                const label = window.prompt("Entry label", `Entry ${pool.entries.length + 1}`);
                if (label) startTransition(async () => flash(await addEntry(pool.id, label)));
              }}
              className="inline-flex items-center gap-1 rounded border px-2 py-1.5 text-xs hover:bg-accent"
            >
              <Plus className="h-3 w-3" /> Entry
            </button>
            <span className="font-mono text-[11px] text-muted-foreground">
              ties {pool.tieRule === "tie_survives" ? "survive" : "lose"} · W{pool.startWeek}–
              {pool.endWeek}
              {pool.strikes > 0 ? ` · ${pool.strikes} strike${pool.strikes === 1 ? "" : "s"}` : ""}
            </span>
            <button
              type="button"
              onClick={() => {
                if (window.confirm(`Delete pool "${pool.name}" and all its picks?`)) {
                  startTransition(async () => {
                    flash(await deletePool(pool.id));
                    setPoolId(null);
                    setEntryId(null);
                  });
                }
              }}
              className="inline-flex items-center gap-1 rounded border px-2 py-1.5 text-xs text-muted-foreground hover:bg-accent"
            >
              <Trash2 className="h-3 w-3" />
            </button>
          </>
        )}

        <button
          type="button"
          onClick={() => setShowPoolForm((value) => !value)}
          className="ml-auto inline-flex items-center gap-1 rounded border px-2.5 py-1.5 text-xs hover:bg-accent"
        >
          <Plus className="h-3.5 w-3.5" /> New pool
        </button>
      </div>

      {showPoolForm && (
        <PoolForm
          season={grid.season}
          weeks={weeks}
          pending={pending}
          onSubmit={(input) =>
            startTransition(async () => {
              const result = await createPool(input);
              flash(result);
              if (result.ok) setShowPoolForm(false);
            })
          }
        />
      )}

      {/* ── controls ───────────────────────────────────────────── */}
      <div className="flex flex-wrap items-center gap-2 rounded-lg border bg-card p-3">
        <div className="relative">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
          <input
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            placeholder="Search teams"
            className="h-8 w-44 rounded border bg-background pl-7 pr-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-emerald-500"
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

        <div className="flex overflow-hidden rounded border">
          {([
            ["survive", "Survive"],
            ["ev", "Fade field"],
          ] as Array<[ObjectiveMode, string]>).map(([mode, label]) => (
            <button
              key={mode}
              type="button"
              onClick={() => setObjective(mode)}
              disabled={mode === "ev" && !hasPickPct}
              title={
                mode === "ev" && !hasPickPct
                  ? "No pick-popularity data for this season yet"
                  : undefined
              }
              className={`px-3 py-1.5 text-xs font-medium transition-colors disabled:opacity-40 ${
                objective === mode
                  ? "bg-emerald-600 text-white"
                  : "bg-background text-muted-foreground hover:bg-accent"
              }`}
            >
              {label}
              {mode === "ev" && !EV_IS_VALIDATED && (
                <span className="ml-1 font-mono text-[9px] uppercase opacity-80">research</span>
              )}
            </button>
          ))}
        </div>

        <button
          type="button"
          onClick={() => setShowPlan((value) => !value)}
          className={`inline-flex items-center gap-1.5 rounded px-3 py-1.5 text-xs font-semibold ${
            showPlan ? "bg-emerald-700 text-white" : "bg-emerald-600 text-white hover:bg-emerald-700"
          }`}
        >
          <Sparkles className="h-3.5 w-3.5" /> {showPlan ? "Hide plan" : "Optimize"}
        </button>

        {!pool && (
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
        )}

        <div className="ml-auto flex items-center gap-2">
          <button
            type="button"
            onClick={exportCsv}
            className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent"
          >
            <Download className="h-3.5 w-3.5" /> CSV
          </button>
          {!pool && (
            <button
              type="button"
              onClick={resetScratchpad}
              className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent"
            >
              <RotateCcw className="h-3.5 w-3.5" /> Reset
            </button>
          )}
        </div>
      </div>

      {objective === "ev" && (
        <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm">
          <div className="mb-1 font-semibold">Fade-the-field mode is research only</div>
          <p className="text-muted-foreground">
            {evAdvisory(pool?.poolSize ?? null)} It takes the least-picked team whose net score is
            within {CONTRARIAN_TOLERANCE.toFixed(2)} nats of the best available — the exact policy
            the 2025 simulation tested. It measurably <strong>loses</strong> below{" "}
            {EV_MIN_POOL_SIZE.toLocaleString()} entries, and one season cannot promote it.
            {plan.deviations.length > 0 && (
              <>
                {" "}It deviates in {plan.deviations.length} of {planWeeks.length} open weeks,
                giving up {pct(plan.survivalGivenUp, 2)} of season survival to do it.
              </>
            )}
          </p>
        </div>
      )}

      {/* ── summary ────────────────────────────────────────────── */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <SummaryCard
          label="Picked"
          value={`${Object.keys(used).length}/${weeks.length}`}
          detail={currentWeek === null ? "season complete" : `next open week: W${currentWeek}`}
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
          value={
            plan.solution.feasible
              ? pct(plan.solution.survivalProb, plan.solution.survivalProb < 0.1 ? 2 : 1)
              : "infeasible"
          }
          detail={
            plan.solution.feasible
              ? `${objective === "ev" ? "faded" : "optimal"} over ${planWeeks.length} open weeks`
              : `week ${planWeeks[plan.solution.unfillableWeek ?? 0] ?? "?"} cannot be filled`
          }
        />
      </div>

      {/* ── this week's decision ───────────────────────────────── */}
      {currentWeek !== null && weekPicks.length > 0 && (
        <section className="rounded-lg border bg-card">
          <header className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
            <div>
              <h2 className="text-sm font-semibold">Week {currentWeek} — best picks by true cost</h2>
              <p className="text-xs text-muted-foreground">
                Ranked by win probability <em>minus</em> what using the team costs the rest of the plan
              </p>
            </div>
            <button
              type="button"
              onClick={freezeCurrent}
              disabled={pending || plan.path[0] == null}
              className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent disabled:opacity-40"
              title="Write this recommendation to the append-only ledger so it can be graded later"
            >
              <Lock className="h-3.5 w-3.5" /> Freeze recommendation
            </button>
          </header>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b text-left font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
                  <th className="px-3 py-1.5">Team</th>
                  <th className="px-3 py-1.5">Game</th>
                  <th className="px-3 py-1.5 text-right">Win</th>
                  {hasPickPct && <th className="px-3 py-1.5 text-right">Field</th>}
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
                  const recommended = plan.path[0] === pick.teamIndex;
                  return (
                    <tr
                      key={team.teamId}
                      className={`border-b last:border-0 hover:bg-accent/40 ${
                        recommended ? "bg-emerald-500/5" : ""
                      }`}
                    >
                      <td className="px-3 py-1.5 font-semibold">
                        {team.abbrev}
                        {recommended && (
                          <span className="ml-1.5 font-mono text-[9px] uppercase text-emerald-600 dark:text-emerald-400">
                            pick
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-1.5 text-muted-foreground">
                        {cell.isHome ? "vs" : "@"} {cell.opponent}{" "}
                        <span className="font-mono text-xs">{spreadLabel(cell)}</span>
                      </td>
                      <td className="px-3 py-1.5 text-right font-mono tabular-nums">{pct(pick.p, 1)}</td>
                      {hasPickPct && (
                        <td className="px-3 py-1.5 text-right font-mono tabular-nums text-muted-foreground">
                          {pct(cell.pickPct, 1)}
                        </td>
                      )}
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
                          onClick={() => setWeekPick(currentWeek, team.teamId)}
                          disabled={pending}
                          className="inline-flex items-center gap-1 rounded border px-2 py-0.5 text-xs hover:bg-accent disabled:opacity-40"
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
                      {teamFsv <= 1e-6 ? "—" : (teamFsv * 100).toFixed(2)}
                    </td>
                    {weeks.map((week) => {
                      const cell = team.cells[week];
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
                      const onPath = plannedByWeek[week] === team.teamId;
                      const committed = used[week] === team.teamId;
                      const modeled = cell.provenance === "model_spread";
                      const probability = advanceProb(cell);
                      return (
                        <td
                          key={week}
                          className={`border-b border-l px-1 py-1 text-center align-top ${cellTone(cell)} ${
                            committed
                              ? "outline outline-2 -outline-offset-2 outline-emerald-500"
                              : onPath
                                ? "outline outline-2 -outline-offset-2 outline-emerald-400/60"
                                : ""
                          }`}
                        >
                          <button
                            type="button"
                            onClick={() =>
                              committed ? removeWeekPick(week) : setWeekPick(week, team.teamId)
                            }
                            disabled={pending}
                            title={
                              `${team.abbrev} ${cell.isHome ? "vs" : "@"} ${cell.opponent}\n` +
                              `${modeled ? "Modeled" : "Market"} ${spreadLabel(cell)} · ${pct(probability, 1)}` +
                              (modeled && cell.sigmaH
                                ? `\n±${cell.sigmaH.toFixed(1)} pts at ${cell.horizonWeeks} weeks out`
                                : "") +
                              (cell.pickPct != null ? `\nfield picks it ${pct(cell.pickPct, 1)}` : "") +
                              (cell.spreadSource ? `\nsource: ${cell.spreadSource}` : "")
                            }
                            className="w-full cursor-pointer leading-tight disabled:cursor-wait"
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

      {/* ── ledger ─────────────────────────────────────────────── */}
      {ledger.length > 0 && (
        <section className="rounded-lg border bg-card">
          <header className="flex flex-wrap items-baseline justify-between gap-2 border-b px-3 py-2">
            <h2 className="text-sm font-semibold">Frozen recommendations</h2>
            <p className="text-xs text-muted-foreground">
              Append-only. A changed recommendation supersedes the old one; nothing is rewritten.
            </p>
          </header>
          <div className="max-h-72 overflow-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-card">
                <tr className="border-b text-left font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
                  <th className="px-3 py-1.5">Week</th>
                  <th className="px-3 py-1.5">Entry</th>
                  <th className="px-3 py-1.5">Pick</th>
                  <th className="px-3 py-1.5 text-right">Advance</th>
                  <th className="px-3 py-1.5 text-right">Path survival</th>
                  <th className="px-3 py-1.5">Frozen</th>
                  <th className="px-3 py-1.5">Result</th>
                </tr>
              </thead>
              <tbody>
                {ledger.map((row) => (
                  <tr
                    key={row.id}
                    className={`border-b last:border-0 ${row.superseded ? "opacity-50" : ""}`}
                  >
                    <td className="px-3 py-1.5 font-mono">W{row.week}</td>
                    <td className="px-3 py-1.5 text-muted-foreground">{row.entryLabel ?? "—"}</td>
                    <td className="px-3 py-1.5 font-semibold">
                      {row.team}
                      {row.superseded && (
                        <span className="ml-1.5 font-mono text-[9px] uppercase text-muted-foreground">
                          superseded
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-1.5 text-right font-mono tabular-nums">
                      {pct(row.pAdvance, 1)}
                    </td>
                    <td className="px-3 py-1.5 text-right font-mono tabular-nums">
                      {pct(row.pathSurvivalProb, 2)}
                    </td>
                    <td className="px-3 py-1.5 font-mono text-[11px] text-muted-foreground">
                      {new Date(row.frozenAt).toLocaleString()}
                    </td>
                    <td className={`px-3 py-1.5 font-mono text-xs uppercase ${RESULT_STYLE[row.result]}`}>
                      {row.result}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

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
              best play <strong className="text-foreground">{pct(far.topPickExactRate)}</strong> of
              the time and lands in the top five{" "}
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
                    <th key={row.horizon} className="px-1.5 text-right font-normal">
                      {row.horizon}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td className="pr-3 text-muted-foreground">spread error</td>
                  {grid.calibration.map((row) => (
                    <td key={row.horizon} className="px-1.5 text-right">
                      ±{row.rmse.toFixed(1)}
                    </td>
                  ))}
                </tr>
                <tr>
                  <td className="pr-3 text-muted-foreground">top pick right</td>
                  {grid.calibration.map((row) => (
                    <td key={row.horizon} className="px-1.5 text-right">
                      {row.topPickExactRate == null
                        ? "—"
                        : `${(row.topPickExactRate * 100).toFixed(0)}%`}
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
        heuristic search — so the survive path it returns is provably the highest-survival one
        under your constraints. Whether that is the right objective depends on your pool: it is in
        a small one, and in a large one you win by being alive when others are not. The
        fade-the-field alternative is measured, not assumed, and it is not yet validated.
      </p>
    </div>
  );
}

function PoolForm({
  season,
  weeks,
  pending,
  onSubmit,
}: {
  season: number;
  weeks: number[];
  pending: boolean;
  onSubmit: (input: {
    name: string;
    season: number;
    poolSize: number | null;
    tieRule: "tie_loses" | "tie_survives";
    strikes: number;
    startWeek: number;
    endWeek: number;
    entryLabels: string[];
  }) => void;
}) {
  const [name, setName] = useState("");
  const [poolSize, setPoolSize] = useState("");
  const [tieRule, setTieRule] = useState<"tie_loses" | "tie_survives">("tie_loses");
  const [strikes, setStrikes] = useState("0");
  const [startWeek, setStartWeek] = useState(String(weeks[0] ?? 1));
  const [endWeek, setEndWeek] = useState(String(weeks[weeks.length - 1] ?? 18));
  const [entries, setEntries] = useState("1");

  return (
    <div className="grid gap-3 rounded-lg border bg-card p-3 sm:grid-cols-2 lg:grid-cols-7">
      <Field label="Name">
        <input
          value={name}
          onChange={(event) => setName(event.target.value)}
          placeholder="Office pool"
          className="h-8 w-full rounded border bg-background px-2 text-sm"
        />
      </Field>
      <Field label="Pool size" hint="drives the objective advice">
        <input
          value={poolSize}
          onChange={(event) => setPoolSize(event.target.value.replace(/[^0-9]/g, ""))}
          placeholder="e.g. 120"
          className="h-8 w-full rounded border bg-background px-2 text-sm"
        />
      </Field>
      <Field label="Entries">
        <input
          value={entries}
          onChange={(event) => setEntries(event.target.value.replace(/[^0-9]/g, ""))}
          className="h-8 w-full rounded border bg-background px-2 text-sm"
        />
      </Field>
      <Field label="Ties">
        <select
          value={tieRule}
          onChange={(event) => setTieRule(event.target.value as "tie_loses" | "tie_survives")}
          className="h-8 w-full rounded border bg-background px-1 text-sm"
        >
          <option value="tie_loses">lose</option>
          <option value="tie_survives">survive</option>
        </select>
      </Field>
      <Field label="Strikes" hint="0 = one and done">
        <input
          value={strikes}
          onChange={(event) => setStrikes(event.target.value.replace(/[^0-9]/g, ""))}
          className="h-8 w-full rounded border bg-background px-2 text-sm"
        />
      </Field>
      <Field label="First week">
        <select
          value={startWeek}
          onChange={(event) => setStartWeek(event.target.value)}
          className="h-8 w-full rounded border bg-background px-1 text-sm"
        >
          {weeks.map((week) => (
            <option key={week} value={week}>{week}</option>
          ))}
        </select>
      </Field>
      <Field label="Last week">
        <select
          value={endWeek}
          onChange={(event) => setEndWeek(event.target.value)}
          className="h-8 w-full rounded border bg-background px-1 text-sm"
        >
          {weeks.map((week) => (
            <option key={week} value={week}>{week}</option>
          ))}
        </select>
      </Field>
      <div className="lg:col-span-7">
        <button
          type="button"
          disabled={pending || !name.trim()}
          onClick={() => {
            const count = Math.max(Number(entries) || 1, 1);
            onSubmit({
              name,
              season,
              poolSize: poolSize ? Number(poolSize) : null,
              tieRule,
              strikes: Number(strikes) || 0,
              startWeek: Number(startWeek),
              endWeek: Number(endWeek),
              entryLabels: Array.from({ length: count }, (_, i) => `Entry ${i + 1}`),
            });
          }}
          className="rounded bg-emerald-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-emerald-700 disabled:opacity-40"
        >
          Create pool
        </button>
      </div>
    </div>
  );
}

function Field({
  label,
  hint,
  children,
}: {
  label: string;
  hint?: string;
  children: React.ReactNode;
}) {
  return (
    <label className="block">
      <span className="mb-0.5 block font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      {children}
      {hint && <span className="mt-0.5 block text-[10px] text-muted-foreground">{hint}</span>}
    </label>
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
      <div
        className={`mt-0.5 font-mono text-2xl tabular-nums ${
          emphasis ? "text-emerald-600 dark:text-emerald-400" : ""
        }`}
      >
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
