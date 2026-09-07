"use client";

/**
 * NFL pick'em / confidence pool workspace.
 *
 * The page is organised around one claim that most pick'em tools get wrong:
 * the entry that scores the most points and the entry most likely to WIN THE
 * POOL are different entries, and which one you want depends on how many
 * people you are playing against.
 *
 * So the layout is deliberately: baseline first (provably optimal, no
 * assumptions), then the price of leaving it (exact, closed-form), then what
 * the simulator thinks that price buys (modeled, badged, and never presented
 * as a fact). A user who reads only the top of the page still gets a correct
 * answer; a user who reads to the bottom gets a more ambitious one and is told
 * exactly how much of it rests on an unmeasured field model.
 *
 * Working picks live in this browser. Freezing a card writes it to the
 * append-only ledger at the bottom of the page, which is what makes any of
 * this gradable: it stores the recommended entry AND the max-points baseline
 * it deviated from, so "was deviating worth it" is a paired comparison against
 * a counterfactual frozen before kickoff rather than one rebuilt afterwards.
 */

import { useEffect, useMemo, useState, useTransition } from "react";
import {
  AlertTriangle,
  Check,
  Copy,
  Download,
  FlaskConical,
  Info,
  Lock,
  Plus,
  RotateCcw,
  Sparkles,
  Trash2,
} from "lucide-react";
import type { PickemLedgerRow, PickemPoolRow, PickemSlate, PickemSlateGame } from "@/db/queries";
import {
  ledgerVerdict,
  summarizeLedger,
  type GradedGameRow,
  type SettledWeek,
} from "@/lib/nfl/pickem-grading";
import {
  createPickemPool,
  deletePickemPool,
  freezePickemRecommendation,
  recordPickemFinish,
  settlePickemRecommendations,
  voidPickemRecommendation,
  type ActionResult,
} from "./actions";
import {
  DEFAULT_SIMS,
  DEVIATION_MIN_POOL,
  FIELD_FAVORITE_BIAS,
  FIELD_SKILL_SIGMA,
  MAX_DEVIATIONS,
  MODEL_VERSION,
  PICKEM_IS_VALIDATED,
  SAMPLE_OPPONENTS,
  SEASON_LONG_NOTE,
  defaultObjective,
  poolAdvisory,
  type Objective,
} from "@/lib/nfl/pickem-policy";
import {
  cheapDifferentiation,
  evOptimalEntry,
  evaluateEntry,
  fieldHomeShare,
  optimizeEntry,
  simulateWorld,
  type Entry,
  type PickemGame,
  type PoolFormat,
} from "@/lib/nfl/pickem-strategy";

type Props = {
  slate: PickemSlate;
  pools: PickemPoolRow[];
  ledger: PickemLedgerRow[];
  initialWeek: number;
  loadedAt: string;
};

const STORAGE_KEY = "nfl-pickem-v1";

type Stored = {
  week: number;
  format: PoolFormat;
  poolEntries: number;
  objective: Objective;
  favoriteBias: number;
  overrides: Record<number, { pickHome?: boolean; confidence?: number; fieldHomePct?: number | null }>;
};

const PROVENANCE_LABEL: Record<string, string> = {
  market_ml_novig: "Market",
  market_spread: "Market",
  model_spread: "Model",
  blocked: "None",
};

function pct(x: number, digits = 1): string {
  return `${(x * 100).toFixed(digits)}%`;
}

export default function PickemClient({ slate, pools, ledger, initialWeek, loadedAt }: Props) {
  const [week, setWeek] = useState(initialWeek);
  const [format, setFormat] = useState<PoolFormat>("confidence");
  const [poolEntries, setPoolEntries] = useState(50);
  const [objective, setObjective] = useState<Objective>(defaultObjective());
  const [favoriteBias, setFavoriteBias] = useState(FIELD_FAVORITE_BIAS);
  const [overrides, setOverrides] = useState<Stored["overrides"]>({});
  const [hydrated, setHydrated] = useState(false);

  const [poolId, setPoolId] = useState<number | null>(null);
  const [showNewPool, setShowNewPool] = useState(false);
  const [newPoolName, setNewPoolName] = useState("");
  const [toast, setToast] = useState<ActionResult | null>(null);
  const [pending, startTransition] = useTransition();

  const run = (fn: () => Promise<ActionResult>) => {
    startTransition(async () => {
      setToast(await fn());
    });
  };

  const activePool = pools.find((p) => p.id === poolId) ?? null;

  // Selecting a pool adopts its rules. Done here rather than in an effect so
  // the state change is a direct consequence of the click: a ledger row has to
  // record the pool it was advising, not whatever the controls last said.
  const selectPool = (id: number | null) => {
    setPoolId(id);
    const pool = pools.find((p) => p.id === id);
    if (pool) {
      setFormat(pool.format);
      setPoolEntries(pool.poolEntries);
    }
  };

  // ---- local persistence -------------------------------------------------
  // Restore is deferred off the render pass and re-run on cross-tab writes,
  // matching the survivor page. Two tabs open on the same pool should not
  // silently disagree about the entry.
  useEffect(() => {
    const restore = (raw: string | null) => {
      try {
        if (raw) {
          const parsed = JSON.parse(raw) as Partial<Stored>;
          if (parsed.format === "confidence" || parsed.format === "straight") setFormat(parsed.format);
          if (typeof parsed.poolEntries === "number" && parsed.poolEntries > 0) {
            setPoolEntries(parsed.poolEntries);
          }
          if (parsed.objective === "ev" || parsed.objective === "win") setObjective(parsed.objective);
          if (typeof parsed.favoriteBias === "number" && Number.isFinite(parsed.favoriteBias)) {
            setFavoriteBias(Math.min(Math.max(parsed.favoriteBias, 1), 2));
          }
          if (parsed.overrides && typeof parsed.overrides === "object") setOverrides(parsed.overrides);
        }
      } catch {
        // A blocked, empty, or corrupt store is a normal state, not an error.
      }
      setHydrated(true);
    };
    const initialLoad = window.setTimeout(() => {
      restore(window.localStorage.getItem(STORAGE_KEY));
    }, 0);
    const syncFromAnotherTab = (event: StorageEvent) => {
      if (event.key === STORAGE_KEY) restore(event.newValue);
    };
    window.addEventListener("storage", syncFromAnotherTab);
    return () => {
      window.clearTimeout(initialLoad);
      window.removeEventListener("storage", syncFromAnotherTab);
    };
  }, []);

  useEffect(() => {
    if (!hydrated) return;
    try {
      window.localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({ week, format, poolEntries, objective, favoriteBias, overrides }),
      );
    } catch {
      /* ignore */
    }
  }, [hydrated, week, format, poolEntries, objective, favoriteBias, overrides]);

  // ---- the slate ---------------------------------------------------------
  const weekGames: PickemSlateGame[] = useMemo(
    () => slate.games.filter((g) => g.week === week),
    [slate.games, week],
  );

  const games: PickemGame[] = useMemo(
    () =>
      weekGames.map((g) => ({
        gameId: g.gameId,
        week: g.week,
        homeAbbrev: g.homeAbbrev,
        awayAbbrev: g.awayAbbrev,
        pHome: g.pHome,
        provenance: g.provenance,
        kickoff: g.kickoff,
        completed: g.completed,
        homeWon: g.homeWon,
        fieldHomePct: overrides[g.gameId]?.fieldHomePct ?? null,
      })),
    [weekGames, overrides],
  );

  const fieldModel = useMemo(
    () => ({ favoriteBias, skillSigma: FIELD_SKILL_SIGMA }),
    [favoriteBias],
  );

  // ---- the two entries ---------------------------------------------------
  const baseline: Entry | null = useMemo(
    () => (games.length ? evOptimalEntry(games, format) : null),
    [games, format],
  );

  const world = useMemo(() => {
    if (!games.length) return null;
    return simulateWorld(games, format, fieldModel, {
      sims: DEFAULT_SIMS,
      poolEntries,
      sampleOpponents: SAMPLE_OPPONENTS,
      seed: 20260907,
    });
  }, [games, format, fieldModel, poolEntries]);

  const plan = useMemo(() => {
    if (!games.length || !world) return null;
    return optimizeEntry(games, format, world, { maxDeviations: MAX_DEVIATIONS });
  }, [games, format, world]);

  const activeEntry: Entry | null = useMemo(() => {
    if (!baseline) return null;
    const source = objective === "win" && plan ? plan.recommended : baseline;
    // Manual overrides sit on top of whichever entry the objective produced.
    const entry: Entry = { pickHome: [...source.pickHome], confidence: [...source.confidence] };
    games.forEach((g, i) => {
      const ov = overrides[g.gameId];
      if (ov?.pickHome !== undefined) entry.pickHome[i] = ov.pickHome;
    });
    return entry;
  }, [baseline, plan, objective, games, overrides]);

  const activeEval = useMemo(
    () => (activeEntry && world ? evaluateEntry(games, activeEntry, world) : null),
    [activeEntry, world, games],
  );

  const cheap = useMemo(
    () => (baseline && format === "confidence" ? cheapDifferentiation(games, baseline, 5) : []),
    [games, baseline, format],
  );

  const provenanceMix = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const g of weekGames) {
      const label = PROVENANCE_LABEL[g.provenance] ?? "Model";
      counts[label] = (counts[label] ?? 0) + 1;
    }
    return counts;
  }, [weekGames]);

  const modeledFieldCount = useMemo(
    () => games.filter((g) => fieldHomeShare(g, fieldModel).source === "modeled").length,
    [games, fieldModel],
  );

  const rows = useMemo(() => {
    if (!activeEntry || !baseline) return [];
    return games
      .map((g, i) => {
        const src = weekGames[i];
        const pickHome = activeEntry.pickHome[i];
        const p = pickHome ? g.pHome : 1 - g.pHome;
        const field = fieldHomeShare(g, fieldModel);
        const fieldOnMyPick = pickHome ? field.share : 1 - field.share;
        return {
          index: i,
          game: g,
          src,
          pickHome,
          pick: pickHome ? g.homeAbbrev : g.awayAbbrev,
          against: pickHome ? g.awayAbbrev : g.homeAbbrev,
          p,
          confidence: activeEntry.confidence[i],
          baselineConfidence: baseline.confidence[i],
          baselinePickHome: baseline.pickHome[i],
          fieldOnMyPick,
          fieldSource: field.source,
          /** Positive = we are on the pick the field is lighter on. */
          leverage: p - fieldOnMyPick,
        };
      })
      .sort((a, b) => b.confidence - a.confidence || b.p - a.p);
  }, [games, weekGames, activeEntry, baseline, fieldModel]);

  const toggleSide = (gameId: number, current: boolean) => {
    setOverrides((prev) => ({ ...prev, [gameId]: { ...prev[gameId], pickHome: !current } }));
  };

  const setFieldPct = (gameId: number, value: string) => {
    const num = Number(value);
    setOverrides((prev) => ({
      ...prev,
      [gameId]: {
        ...prev[gameId],
        fieldHomePct: value.trim() === "" || !Number.isFinite(num) ? null : Math.min(Math.max(num / 100, 0.01), 0.99),
      },
    }));
  };

  const copyCard = () => {
    const text = rows
      .map((r) => `${format === "confidence" ? `${r.confidence}\t` : ""}${r.pick} over ${r.against}`)
      .join("\n");
    void navigator.clipboard?.writeText(text);
  };

  const downloadCsv = () => {
    const header = "confidence,pick,opponent,win_prob,field_share_on_pick,field_source,provenance\n";
    const body = rows
      .map((r) =>
        [
          r.confidence,
          r.pick,
          r.against,
          r.p.toFixed(4),
          r.fieldOnMyPick.toFixed(4),
          r.fieldSource,
          r.game.provenance,
        ].join(","),
      )
      .join("\n");
    const blob = new Blob([header + body], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `pickem-${slate.season}-wk${week}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const evGiveUp =
    plan && objective === "win"
      ? plan.baselineEval.expectedPoints - plan.recommendedEval.expectedPoints
      : 0;

  // ---- ledger ------------------------------------------------------------
  // Superseded rows stay visible as audit history but never enter a summary:
  // they are what the tool said before it changed its mind, not what it
  // advised. Void rows are excluded for the same reason.
  const liveLedger = useMemo(
    () => ledger.filter((r) => r.supersededBy == null && r.status !== "void"),
    [ledger],
  );

  const ledgerSummary = useMemo(() => {
    const weeks: SettledWeek[] = liveLedger
      .filter((r) => r.gamesGraded > 0)
      .map((r) => ({
        week: r.week,
        objective: r.objective,
        baselinePoints: r.baselineActualPoints ?? 0,
        recommendedPoints: r.recommendedActualPoints ?? 0,
        expectedPointsDelta:
          (r.recommendedExpectedPoints ?? 0) - (r.baselineExpectedPoints ?? 0),
        gamesGraded: r.gamesGraded,
        brier: r.brier,
        coinflipBrier: r.coinflipBrier,
        wonPool: r.wonPool,
        finishRank: r.finishRank,
      }));
    const rows: GradedGameRow[] = liveLedger.flatMap((r) =>
      r.games.map((g) => ({
        gameId: g.gameId,
        pHome: g.pHome,
        provenance: g.provenance,
        homeWon: g.homeWon,
        baselinePickHome: g.baselinePickHome,
        baselineConfidence: g.baselineConfidence,
        recommendedPickHome: g.recommendedPickHome,
        recommendedConfidence: g.recommendedConfidence,
      })),
    );
    return summarizeLedger(weeks, rows);
  }, [liveLedger]);

  const frozenThisWeek = liveLedger.find(
    (r) => r.week === week && (r.poolId ?? null) === poolId,
  );

  const weekHasStarted = weekGames.some(
    (g) => g.kickoff != null && new Date(g.kickoff) <= new Date(),
  );

  const freezeCard = () => {
    if (!plan || !activeEntry || !baseline || games.length === 0) return;
    const frozen = games.map((g, i) => {
      const src = weekGames[i];
      const field = fieldHomeShare(g, fieldModel);
      return {
        gameId: g.gameId,
        homeTeamId: src.homeTeamId,
        awayTeamId: src.awayTeamId,
        pHome: g.pHome,
        provenance: g.provenance,
        kickoff: g.kickoff,
        baselinePickHome: baseline.pickHome[i],
        baselineConfidence: baseline.confidence[i],
        // The entry as it stands on screen, manual overrides included -- the
        // ledger records what was actually advised, not what the optimizer
        // would have said if left alone.
        recommendedPickHome: activeEntry.pickHome[i],
        recommendedConfidence: activeEntry.confidence[i],
        fieldHomeShare: field.share,
        fieldSource: field.source,
      };
    });
    run(() =>
      freezePickemRecommendation({
        poolId,
        season: slate.season,
        week,
        format,
        objective,
        poolEntries,
        sims: DEFAULT_SIMS,
        modelVersion: MODEL_VERSION,
        baselineExpectedPoints: plan.baselineEval.expectedPoints,
        recommendedExpectedPoints: activeEval?.expectedPoints ?? plan.recommendedEval.expectedPoints,
        baselinePrizeShare: plan.baselineEval.prizeShare,
        recommendedPrizeShare: activeEval?.prizeShare ?? plan.recommendedEval.prizeShare,
        fieldModel: {
          favoriteBias,
          skillSigma: FIELD_SKILL_SIGMA,
          observedGames: games.length - modeledFieldCount,
          modeledGames: modeledFieldCount,
        },
        deviations: plan.deviations,
        games: frozen,
      }),
    );
  };

  return (
    <div className="mx-auto max-w-[1500px] space-y-4 p-4">
      {/* ---------------------------------------------------------------- */}
      <header className="space-y-2">
        <div className="flex flex-wrap items-center gap-3">
          <h1 className="text-2xl font-bold tracking-tight">Pick&apos;em Pools</h1>
          <span className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
            {MODEL_VERSION}
          </span>
          {!PICKEM_IS_VALIDATED && (
            <span className="inline-flex items-center gap-1 rounded border border-amber-500/40 bg-amber-500/10 px-2 py-0.5 font-mono text-[11px] uppercase tracking-wider text-amber-600 dark:text-amber-400">
              <FlaskConical className="h-3 w-3" /> research
            </span>
          )}
        </div>
        <p className="max-w-4xl text-sm text-muted-foreground">
          The entry that scores the most points and the entry most likely to{" "}
          <strong className="text-foreground">win the pool</strong> are different entries. This page
          computes the first one exactly, prices every step away from it exactly, and then simulates
          whether that price is worth paying against a field of your pool&apos;s size.
        </p>
      </header>

      {/* ---- controls --------------------------------------------------- */}
      <div className="flex flex-wrap items-center gap-3 rounded-lg border bg-card p-3">
        <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
          Week
          <select
            value={week}
            onChange={(e) => setWeek(Number(e.target.value))}
            className="h-8 rounded border bg-background px-2 text-sm"
          >
            {slate.weeks.map((w) => (
              <option key={w} value={w}>{w}</option>
            ))}
          </select>
        </label>

        <div className="flex overflow-hidden rounded border">
          {(["confidence", "straight"] as PoolFormat[]).map((f) => (
            <button
              key={f}
              onClick={() => setFormat(f)}
              className={`px-2.5 py-1.5 text-xs ${
                format === f ? "bg-primary text-primary-foreground" : "hover:bg-accent"
              }`}
            >
              {f === "confidence" ? "Confidence" : "Straight"}
            </button>
          ))}
        </div>

        <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
          Pool entries
          <input
            type="number"
            min={1}
            max={100000}
            value={poolEntries}
            onChange={(e) => setPoolEntries(Math.max(1, Number(e.target.value) || 1))}
            className="h-8 w-24 rounded border bg-background px-2 text-sm"
          />
        </label>

        <div className="flex overflow-hidden rounded border">
          <button
            onClick={() => setObjective("ev")}
            className={`px-2.5 py-1.5 text-xs ${
              objective === "ev" ? "bg-primary text-primary-foreground" : "hover:bg-accent"
            }`}
          >
            Max points
          </button>
          <button
            onClick={() => setObjective("win")}
            className={`px-2.5 py-1.5 text-xs ${
              objective === "win" ? "bg-amber-500 text-white" : "hover:bg-accent"
            }`}
          >
            <Sparkles className="mr-1 inline h-3 w-3" />
            Max win chance
            <span className="ml-1 font-mono text-[9px] uppercase opacity-80">research</span>
          </button>
        </div>

        <label className="flex items-center gap-1.5 text-xs text-muted-foreground">
          Pool
          <select
            value={poolId ?? ""}
            onChange={(e) => selectPool(e.target.value === "" ? null : Number(e.target.value))}
            className="h-8 rounded border bg-background px-2 text-sm"
          >
            <option value="">Scratchpad (no pool)</option>
            {pools.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
        </label>
        <button
          onClick={() => setShowNewPool((v) => !v)}
          className="inline-flex items-center gap-1 rounded border px-2 py-1.5 text-xs hover:bg-accent"
        >
          <Plus className="h-3 w-3" /> Pool
        </button>
        {activePool && (
          <button
            onClick={() => {
              if (confirm(`Delete "${activePool.name}" and every ledger row under it?`)) {
                selectPool(null);
                run(() => deletePickemPool(activePool.id));
              }
            }}
            className="inline-flex items-center gap-1 rounded border px-2 py-1.5 text-xs text-muted-foreground hover:bg-accent"
            title="Deletes the pool and its ledger rows"
          >
            <Trash2 className="h-3 w-3" />
          </button>
        )}

        <div className="ml-auto flex items-center gap-2">
          <button
            onClick={freezeCard}
            disabled={pending || games.length === 0 || weekHasStarted}
            className="inline-flex items-center gap-1.5 rounded border border-emerald-500/50 bg-emerald-500/10 px-2.5 py-1.5 text-xs font-medium text-emerald-700 hover:bg-emerald-500/20 disabled:opacity-40 dark:text-emerald-400"
            title={
              weekHasStarted
                ? "This week has already started — a card frozen after kickoff is hindsight."
                : "Freeze this card and the max-points baseline into the ledger"
            }
          >
            <Lock className="h-3.5 w-3.5" />
            {frozenThisWeek ? "Re-freeze" : "Freeze card"}
          </button>
          <button
            onClick={copyCard}
            className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent"
          >
            <Copy className="h-3.5 w-3.5" /> Copy card
          </button>
          <button
            onClick={downloadCsv}
            className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent"
          >
            <Download className="h-3.5 w-3.5" /> CSV
          </button>
          {Object.keys(overrides).length > 0 && (
            <button
              onClick={() => setOverrides({})}
              className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs text-muted-foreground hover:bg-accent"
            >
              <RotateCcw className="h-3.5 w-3.5" /> Clear edits
            </button>
          )}
        </div>
      </div>

      {showNewPool && (
        <div className="flex flex-wrap items-end gap-3 rounded-lg border bg-card p-3">
          <label className="flex flex-col gap-1 text-xs text-muted-foreground">
            Pool name
            <input
              value={newPoolName}
              onChange={(e) => setNewPoolName(e.target.value)}
              placeholder="Office pool"
              className="h-8 w-56 rounded border bg-background px-2 text-sm"
            />
          </label>
          <span className="pb-1.5 text-xs text-muted-foreground">
            Adopts the current format ({format}) and {poolEntries} entries.
          </span>
          <button
            onClick={() => {
              run(async () => {
                const result = await createPickemPool({
                  name: newPoolName,
                  season: slate.season,
                  format,
                  poolEntries,
                  notes: null,
                });
                if (result.ok) {
                  setNewPoolName("");
                  setShowNewPool(false);
                }
                return result;
              });
            }}
            disabled={pending}
            className="h-8 rounded border px-2.5 text-xs hover:bg-accent disabled:opacity-40"
          >
            Create
          </button>
        </div>
      )}

      {toast && (
        <div
          className={`flex items-start gap-2 rounded-lg border p-3 text-sm ${
            toast.ok
              ? "border-emerald-500/40 bg-emerald-500/5"
              : "border-rose-500/40 bg-rose-500/5"
          }`}
        >
          {toast.ok ? (
            <Check className="mt-0.5 h-4 w-4 shrink-0 text-emerald-600 dark:text-emerald-400" />
          ) : (
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-rose-600 dark:text-rose-400" />
          )}
          <span>{toast.ok ? toast.message : toast.error}</span>
          <button
            onClick={() => setToast(null)}
            className="ml-auto text-xs text-muted-foreground hover:underline"
          >
            dismiss
          </button>
        </div>
      )}

      {/* ---- pool-size advisory ----------------------------------------- */}
      <div className="rounded-lg border bg-card p-3 text-sm">
        <div className="mb-1 flex items-center gap-1.5 font-semibold">
          <Info className="h-3.5 w-3.5" /> How much deviation is worth paying for
        </div>
        <p className="text-muted-foreground">{poolAdvisory(poolEntries, format)}</p>
        <p className="mt-1.5 text-muted-foreground">{SEASON_LONG_NOTE}</p>
      </div>

      {objective === "win" && (
        <div className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-3 text-sm">
          <div className="mb-1 flex items-center gap-1.5 font-semibold">
            <AlertTriangle className="h-3.5 w-3.5 text-amber-600 dark:text-amber-400" />
            Max win chance is research only
          </div>
          <p className="text-muted-foreground">
            Max points is <strong className="text-foreground">proved</strong> optimal — it needs no
            assumptions and cannot be wrong. Max win chance is a simulation whose field model has
            never been measured against a real pick&apos;em pool: this repo has no pick&apos;em
            pick-share feed, and the survivor popularity feed is a different distribution that would
            be wrong to substitute.{" "}
            {modeledFieldCount === games.length
              ? `All ${games.length} games use the modeled field.`
              : `${modeledFieldCount} of ${games.length} games use the modeled field; the rest use pick shares you entered.`}{" "}
            No pick&apos;em entry has ever been settled here, so nothing below has been graded. Read
            it as an ordering, not a forecast.
          </p>
          {poolEntries < DEVIATION_MIN_POOL && (
            <p className="mt-1.5 font-medium text-amber-700 dark:text-amber-400">
              At {poolEntries} entries this mode is working against you — take the max-points entry.
            </p>
          )}
        </div>
      )}

      {/* ---- headline numbers ------------------------------------------- */}
      {activeEval && plan && (
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
          <Stat
            label="Expected points"
            value={activeEval.expectedPoints.toFixed(2)}
            sub={
              evGiveUp > 0.005
                ? `${evGiveUp.toFixed(2)} given up vs the max-points entry`
                : "This is the maximum achievable"
            }
          />
          <Stat
            label="Win the pool"
            value={pct(activeEval.prizeShare, 2)}
            sub={`Expected prize share vs ${poolEntries - 1} rival${poolEntries === 2 ? "" : "s"}`}
            highlight
          />
          <Stat
            label="Score spread"
            value={`${activeEval.meanScore.toFixed(1)} ± ${activeEval.scoreStdDev.toFixed(1)}`}
            sub="Mean and SD of this entry's score"
          />
          <Stat
            label="Baseline win rate"
            value={pct(plan.baselineEval.prizeShare, 2)}
            sub={
              plan.recommendedEval.prizeShare > plan.baselineEval.prizeShare
                ? `Optimizer found +${((plan.recommendedEval.prizeShare - plan.baselineEval.prizeShare) * 100).toFixed(2)}pp`
                : "Optimizer found no improvement worth its cost"
            }
          />
        </div>
      )}

      {/* ---- what the optimizer changed --------------------------------- */}
      {plan && plan.deviations.length > 0 && (
        <section className="rounded-lg border bg-card">
          <header className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
            <h2 className="text-sm font-semibold">
              Deviations from the max-points entry
              <span className="ml-2 font-normal text-muted-foreground">
                each priced exactly, then simulated
              </span>
            </h2>
            {!plan.converged && (
              <span className="font-mono text-[10px] uppercase text-muted-foreground">
                stopped at the {MAX_DEVIATIONS}-move cap
              </span>
            )}
          </header>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 text-left text-xs text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 font-medium">Move</th>
                  <th className="px-3 py-2 text-right font-medium">Costs (pts)</th>
                  <th className="px-3 py-2 text-right font-medium">Buys (win %)</th>
                  <th className="px-3 py-2 text-right font-medium">Points per pp</th>
                </tr>
              </thead>
              <tbody>
                {plan.deviations.map((d, i) => (
                  <tr key={i} className="border-t">
                    <td className="px-3 py-2">{d.description}</td>
                    <td className="px-3 py-2 text-right font-mono tabular-nums text-rose-600 dark:text-rose-400">
                      −{d.evCost.toFixed(2)}
                    </td>
                    <td className="px-3 py-2 text-right font-mono tabular-nums text-emerald-600 dark:text-emerald-400">
                      +{(d.shareGain * 100).toFixed(3)}pp
                    </td>
                    <td className="px-3 py-2 text-right font-mono tabular-nums text-muted-foreground">
                      {(d.evCost / Math.max(d.shareGain * 100, 1e-6)).toFixed(2)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="border-t px-3 py-2 text-xs text-muted-foreground">
            The cost column is exact arithmetic:{" "}
            <code className="font-mono">(c_i − c_j)(p_i − p_j)</code> for a confidence swap,{" "}
            <code className="font-mono">c(2p − 1)</code> for a side flip. The gain column is Monte
            Carlo over {DEFAULT_SIMS.toLocaleString()} slates against a modeled field — the soft half
            of every row.
          </p>
        </section>
      )}

      {/* ---- the card ---------------------------------------------------- */}
      <section className="rounded-lg border bg-card">
        <header className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
          <h2 className="text-sm font-semibold">
            Week {week} card
            <span className="ml-2 font-normal text-muted-foreground">
              {rows.length} game{rows.length === 1 ? "" : "s"} ·{" "}
              {Object.entries(provenanceMix)
                .map(([k, v]) => `${v} ${k.toLowerCase()}`)
                .join(", ")}
            </span>
          </h2>
          <span className="font-mono text-[10px] uppercase text-muted-foreground">
            click a team to override
          </span>
        </header>

        {modeledFieldCount > 0 && rows.length > 0 && (
          <p className="border-b bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
            <strong className="text-foreground">
              {modeledFieldCount === rows.length ? "Every" : `${modeledFieldCount} of ${rows.length}`}
            </strong>{" "}
            field share{modeledFieldCount === 1 ? " is" : "s are"} estimated from the favourite-bias
            curve, not observed — so leverage on those rows is a restatement of the win probability
            and cannot tell you anything new. If your pool publishes pick percentages, type them into
            the field column: that is the single change that makes everything downstream of it real.
          </p>
        )}

        {rows.length === 0 ? (
          <p className="p-4 text-sm text-muted-foreground">
            No games with a win probability for week {week} of {slate.season}. The survivor pipeline
            (<code className="font-mono">ingest/refresh_nfl_survivor.py</code>) populates{" "}
            <code className="font-mono">nfl_game_win_probs</code>, which this page reads rather than
            computing its own — two pages in this app disagreeing about the same game would be a
            defect.
          </p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 text-left text-xs text-muted-foreground">
                <tr>
                  {format === "confidence" && <th className="px-3 py-2 font-medium">Conf</th>}
                  <th className="px-3 py-2 font-medium">Pick</th>
                  <th className="px-3 py-2 font-medium">Over</th>
                  <th className="px-3 py-2 text-right font-medium">Win prob</th>
                  <th className="px-3 py-2 font-medium">Source</th>
                  <th className="px-3 py-2 text-right font-medium">Field on my side</th>
                  <th className="px-3 py-2 text-right font-medium">Leverage</th>
                  <th className="px-3 py-2 font-medium">Result</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const changed =
                    r.pickHome !== r.baselinePickHome || r.confidence !== r.baselineConfidence;
                  const correct =
                    r.game.homeWon == null ? null : r.game.homeWon === r.pickHome;
                  return (
                    <tr key={r.game.gameId} className={`border-t ${changed ? "bg-amber-500/5" : ""}`}>
                      {format === "confidence" && (
                        <td className="px-3 py-2 font-mono tabular-nums font-semibold">
                          {r.confidence}
                          {r.confidence !== r.baselineConfidence && (
                            <span className="ml-1 font-normal text-[10px] text-amber-600 dark:text-amber-400">
                              was {r.baselineConfidence}
                            </span>
                          )}
                        </td>
                      )}
                      <td className="px-3 py-2">
                        <button
                          onClick={() => toggleSide(r.game.gameId, r.pickHome)}
                          className="rounded px-1.5 py-0.5 font-semibold hover:bg-accent"
                          title="Switch to the other side"
                        >
                          {r.pick}
                        </button>
                        {r.pickHome !== r.baselinePickHome && (
                          <span className="ml-1 text-[10px] text-amber-600 dark:text-amber-400">
                            flipped
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-muted-foreground">{r.against}</td>
                      <td className="px-3 py-2 text-right font-mono tabular-nums">{pct(r.p)}</td>
                      <td className="px-3 py-2">
                        <span
                          className={`rounded px-1.5 py-0.5 font-mono text-[10px] uppercase ${
                            PROVENANCE_LABEL[r.game.provenance] === "Market"
                              ? "bg-emerald-500/10 text-emerald-700 dark:text-emerald-400"
                              : "bg-muted text-muted-foreground"
                          }`}
                        >
                          {PROVENANCE_LABEL[r.game.provenance] ?? "Model"}
                        </span>
                      </td>
                      <td className="px-3 py-2 text-right">
                        <input
                          type="number"
                          min={1}
                          max={99}
                          placeholder={(r.fieldOnMyPick * 100).toFixed(0)}
                          value={
                            overrides[r.game.gameId]?.fieldHomePct != null
                              ? Math.round(
                                  (r.pickHome
                                    ? overrides[r.game.gameId]!.fieldHomePct!
                                    : 1 - overrides[r.game.gameId]!.fieldHomePct!) * 100,
                                )
                              : ""
                          }
                          onChange={(e) => {
                            // The input is "share on MY pick"; storage is always
                            // share on HOME, so an away pick has to be inverted.
                            const v = Number(e.target.value);
                            if (e.target.value.trim() === "" || !Number.isFinite(v)) {
                              setFieldPct(r.game.gameId, "");
                            } else {
                              setFieldPct(r.game.gameId, String(r.pickHome ? v : 100 - v));
                            }
                          }}
                          className={`h-7 w-16 rounded border bg-background px-1.5 text-right text-xs ${
                            r.fieldSource === "modeled" ? "text-muted-foreground" : "font-semibold"
                          }`}
                          title={
                            r.fieldSource === "modeled"
                              ? "Modeled from the favourite-bias curve. Type your pool's real pick % to replace it."
                              : "Your pool's observed pick %"
                          }
                        />
                      </td>
                      {/*
                        Leverage is only real information when the field share
                        was OBSERVED. When it is modeled, the share is a
                        monotone function of our own probability, so leverage is
                        negative on every favourite by construction and says
                        nothing we did not already know from the win-prob
                        column. Colouring it red on all sixteen rows would
                        assert a judgement the number cannot support, so a
                        modeled row is rendered muted and labelled.
                      */}
                      <td
                        className={`px-3 py-2 text-right font-mono tabular-nums ${
                          r.fieldSource === "modeled"
                            ? "text-muted-foreground/60"
                            : r.leverage > 0.02
                              ? "text-emerald-600 dark:text-emerald-400"
                              : r.leverage < -0.02
                                ? "text-rose-600 dark:text-rose-400"
                                : "text-muted-foreground"
                        }`}
                        title={
                          r.fieldSource === "modeled"
                            ? "Derived from the modeled field, which is a function of this same win probability — it adds no independent information. Enter your pool's real pick % to make this meaningful."
                            : "Our win probability minus the share of the field on the same side. Positive means we like it more than the room does."
                        }
                      >
                        {r.leverage >= 0 ? "+" : ""}
                        {(r.leverage * 100).toFixed(1)}
                        {r.fieldSource === "modeled" && (
                          <span className="ml-1 text-[9px] uppercase">est</span>
                        )}
                      </td>
                      <td className="px-3 py-2">
                        {correct == null ? (
                          <span className="text-xs text-muted-foreground">—</span>
                        ) : correct ? (
                          <span className="font-mono text-xs text-emerald-600 dark:text-emerald-400">
                            +{r.confidence}
                          </span>
                        ) : (
                          <span className="font-mono text-xs text-rose-600 dark:text-rose-400">0</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      {/* ---- cheap differentiation --------------------------------------- */}
      {cheap.length > 0 && (
        <section className="rounded-lg border bg-card">
          <header className="border-b px-3 py-2">
            <h2 className="text-sm font-semibold">
              Cheapest ways to look different
              <span className="ml-2 font-normal text-muted-foreground">
                exact arithmetic, no simulation
              </span>
            </h2>
          </header>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 text-left text-xs text-muted-foreground">
                <tr>
                  <th className="px-3 py-2 font-medium">Swap</th>
                  <th className="px-3 py-2 text-right font-medium">Costs (pts)</th>
                  <th className="px-3 py-2 text-right font-medium">Confidence moved</th>
                  <th className="px-3 py-2 text-right font-medium">Prob gap</th>
                </tr>
              </thead>
              <tbody>
                {cheap.map((c) => (
                  <tr key={`${c.i}-${c.j}`} className="border-t">
                    <td className="px-3 py-2">{c.label}</td>
                    <td className="px-3 py-2 text-right font-mono tabular-nums">
                      −{c.evCost.toFixed(2)}
                    </td>
                    <td className="px-3 py-2 text-right font-mono tabular-nums">{c.confidenceGap}</td>
                    <td className="px-3 py-2 text-right font-mono tabular-nums text-muted-foreground">
                      {(c.probabilityGap * 100).toFixed(1)}pp
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="border-t px-3 py-2 text-xs text-muted-foreground">
            A real NFL slate has a flat middle where six or seven games sit within a couple of points
            of a coin flip while occupying confidence ranks five or six apart. Transposing inside that
            block costs a large weight gap times a tiny probability gap — nearly free — while moving
            your score well away from everyone who sorted the same slate the same way. Deviating on
            the game you are most sure of buys the same separation for an order of magnitude more.
          </p>
        </section>
      )}

      {/* ---- the ledger --------------------------------------------------- */}
      <section className="rounded-lg border bg-card">
        <header className="flex flex-wrap items-center justify-between gap-2 border-b px-3 py-2">
          <h2 className="text-sm font-semibold">
            Recommendation ledger
            <span className="ml-2 font-normal text-muted-foreground">
              append-only, frozen before kickoff, graded against real results
            </span>
          </h2>
          <button
            onClick={() => run(() => settlePickemRecommendations(slate.season))}
            disabled={pending || liveLedger.length === 0}
            className="inline-flex items-center gap-1.5 rounded border px-2.5 py-1.5 text-xs hover:bg-accent disabled:opacity-40"
          >
            <Check className="h-3.5 w-3.5" /> Settle completed weeks
          </button>
        </header>

        <p className="border-b px-3 py-2 text-xs text-muted-foreground">
          Every row stores <strong className="text-foreground">both</strong> entries — the one
          recommended and the max-points baseline it deviated from. That is the point: rebuilding
          the baseline after the results are known would compare against a card the model might no
          longer produce, so the counterfactual is frozen at the same instant as the recommendation.
          Points and calibration settle automatically from real scores; whether the entry actually{" "}
          <em>won</em> is the one thing we cannot see, so it is entered by hand and an absent value
          stays absent rather than becoming a loss.
        </p>

        {liveLedger.length === 0 ? (
          <p className="p-4 text-sm text-muted-foreground">
            Nothing frozen yet. Set the week and objective above, then{" "}
            <strong className="text-foreground">Freeze card</strong> before the first kickoff. A card
            can only be frozen while every game is still ahead of it.
          </p>
        ) : (
          <>
            <div className="grid gap-3 border-b p-3 sm:grid-cols-2 lg:grid-cols-4">
              <Stat
                label="Settled weeks"
                value={String(ledgerSummary.settledWeeks)}
                sub={`${ledgerSummary.gamesGraded} games graded`}
              />
              <Stat
                label="Calibration (Brier)"
                value={ledgerSummary.brier != null ? ledgerSummary.brier.toFixed(4) : "—"}
                sub={
                  ledgerSummary.coinflipBrier != null
                    ? `vs ${ledgerSummary.coinflipBrier.toFixed(4)} for a coin flip on the same games`
                    : "Nothing graded yet"
                }
              />
              <Stat
                label="Paired points delta"
                value={
                  ledgerSummary.settledWeeks > 0
                    ? `${ledgerSummary.totalPointsDelta >= 0 ? "+" : ""}${ledgerSummary.totalPointsDelta.toFixed(0)}`
                    : "—"
                }
                sub={
                  ledgerSummary.settledWeeks > 0
                    ? `Priced at ${ledgerSummary.totalExpectedPointsDelta.toFixed(1)} before kickoff — that is the bar, not zero`
                    : "Recommended minus baseline"
                }
              />
              <Stat
                label="Pools won"
                value={
                  ledgerSummary.reportedFinishes > 0
                    ? `${ledgerSummary.poolsWon} / ${ledgerSummary.reportedFinishes}`
                    : "Not reported"
                }
                sub={
                  ledgerSummary.reportedFinishes > 0
                    ? "Descriptive only — see the verdict below"
                    : "The only decisive measurement, and it needs your input"
                }
                highlight={ledgerSummary.reportedFinishes > 0}
              />
            </div>

            <p className="border-b bg-muted/30 px-3 py-2 text-xs text-muted-foreground">
              {ledgerVerdict(ledgerSummary)}
            </p>

            {ledgerSummary.calibration.length > 0 && (
              <div className="border-b px-3 py-2">
                <div className="mb-1.5 text-xs font-semibold">
                  Favourite reliability
                  <span className="ml-2 font-normal text-muted-foreground">
                    folded onto the favourite so a bin cannot average to 50% and look calibrated
                  </span>
                </div>
                <div className="flex flex-wrap gap-2">
                  {ledgerSummary.calibration.map((b) => (
                    <div key={b.label} className="rounded border px-2 py-1 text-xs">
                      <span className="font-mono">{b.label}</span>
                      <span className="ml-2 text-muted-foreground">n={b.n}</span>
                      <span className="ml-2 font-mono tabular-nums">
                        said {pct(b.meanForecast, 0)} · went {pct(b.hitRate, 0)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-muted/50 text-left text-xs text-muted-foreground">
                  <tr>
                    <th className="px-3 py-2 font-medium">Wk</th>
                    <th className="px-3 py-2 font-medium">Pool</th>
                    <th className="px-3 py-2 font-medium">Objective</th>
                    <th className="px-3 py-2 text-right font-medium">Baseline</th>
                    <th className="px-3 py-2 text-right font-medium">Recommended</th>
                    <th className="px-3 py-2 text-right font-medium">Delta</th>
                    <th className="px-3 py-2 text-right font-medium">Brier</th>
                    <th className="px-3 py-2 font-medium">Status</th>
                    <th className="px-3 py-2 font-medium">Finish</th>
                  </tr>
                </thead>
                <tbody>
                  {liveLedger.map((r) => {
                    const delta =
                      r.recommendedActualPoints != null && r.baselineActualPoints != null
                        ? r.recommendedActualPoints - r.baselineActualPoints
                        : null;
                    const priced =
                      (r.recommendedExpectedPoints ?? 0) - (r.baselineExpectedPoints ?? 0);
                    return (
                      <tr key={r.id} className="border-t align-top">
                        <td className="px-3 py-2 font-mono tabular-nums">{r.week}</td>
                        <td className="px-3 py-2 text-muted-foreground">
                          {r.poolName ?? "Scratchpad"}
                          <div className="font-mono text-[10px]">
                            {r.poolEntries} entries · {r.format}
                          </div>
                        </td>
                        <td className="px-3 py-2">
                          <span
                            className={`rounded px-1.5 py-0.5 font-mono text-[10px] uppercase ${
                              r.objective === "win"
                                ? "bg-amber-500/10 text-amber-700 dark:text-amber-400"
                                : "bg-muted text-muted-foreground"
                            }`}
                          >
                            {r.objective === "win" ? "max win" : "max pts"}
                          </span>
                          {priced < -0.005 && (
                            <div className="mt-0.5 font-mono text-[10px] text-muted-foreground">
                              paid {priced.toFixed(2)} EV
                            </div>
                          )}
                        </td>
                        <td className="px-3 py-2 text-right font-mono tabular-nums">
                          {r.baselineActualPoints != null
                            ? r.baselineActualPoints.toFixed(0)
                            : r.baselineExpectedPoints?.toFixed(1) ?? "—"}
                          {r.baselineActualPoints == null && (
                            <span className="ml-1 text-[9px] uppercase text-muted-foreground">exp</span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-right font-mono tabular-nums">
                          {r.recommendedActualPoints != null
                            ? r.recommendedActualPoints.toFixed(0)
                            : r.recommendedExpectedPoints?.toFixed(1) ?? "—"}
                          {r.recommendedActualPoints == null && (
                            <span className="ml-1 text-[9px] uppercase text-muted-foreground">exp</span>
                          )}
                        </td>
                        <td
                          className={`px-3 py-2 text-right font-mono tabular-nums ${
                            delta == null
                              ? "text-muted-foreground"
                              : delta > 0
                                ? "text-emerald-600 dark:text-emerald-400"
                                : delta < 0
                                  ? "text-rose-600 dark:text-rose-400"
                                  : "text-muted-foreground"
                          }`}
                        >
                          {delta == null ? "—" : `${delta >= 0 ? "+" : ""}${delta.toFixed(0)}`}
                        </td>
                        <td className="px-3 py-2 text-right font-mono tabular-nums text-muted-foreground">
                          {r.brier != null ? r.brier.toFixed(3) : "—"}
                        </td>
                        <td className="px-3 py-2">
                          <span className="font-mono text-[10px] uppercase text-muted-foreground">
                            {r.status === "settled"
                              ? "settled"
                              : `${r.gamesGraded}/${r.gamesTotal} graded`}
                          </span>
                        </td>
                        <td className="px-3 py-2">
                          {r.gamesGraded === 0 ? (
                            <span className="text-xs text-muted-foreground">—</span>
                          ) : r.wonPool != null ? (
                            <span
                              className={`font-mono text-xs ${
                                r.wonPool
                                  ? "text-emerald-600 dark:text-emerald-400"
                                  : "text-muted-foreground"
                              }`}
                            >
                              {r.wonPool ? "WON" : `lost${r.finishRank ? ` (#${r.finishRank})` : ""}`}
                            </span>
                          ) : (
                            <div className="flex items-center gap-1">
                              <input
                                type="number"
                                min={1}
                                placeholder="rank"
                                className="h-7 w-16 rounded border bg-background px-1.5 text-xs"
                                onKeyDown={(e) => {
                                  if (e.key !== "Enter") return;
                                  const rank = Number((e.target as HTMLInputElement).value);
                                  if (!Number.isFinite(rank) || rank < 1) return;
                                  run(() =>
                                    recordPickemFinish({
                                      recommendationId: r.id,
                                      finishRank: rank,
                                      winningScore: null,
                                      wonPool: rank === 1,
                                    }),
                                  );
                                }}
                                title="Your finishing position, then Enter. Rank 1 records a win."
                              />
                              <button
                                onClick={() => run(() => voidPickemRecommendation(r.id))}
                                className="text-[10px] text-muted-foreground hover:underline"
                                title="Exclude this row from every summary. It stays in the ledger."
                              >
                                void
                              </button>
                            </div>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {ledger.length > liveLedger.length && (
              <p className="border-t px-3 py-2 text-xs text-muted-foreground">
                {ledger.length - liveLedger.length} superseded or voided row
                {ledger.length - liveLedger.length === 1 ? "" : "s"} kept in the ledger and excluded
                from every number above. Changing a recommendation appends; it never overwrites.
              </p>
            )}
          </>
        )}
      </section>

      {/* ---- the strategy, written out ----------------------------------- */}
      <section className="rounded-lg border bg-card p-4">
        <h2 className="mb-3 text-sm font-semibold">The strategy, and what it rests on</h2>
        <div className="grid gap-4 text-sm text-muted-foreground lg:grid-cols-2">
          <div className="space-y-2">
            <h3 className="font-semibold text-foreground">Proved — no assumptions</h3>
            <p>
              <strong className="text-foreground">1. The max-points entry is a sort.</strong> Expected
              points are <code className="font-mono">Σ cᵢpᵢ</code> over a permutation of the weights.
              The rearrangement inequality says that sum is largest when the biggest weight meets the
              biggest probability. So &quot;rank the games by win probability&quot; is not a heuristic,
              it is the answer — and there is nothing else to find.
            </p>
            <p>
              <strong className="text-foreground">2. That is exactly the problem.</strong> Everyone
              competent submits the same entry. A pool of correct sorters is decided by whose coin
              flips landed, not by whose analysis was better. Your win probability is roughly
              1/N, whatever N is.
            </p>
            <p>
              <strong className="text-foreground">3. Deviation has an exact price.</strong> Swapping
              two confidence weights costs <code className="font-mono">(cᵢ−cⱼ)(pᵢ−pⱼ)</code>; flipping a
              side costs <code className="font-mono">c(2p−1)</code>. Nothing about those numbers is
              estimated, which is why every move this page suggests arrives with its bill attached.
            </p>
            <p>
              <strong className="text-foreground">4. Prize share has a closed form.</strong> Given one
              rival&apos;s score distribution, the expected share against R rivals is{" "}
              <code className="font-mono">((x+y)^(R+1) − y^(R+1)) / ((R+1)x)</code> where y is
              P(rival below you) and x is P(rival level). So a 5,000-entry pool costs no more to
              evaluate than a 20-entry one.
            </p>
          </div>
          <div className="space-y-2">
            <h3 className="font-semibold text-foreground">Modeled — and where it can be wrong</h3>
            <p>
              <strong className="text-foreground">5. Pool size sets the exchange rate.</strong> To win
              you must beat the best of N−1 others. As N grows that bar rises toward a near-perfect
              card, so the value of variance rises and the value of a point falls. Below about{" "}
              {DEVIATION_MIN_POOL} entries the field&apos;s best score lands near your own and paying
              points for variance is just paying points. This is Clair &amp; Letscher&apos;s result;
              it reproduces here, but reproducing inside your own simulator is not evidence.
            </p>
            <p>
              <strong className="text-foreground">6. Differentiate where it is cheap.</strong> The flat
              middle of the slate, not the top. Never give up the game you are most sure of to look
              clever — it is the most expensive separation on the board.
            </p>
            <p>
              <strong className="text-foreground">7. Season-long is a different game.</strong> Over 18
              weeks weekly variance largely averages out while the EV you paid for it does not. Play
              near the max-points entry until you are behind late.
            </p>
            <p className="rounded border border-amber-500/40 bg-amber-500/5 p-2 text-foreground">
              <strong>The honest gap.</strong> The field model — how much harder the public backs
              favourites (currently {favoriteBias.toFixed(2)}) and how much opponents&apos; rankings
              scatter ({FIELD_SKILL_SIGMA}) — is a stated prior, not a measurement. There is no
              pick&apos;em pick-share feed in this repo, and the survivor popularity feed is a
              different distribution that it would be wrong to substitute. No pick&apos;em entry here
              has ever been settled, so none of section two has been graded. Set the bias to 1.00
              below to see the conservative case, where the field mirrors the market and contrarian
              value nearly vanishes.
            </p>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3 border-t pt-3">
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            Field favourite bias
            <input
              type="range"
              min={1}
              max={2}
              step={0.05}
              value={favoriteBias}
              onChange={(e) => setFavoriteBias(Number(e.target.value))}
              className="w-40"
            />
            <span className="font-mono tabular-nums">{favoriteBias.toFixed(2)}</span>
          </label>
          <span className="text-xs text-muted-foreground">
            {favoriteBias <= 1.001
              ? "The field mirrors the market — the conservative case."
              : `A 60% favourite draws ${pct(1 / (1 + Math.exp(-favoriteBias * Math.log(0.6 / 0.4))), 0)} of the field.`}
          </span>
        </div>

        <p className="mt-3 border-t pt-3 text-xs text-muted-foreground">
          Win probabilities come from <code className="font-mono">nfl_game_win_probs</code>, the same
          table the survivor page reads, with tie probability renormalised away (a pool scores two
          outcomes, not three). Simulation: {DEFAULT_SIMS.toLocaleString()} slates,{" "}
          {SAMPLE_OPPONENTS} sampled opponents per slate, common random numbers so candidate entries
          are compared under identical draws. Model {slate.modelVersion ?? "—"}, computed{" "}
          {slate.computedAt ?? "—"}. Page loaded {loadedAt}.
        </p>
      </section>
    </div>
  );
}

function Stat({
  label,
  value,
  sub,
  highlight,
}: {
  label: string;
  value: string;
  sub: string;
  highlight?: boolean;
}) {
  return (
    <div className={`rounded-lg border bg-card p-3 ${highlight ? "border-emerald-500/40" : ""}`}>
      <div className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className="mt-1 text-2xl font-bold tabular-nums">{value}</div>
      <div className="mt-0.5 text-xs text-muted-foreground">{sub}</div>
    </div>
  );
}
