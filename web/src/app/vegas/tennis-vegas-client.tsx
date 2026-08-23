"use client";

import { AlertTriangle, ChevronRight } from "lucide-react";
import { useMemo, useState } from "react";
import type { TennisMatchRow, TennisBetRow, TennisBetBacktestRow, TennisLegacyBetSummary, TennisEloDashboard, MlbLineMovementRow, LineAlertRow, LineAlertBacktestRow, DetectorHealthRow } from "@/db/queries";
import LineMovementPanel from "./line-movement-panel";
import LineAlertsPanel from "./line-alerts-panel";
import DetectorHealthPanel from "./detector-health-panel";
import TennisSurfaceEvidence from "./tennis-surface-evidence";

const fmtMl = (ml: number | null) => (ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml));
const fmtPct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(0)}%`);
const fmtSignedPp = (v: number | null) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(0)}%`);

function Stars({ n }: { n: number }) {
  return (
    <span className="tabular-nums tracking-tight text-amber-400" title={`${n} of 5 stars`}>
      {"★".repeat(n)}
      <span className="text-muted-foreground/40">{"★".repeat(5 - n)}</span>
    </span>
  );
}

// Mirror the Python _blended_elo logic (grass_matches >= 20 → pure grass, else lerp).
function blendedElo(overall: number | null, grassElo: number | null, grassMatches: number | null): number | null {
  if (overall == null) return null;
  if (grassElo == null || grassMatches == null || grassMatches < 5) return overall;
  const t = Math.min(grassMatches / 20, 1);
  return Math.round(overall * (1 - t) + grassElo * t);
}

function fmtTime(iso: string | null): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
}

function fmtDayHeading(date: string): string {
  const d = new Date(`${date}T00:00:00`);
  if (Number.isNaN(d.getTime())) return date;
  return d.toLocaleDateString(undefined, { weekday: "long", month: "long", day: "numeric" });
}

// Diverging market bar with our model's home probability overlaid as a tick,
// rather than as a second stacked number — the model number IS its position
// on the market's own bar, so the disagreement is a single visual offset.
function DivergingProbBar({ marketHome, ourHome }: { marketHome: number | null; ourHome: number | null }) {
  if (marketHome == null) return <div className="h-1.5 w-full rounded bg-muted" />;
  const tickPct = ourHome != null ? Math.min(99, Math.max(1, ourHome * 100)) : null;
  return (
    <div className={tickPct != null ? "relative pt-3" : ""}>
      {tickPct != null && (
        <div
          className="absolute top-0 -translate-x-1/2 text-[9px] font-semibold tabular-nums"
          style={{ left: `${tickPct}%` }}
        >
          {(ourHome! * 100).toFixed(0)}%
        </div>
      )}
      <div className="flex h-1.5 w-full overflow-hidden rounded">
        <div style={{ width: `${marketHome * 100}%` }} className="bg-blue-500" />
        <div style={{ width: `${(1 - marketHome) * 100}%` }} className="bg-rose-500" />
      </div>
      {tickPct != null && (
        <div
          className="absolute top-3 h-2.5 w-0.5 -translate-x-1/2 bg-foreground"
          style={{ left: `${tickPct}%` }}
        />
      )}
    </div>
  );
}

// Signal derived from our-vs-market disagreement. Deliberately never a
// success/actionable color: sky (agree) and gray (no signal) are neutral,
// amber (disagree) is "worth a second look" — never emerald, which this app
// reserves for realized positive outcomes, not a live recommendation. Tennis
// moneyline has no confirmed edge (see memory: tennis-moneyline-no-edge), so
// this badge must never read as "bet this."
type MatchSignal = "agree" | "disagree" | "no_signal";

function matchSignal(m: TennisMatchRow): MatchSignal {
  if (m.ourProbHome == null || m.homeWinProb == null) return "no_signal";
  const edge = m.ourProbHome - m.homeWinProb;
  return Math.abs(edge) > 0.01 ? "disagree" : "agree";
}

function SignalBadge({ signal }: { signal: MatchSignal }) {
  const cls =
    signal === "disagree" ? "bg-amber-500/15 text-amber-500"
    : signal === "agree" ? "bg-sky-500/15 text-sky-400"
    : "bg-muted text-muted-foreground";
  const label = signal === "disagree" ? "Disagree" : signal === "agree" ? "Agree" : "No signal";
  return <span className={`shrink-0 rounded px-1.5 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${cls}`}>{label}</span>;
}

// One match, one card — replaces the old 9-column table row (with its two
// stacked lines per cell) with a single scannable unit that reflows to one
// column on mobile instead of forcing horizontal scroll.
function MatchCard({ m }: { m: TennisMatchRow }) {
  const homeFav = (m.homeWinProb ?? 0) >= (m.awayWinProb ?? 0);
  const signal = matchSignal(m);
  const edgeH = m.ourProbHome != null && m.homeWinProb != null ? m.ourProbHome - m.homeWinProb : null;
  const homeEloVal = blendedElo(m.homeElo, m.homeGrassElo, m.homeGrassMatches);
  const awayEloVal = blendedElo(m.awayElo, m.awayGrassElo, m.awayGrassMatches);
  const secondary = [
    m.totalGamesLine != null ? `Games ${m.totalGamesLine}` : null,
    m.setHandicap != null ? `Hcap ${m.setHandicap > 0 ? `+${m.setHandicap}` : m.setHandicap}` : null,
  ].filter(Boolean).join(" · ") || "No total/handicap quote";

  return (
    <div className="flex flex-col gap-2.5 rounded-lg border bg-card p-3.5">
      <div className="flex items-center justify-between gap-2">
        <span className="text-[11px] text-muted-foreground tabular-nums">{fmtTime(m.commenceTime)} · {m.tour}</span>
        <SignalBadge signal={signal} />
      </div>

      <div className="flex flex-col gap-1 text-sm">
        <div className={`flex items-baseline justify-between gap-2 ${homeFav ? "font-semibold" : ""}`}>
          <span className="truncate">
            {m.homePlayer}
            {homeEloVal != null && <span className="ml-1.5 text-[10px] font-normal text-muted-foreground tabular-nums">{homeEloVal}</span>}
          </span>
          <span className="tabular-nums">{fmtMl(m.homeMl)}</span>
        </div>
        <div className={`flex items-baseline justify-between gap-2 ${!homeFav ? "font-semibold" : "text-muted-foreground"}`}>
          <span className="truncate">
            {m.awayPlayer}
            {awayEloVal != null && <span className="ml-1.5 text-[10px] font-normal text-muted-foreground tabular-nums">{awayEloVal}</span>}
          </span>
          <span className="tabular-nums">{fmtMl(m.awayMl)}</span>
        </div>
      </div>

      <DivergingProbBar marketHome={m.homeWinProb} ourHome={m.ourProbHome} />

      <div className="flex items-center justify-between text-[10px] text-muted-foreground tabular-nums">
        <span>Market {fmtPct(m.homeWinProb)} / {fmtPct(m.awayWinProb)}</span>
        {edgeH != null && Math.abs(edgeH) > 0.01 && (
          <span className="text-amber-500">{fmtSignedPp(edgeH)} home</span>
        )}
      </div>

      <div className="border-t pt-2 text-[10px] text-muted-foreground tabular-nums">
        {secondary} · {m.nBooks ?? "—"} books
      </div>
    </div>
  );
}

function StatusPill({ status }: { status: string }) {
  const cls =
    status === "won" ? "bg-emerald-500/15 text-emerald-400"
    : status === "lost" ? "bg-rose-500/15 text-rose-400"
    : status === "void" ? "bg-sky-500/15 text-sky-400"
    : "bg-muted text-muted-foreground";
  return <span className={`rounded px-1.5 py-0.5 text-[10px] font-medium uppercase ${cls}`}>{status}</span>;
}

// ── Results & calibration ─────────────────────────────────────────────────────
// Renders its full structure even before any bet is settled, so the analytics
// surface exists from day one. Realized win%/ROI populate once the tennis-data.co.uk
// settlement job grades bets (status won/lost).
export function TennisResults({
  bets,
  backtest,
  legacyBetSummary,
}: {
  bets: TennisBetRow[];
  backtest: TennisBetBacktestRow[];
  legacyBetSummary: TennisLegacyBetSummary;
}) {
  const isLegacy = (b: TennisBetRow) =>
    legacyBetSummary.currentModelVersion != null && b.modelVersion !== legacyBetSummary.currentModelVersion;
  const settled = bets.filter((b) => b.status === "won" || b.status === "lost");
  const won = settled.filter((b) => b.status === "won").length;
  const lost = settled.filter((b) => b.status === "lost").length;
  const pending = bets.filter((b) => b.status === "pending").length;
  const winRate = won + lost > 0 ? won / (won + lost) : null;
  const marketSettled = settled.filter((b) => b.marketOdds != null);
  const profit = marketSettled.reduce(
    (s, b) => s + (b.status === "won" ? (b.marketOdds! > 0 ? b.marketOdds! / 100 : 100 / Math.abs(b.marketOdds!)) : -1),
    0,
  );
  const roi = marketSettled.length > 0 ? profit / marketSettled.length : null;

  const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
  const roiStr = (v: number | null) => (v == null ? "—" : `${v >= 0 ? "+" : ""}${(v * 100).toFixed(1)}%`);

  // Aggregate 'All' rollup client-side.
  const totalN = backtest.reduce((s, r) => s + r.n, 0);

  return (
    <section className="space-y-4">
      <h2 className="text-lg font-bold">📈 Results &amp; Calibration</h2>

      {/* Overall summary strip */}
      <div className="flex flex-wrap items-center gap-4 rounded-lg border bg-card px-4 py-3 text-sm">
        <div>
          <span className="text-muted-foreground text-xs">Settled</span>
          <span className="ml-1.5 font-bold tabular-nums">{settled.length}</span>
          <span className="ml-1 text-xs text-muted-foreground">({won}W · {lost}L)</span>
        </div>
        <div className="h-4 w-px bg-border hidden sm:block" />
        <div>
          <span className="text-muted-foreground text-xs">Win rate</span>
          <span className={`ml-1.5 font-bold tabular-nums ${winRate != null && winRate >= 0.5 ? "text-emerald-400" : winRate != null ? "text-rose-400" : "text-muted-foreground"}`}>
            {pct(winRate)}
          </span>
        </div>
        <div className="h-4 w-px bg-border hidden sm:block" />
        <div>
          <span className="text-muted-foreground text-xs">ROI</span>
          <span className={`ml-1.5 font-bold tabular-nums ${roi != null && roi >= 0 ? "text-emerald-400" : roi != null ? "text-rose-400" : "text-muted-foreground"}`}>
            {roiStr(roi)}
          </span>
          <span className="ml-1 text-xs text-muted-foreground">({marketSettled.length} bets)</span>
        </div>
        <div className="h-4 w-px bg-border hidden sm:block" />
        <div>
          <span className="text-muted-foreground text-xs">Pending</span>
          <span className="ml-1.5 font-bold tabular-nums">{pending}</span>
        </div>
      </div>

      {/* Bet Ledger Backtest — star-tier calibration */}
      <div className="rounded-lg border bg-card p-4">
        <div className="flex flex-wrap items-baseline justify-between gap-2 mb-1">
          <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
            Bet Ledger Backtest — Moneyline
          </h3>
          <span className="text-xs text-muted-foreground">{totalN} settled</span>
        </div>
        <p className="text-xs text-muted-foreground mb-2">
          Calibration on settled bets: realized win% should meet or beat expected (our_prob) in each
          star tier. ROI is priced at each bet&rsquo;s true odds. Populates once tennis-data.co.uk results grade the bets.
        </p>
        {legacyBetSummary.legacySettledCount > 0 && (
          <p className="text-[11px] text-amber-500 mb-2">
            Filtered to {legacyBetSummary.currentModelVersion} (current methodology). {legacyBetSummary.legacySettledCount} settled
            bet{legacyBetSummary.legacySettledCount === 1 ? "" : "s"} from a superseded model version — pooling those in would
            blur two different rating methodologies together — are excluded here but still shown, marked, in the full ledger below.
          </p>
        )}
        {backtest.length === 0 ? (
          <div className="rounded border border-dashed bg-muted/20 p-4 text-center text-xs text-muted-foreground">
            No settled bets yet. The {pending} pending recommendations grade automatically once the
            tennis-data.co.uk results publish (near-daily during a Slam). Star-tier calibration appears here then.
          </div>
        ) : (
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="border-b text-muted-foreground">
                <th className="py-1 text-left">Stars</th>
                <th className="py-1 text-right">n</th>
                <th className="py-1 text-right">Exp win%</th>
                <th className="py-1 text-right">Real win%</th>
                <th className="py-1 text-right">ROI</th>
                <th className="py-1 text-right">Brier</th>
              </tr>
            </thead>
            <tbody>
              {backtest.map((r) => (
                <tr key={r.stars} className={`border-b border-border/40 ${r.stars >= 4 ? "bg-emerald-500/5" : ""}`}>
                  <td className="py-1.5"><Stars n={r.stars} /></td>
                  <td className="py-1.5 text-right tabular-nums text-muted-foreground">{r.n}</td>
                  <td className="py-1.5 text-right tabular-nums text-muted-foreground">{pct(r.expectedWinRate)}</td>
                  <td className={`py-1.5 text-right tabular-nums font-medium ${r.realizedWinRate >= r.expectedWinRate ? "text-emerald-400" : "text-rose-400"}`}>
                    {pct(r.realizedWinRate)}
                  </td>
                  <td className={`py-1.5 text-right tabular-nums ${r.roi != null && r.roi >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                    {roiStr(r.roi)}
                  </td>
                  <td className="py-1.5 text-right tabular-nums text-muted-foreground">{r.brier != null ? r.brier.toFixed(3) : "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Full bet ledger — every rated selection with status */}
      <div>
        <h3 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Full ledger ({bets.length})
        </h3>
        {bets.length === 0 ? (
          <div className="rounded-lg border bg-card p-4 text-sm text-muted-foreground">
            No rated bets yet — run the prediction + rating pipeline.
          </div>
        ) : (
          <div className="overflow-x-auto rounded-lg border bg-card">
            <table className="w-full min-w-[720px] text-sm">
              <thead>
                <tr className="border-b text-[10px] uppercase text-muted-foreground">
                  <th className="px-3 py-2 text-left font-medium">Rating</th>
                  <th className="px-3 py-2 text-left font-medium">Pick</th>
                  <th className="px-3 py-2 text-left font-medium">Match</th>
                  <th className="px-2 py-2 text-center font-medium">Our %</th>
                  <th className="px-2 py-2 text-center font-medium">Mkt %</th>
                  <th className="px-2 py-2 text-center font-medium">Edge</th>
                  <th className="px-2 py-2 text-center font-medium">Odds</th>
                  <th className="px-2 py-2 text-center font-medium">EV</th>
                  <th className="px-2 py-2 text-center font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {bets.map((b) => {
                  const legacy = isLegacy(b);
                  return (
                    <tr key={b.id} className={`border-b last:border-0 hover:bg-accent/40 ${legacy ? "opacity-60" : ""}`}>
                      <td className="px-3 py-2">
                        <Stars n={b.stars} />
                        {legacy && (
                          <span
                            className="ml-1.5 rounded bg-muted px-1 py-0.5 text-[9px] font-medium uppercase text-muted-foreground"
                            title={`Rated under ${b.modelVersion}, a superseded methodology — not comparable to current ${legacyBetSummary.currentModelVersion} ratings`}
                          >
                            legacy
                          </span>
                        )}
                      </td>
                      <td className="px-3 py-2 font-medium">{b.selectionLabel}</td>
                      <td className="px-3 py-2 text-xs text-muted-foreground">{b.fixture}</td>
                      <td className="px-2 py-2 text-center tabular-nums">{fmtPct(b.ourProb)}</td>
                      <td className="px-2 py-2 text-center tabular-nums text-muted-foreground">{fmtPct(b.marketProb)}</td>
                      <td className={`px-2 py-2 text-center tabular-nums ${legacy ? "text-muted-foreground" : (b.edge ?? 0) > 0 ? "text-amber-500" : "text-muted-foreground"}`}>
                        {fmtSignedPp(b.edge)}
                      </td>
                      <td className="px-2 py-2 text-center tabular-nums">{fmtMl(b.marketOdds)}</td>
                      <td className={`px-2 py-2 text-center tabular-nums ${legacy ? "text-muted-foreground" : (b.ev ?? 0) > 0 ? "text-amber-500" : "text-muted-foreground"}`}>
                        {fmtSignedPp(b.ev)}
                      </td>
                      <td className="px-2 py-2 text-center"><StatusPill status={b.status} /></td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}

export default function TennisVegasClient({
  matchups,
  bets,
  backtest,
  legacyBetSummary,
  lineMovement,
  lineAlerts,
  lineAlertBacktest,
  eloDashboard,
  detectorHealth,
  queryDate,
}: {
  matchups: TennisMatchRow[];
  bets: TennisBetRow[];
  backtest: TennisBetBacktestRow[];
  legacyBetSummary: TennisLegacyBetSummary;
  lineMovement: MlbLineMovementRow[];
  lineAlerts: LineAlertRow[];
  lineAlertBacktest: LineAlertBacktestRow[];
  eloDashboard: TennisEloDashboard;
  detectorHealth: DetectorHealthRow[];
  queryDate: string | null;
}) {
  const [tour, setTour] = useState<"all" | "ATP" | "WTA">("all");
  // Which day groups are expanded. A day defaults open only if it's the
  // first (nearest) one and hasn't been explicitly toggled — explicit
  // toggles always win, so re-collapsing "today" sticks.
  const [openDays, setOpenDays] = useState<Record<string, boolean>>({});
  const isDayOpen = (day: string, index: number) => openDays[day] ?? index === 0;
  const toggleDay = (day: string, index: number) =>
    setOpenDays((prev) => ({ ...prev, [day]: !isDayOpen(day, index) }));

  const filtered = useMemo(
    () => (tour === "all" ? matchups : matchups.filter((m) => m.tour === tour)),
    [matchups, tour],
  );

  // Group by match_date for day headings.
  const byDay = useMemo(() => {
    const map = new Map<string, TennisMatchRow[]>();
    for (const m of filtered) {
      const arr = map.get(m.matchDate) ?? [];
      arr.push(m);
      map.set(m.matchDate, arr);
    }
    return Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  }, [filtered]);

  const atpCount = matchups.filter((m) => m.tour === "ATP").length;
  const wtaCount = matchups.filter((m) => m.tour === "WTA").length;

  return (
    <div className="space-y-6 p-4 sm:p-6 max-w-5xl mx-auto">
      {/* Trust banner — replaces the old plain-gray disclaimer paragraph.
          Same underlying finding (tennis-moneyline-no-edge), but the visual
          weight now matches the honesty: this is a research/calibration
          surface, not a recommendation feed. */}
      <div className="flex items-start gap-2 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2.5">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-500" />
        <p className="text-xs leading-relaxed text-amber-600 dark:text-amber-400">
          <strong className="font-semibold">Research / calibration only.</strong> Tennis moneyline has no
          confirmed edge — every rating is capped at 2★ and nothing here is a recommendation to bet.
          Total-games and handicap quotes are tracked for alert research only, not promoted as picks.
        </p>
      </div>

      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h1 className="text-xl font-bold">Tennis 🎾</h1>
        <span className="text-xs text-muted-foreground">
          {matchups.length} matches · consensus across books, vig removed · {queryDate ?? "upcoming"}
        </span>
      </div>

      {lineMovement.length > 0 && (
        <LineMovementPanel rows={lineMovement} cadenceNote="the 6-hourly odds captures" />
      )}
      {(lineAlerts.length > 0 || lineAlertBacktest.length > 0) && (
        <LineAlertsPanel alerts={lineAlerts} backtest={lineAlertBacktest} />
      )}
      <DetectorHealthPanel health={detectorHealth} />

      <TennisSurfaceEvidence dashboard={eloDashboard} />

      {/* Top rated bets — the model's recommendations, best first. Settled bets
          (won/lost/void) are excluded even if high-starred: a bet from a since-
          superseded model version whose match already happened is history, not
          a live recommendation (see memory tennis-moneyline-no-edge). */}
      {bets.length > 0 && (() => {
        const top = bets.filter((b) => b.status === "pending" && b.stars >= 3 && (b.edge ?? 0) > 0).slice(0, 12);
        if (top.length === 0) return null;
        return (
          <div>
            <h2 className="mb-2 text-sm font-semibold text-muted-foreground uppercase tracking-wide">
              Rated ATP moneyline plays (3★+ value) · {top.length}
            </h2>
            <div className="space-y-1.5">
              {top.map((b) => (
                <div
                  key={b.id}
                  className="flex flex-wrap items-center gap-x-4 gap-y-1 rounded-lg border bg-card px-3.5 py-2.5"
                >
                  <Stars n={b.stars} />
                  <div className="min-w-0 flex-1">
                    <span className="font-medium">{b.selectionLabel}</span>
                    <span className="ml-2 text-xs text-muted-foreground">{b.fixture}</span>
                  </div>
                  <div className="flex items-center gap-3 text-xs tabular-nums">
                    <span title="Our probability">{fmtPct(b.ourProb)}</span>
                    <span className="text-muted-foreground" title="Market probability">{fmtPct(b.marketProb)}</span>
                    <span className="text-amber-500" title="Edge">{fmtSignedPp(b.edge)}</span>
                    <span title="Odds">{fmtMl(b.marketOdds)}</span>
                    <span className="text-amber-500" title="Expected value">{fmtSignedPp(b.ev)}</span>
                  </div>
                </div>
              ))}
            </div>
            <p className="mt-1.5 text-[11px] text-muted-foreground">
              Edge = our model prob − vig-free market prob. EV = expected ROI per unit at the offered price.
              Calibration (do these win at the rate we claim?) lands as tennis-data.co.uk results settle the bets.
            </p>
          </div>
        );
      })()}

      {/* Tour filter */}
      <div className="flex gap-1.5">
        {([
          ["all", `All (${matchups.length})`],
          ["ATP", `ATP ${atpCount}`],
          ["WTA", `WTA ${wtaCount}`],
        ] as const).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTour(key as "all" | "ATP" | "WTA")}
            className={`rounded-full border px-3 py-1 text-xs font-medium transition ${
              tour === key
                ? "border-foreground bg-foreground text-background"
                : "border-border bg-background text-muted-foreground hover:text-foreground"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {byDay.length === 0 && (
        <div className="rounded-lg border bg-card p-6 text-sm text-muted-foreground">
          No matches in the feed. Run <code className="rounded bg-muted px-1 py-0.5">python -m ingest.tennis_schedule</code> to seed odds.
        </div>
      )}

      {byDay.map(([day, matches], dayIndex) => {
        const open = isDayOpen(day, dayIndex);
        return (
          <div key={day} className="space-y-2">
            <button
              onClick={() => toggleDay(day, dayIndex)}
              className="flex items-center gap-1.5 text-sm font-semibold text-muted-foreground uppercase tracking-wide"
            >
              <ChevronRight className={`h-3.5 w-3.5 transition-transform ${open ? "rotate-90" : ""}`} />
              {fmtDayHeading(day)}
              <span className="text-xs font-normal normal-case tabular-nums">{matches.length}</span>
            </button>
            {open && (
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
                {matches.map((m) => (
                  <MatchCard key={m.id} m={m} />
                ))}
              </div>
            )}
          </div>
        );
      })}

      {/* Results & calibration — renders structure even with zero settled bets */}
      <TennisResults bets={bets} backtest={backtest} legacyBetSummary={legacyBetSummary} />
    </div>
  );
}
