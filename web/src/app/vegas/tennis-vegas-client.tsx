"use client";

import type { TennisMatchRow, TennisBetRow, TennisBetBacktestRow, TennisLegacyBetSummary, TennisEloDashboard, MlbLineMovementRow, LineAlertRow, LineAlertBacktestRow, DetectorHealthRow } from "@/db/queries";
import LineMovementPanel from "./line-movement-panel";
import LineAlertsPanel from "./line-alerts-panel";
import DetectorHealthPanel from "./detector-health-panel";
import TennisSurfaceEvidence from "./tennis-surface-evidence";
import TennisTerminal from "./tennis-terminal";

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
  sportsbookMovement,
  polymarketMovement,
  lineAlerts,
  lineAlertBacktest,
  eloDashboard,
  detectorHealth,
  scorecard,
  captureHealth,
  queryDate,
}: {
  matchups: TennisMatchRow[];
  bets: TennisBetRow[];
  backtest: TennisBetBacktestRow[];
  legacyBetSummary: TennisLegacyBetSummary;
  sportsbookMovement: MlbLineMovementRow[];
  polymarketMovement: MlbLineMovementRow[];
  lineAlerts: LineAlertRow[];
  lineAlertBacktest: LineAlertBacktestRow[];
  eloDashboard: TennisEloDashboard;
  detectorHealth: DetectorHealthRow[];
  scorecard: import("@/db/queries").MarketSignalScorecardRow[];
  captureHealth: import("@/db/queries").MarketCaptureHealth;
  queryDate: string | null;
}) {
  return <TennisTerminal matchups={matchups} movement={sportsbookMovement} alerts={lineAlerts} queryDate={queryDate} scorecard={scorecard} captureHealth={captureHealth}>
    <LineMovementPanel rows={sportsbookMovement} cadenceNote="scheduled sportsbook captures, increasing to five-minute targets near match start" lane="sportsbook" researchOnly />
    <LineMovementPanel rows={polymarketMovement} cadenceNote="the independent Polymarket captures" lane="polymarket" />
    {(lineAlerts.length > 0 || lineAlertBacktest.length > 0) && <LineAlertsPanel alerts={lineAlerts} backtest={lineAlertBacktest} tennisResearch />}
    <DetectorHealthPanel health={detectorHealth} />
    <TennisSurfaceEvidence dashboard={eloDashboard} />
    <TennisResults bets={bets} backtest={backtest} legacyBetSummary={legacyBetSummary} />
  </TennisTerminal>;
}
