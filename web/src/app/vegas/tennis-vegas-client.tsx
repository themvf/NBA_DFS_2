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
const americanToDecimal = (ml: number | null) => ml == null ? null : 1 + (ml > 0 ? ml / 100 : 100 / Math.abs(ml));
const bookLabel = (key: string | null) => ({
  draftkings: "DK", betmgm: "BetMGM", fanatics: "Fanatics",
  williamhill_us: "Caesars", fanduel: "FanDuel", betrivers: "BetRivers",
}[key ?? ""] ?? key ?? "—");

function BestPrice({ book, odds, decimal, dkOdds }: {
  book: string | null; odds: number | null; decimal: number | null; dkOdds: number | null;
}) {
  if (book == null || odds == null) return <span className="text-muted-foreground">No executable quote</span>;
  const dkDecimal = americanToDecimal(dkOdds);
  const lift = decimal != null && dkDecimal != null ? (decimal / dkDecimal - 1) * 100 : null;
  return (
    <span className="inline-flex flex-wrap items-center justify-end gap-1 tabular-nums" title="Best latest pre-match moneyline among the approved executable sportsbooks">
      <span className="rounded bg-[#c7ff3d]/25 px-1.5 py-0.5 font-semibold text-foreground">{bookLabel(book)} {fmtMl(odds)}</span>
      {lift != null && lift > 0.05 && <span className="text-[9px] font-semibold text-emerald-600">+{lift.toFixed(1)}% payout vs DK</span>}
    </span>
  );
}

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

function editorialRead(m: TennisMatchRow) {
  if (m.homeWinProb == null || m.ourProbHome == null) {
    return {
      kicker: "Market still forming",
      headline: "The number needs more evidence.",
      detail: "Model or consensus probability is not available yet. Keep the match visible without pretending the read is complete.",
    };
  }

  const edge = m.ourProbHome - m.homeWinProb;
  const magnitude = Math.abs(edge);
  const favored = m.homeWinProb >= 0.5 ? m.homePlayer : m.awayPlayer;
  if (magnitude <= 0.01) {
    return {
      kicker: "Low disagreement",
      headline: `${favored} controls the number.`,
      detail: "The market and model arrive at nearly the same probability. The matchup is legible; the pricing gap is not.",
    };
  }

  const modelSide = edge > 0 ? m.homePlayer : m.awayPlayer;
  return {
    kicker: `${(magnitude * 100).toFixed(1)} point disagreement`,
    headline: `The model sees more in ${modelSide}.`,
    detail: `This is a calibration signal, not a bet recommendation. Watch whether the market moves toward the model before first serve.`,
  };
}

function CourtProbability({ m }: { m: TennisMatchRow }) {
  const marketHome = m.homeWinProb ?? 0.5;
  const modelHome = m.ourProbHome;
  const marker = modelHome == null ? null : Math.min(96, Math.max(4, modelHome * 100));

  return (
    <div className="relative min-h-[290px] overflow-hidden bg-[#71963f] text-[#f7ffe9] sm:min-h-[360px]">
      <div className="absolute inset-4 border-2 border-[#f7ffe9]/80 sm:inset-5" />
      <div className="absolute inset-y-4 left-1/2 w-0.5 -translate-x-1/2 bg-[#f7ffe9]/75 sm:inset-y-5" />
      <div className="absolute inset-x-4 top-1/2 h-1.5 -translate-y-1/2 bg-[#263719]/55 sm:inset-x-5" />
      <div className="absolute inset-y-[27%] left-4 right-4 border-y-2 border-[#f7ffe9]/70 sm:left-5 sm:right-5" />

      <div className="absolute left-7 top-7 z-10 text-[10px] font-semibold uppercase tracking-[0.18em] sm:left-9 sm:top-9">
        Consensus win probability
      </div>
      <div className="absolute left-7 top-1/2 z-10 -translate-y-1/2 sm:left-9">
        <div className="font-serif text-6xl leading-none tracking-[-0.08em] sm:text-8xl">
          {(marketHome * 100).toFixed(0)}
        </div>
        <div className="mt-1 text-[10px] uppercase tracking-[0.14em]">{m.homePlayer}</div>
      </div>

      {marker != null && (
        <div className="absolute inset-y-4 z-10 w-px bg-[#f7ffe9]/85 sm:inset-y-5" style={{ left: `${marker}%` }}>
          <span className="absolute bottom-4 left-1/2 -translate-x-1/2 whitespace-nowrap rounded-full bg-[#f7ffe9] px-2.5 py-1.5 text-[9px] font-bold uppercase tracking-wide text-[#1c2b10]">
            Model {(modelHome! * 100).toFixed(0)}%
          </span>
        </div>
      )}

      <div className="absolute bottom-7 right-7 z-10 text-right text-[10px] uppercase tracking-[0.12em] sm:bottom-9 sm:right-9">
        <div>{m.nBooks ?? "—"} books</div>
        <div className="mt-1 opacity-75">vig removed</div>
      </div>
    </div>
  );
}

function FeaturedMatch({ m }: { m: TennisMatchRow }) {
  const read = editorialRead(m);
  const homeEloVal = blendedElo(m.homeElo, m.homeGrassElo, m.homeGrassMatches);
  const awayEloVal = blendedElo(m.awayElo, m.awayGrassElo, m.awayGrassMatches);
  const edge = m.ourProbHome != null && m.homeWinProb != null ? m.ourProbHome - m.homeWinProb : null;

  return (
    <section className="overflow-hidden rounded-[1.4rem] bg-card shadow-[0_24px_70px_rgba(0,0,0,0.14)]">
      <div className="grid lg:grid-cols-[minmax(0,0.9fr)_minmax(320px,1.28fr)_minmax(220px,0.72fr)]">
        <div className="order-2 p-6 lg:order-1 lg:p-7">
          <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">
            {fmtTime(m.commenceTime) || "Time TBD"} · {m.tour} · {m.matchDate}
          </div>
          <div className="mt-5 border-b pb-5">
            <div className="font-serif text-3xl tracking-[-0.04em]">{m.homePlayer}</div>
            <div className="mt-2 flex items-center justify-between text-xs text-muted-foreground">
              <span>Elo {homeEloVal ?? "—"}</span><BestPrice book={m.bestHomeBook} odds={m.bestHomeMl} decimal={m.bestHomeDecimal} dkOdds={m.dkHomeMl} />
            </div>
          </div>
          <div className="py-5">
            <div className="font-serif text-3xl tracking-[-0.04em]">{m.awayPlayer}</div>
            <div className="mt-2 flex items-center justify-between text-xs text-muted-foreground">
              <span>Elo {awayEloVal ?? "—"}</span><BestPrice book={m.bestAwayBook} odds={m.bestAwayMl} decimal={m.bestAwayDecimal} dkOdds={m.dkAwayMl} />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-2 border-t pt-4 text-xs">
            <div><span className="block text-[10px] uppercase tracking-wide text-muted-foreground">Total games</span><strong>{m.totalGamesLine ?? "—"}</strong></div>
            <div><span className="block text-[10px] uppercase tracking-wide text-muted-foreground">Set handicap</span><strong>{m.setHandicap != null && m.setHandicap > 0 ? `+${m.setHandicap}` : m.setHandicap ?? "—"}</strong></div>
          </div>
        </div>

        <div className="order-1 lg:order-2"><CourtProbability m={m} /></div>

        <aside className="order-3 flex min-h-[280px] flex-col p-6 lg:p-7">
          <span className="w-fit rounded-full bg-[#c7ff3d] px-2.5 py-1 text-[9px] font-bold uppercase tracking-[0.12em] text-[#152500]">
            {read.kicker}
          </span>
          <h2 className="mt-4 font-serif text-3xl leading-[0.94] tracking-[-0.045em]">{read.headline}</h2>
          <div className="mt-6 space-y-3 text-xs">
            <div className="flex justify-between border-b pb-3"><span className="text-muted-foreground">Market split</span><strong>{fmtPct(m.homeWinProb)} / {fmtPct(m.awayWinProb)}</strong></div>
            <div className="flex justify-between border-b pb-3"><span className="text-muted-foreground">Model gap</span><strong>{fmtSignedPp(edge)}</strong></div>
            <div className="flex justify-between border-b pb-3"><span className="text-muted-foreground">Coverage</span><strong>{m.nBooks ?? "—"} books</strong></div>
          </div>
          <p className="mt-auto pt-6 text-[11px] leading-relaxed text-muted-foreground">{read.detail}</p>
        </aside>
      </div>
    </section>
  );
}

// Secondary matches read like an editorial run-of-show: quiet typographic
// tickets beneath the featured court, not a wall of interchangeable cards.
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
    <article className="group border-t border-border pt-3">
      <div className="flex items-center justify-between gap-2">
        <span className="text-[10px] font-semibold uppercase tracking-[0.13em] text-muted-foreground tabular-nums">
          {fmtTime(m.commenceTime) || "TBD"} · {m.tour}
        </span>
        <SignalBadge signal={signal} />
      </div>

      <div className="mt-4 space-y-2">
        <div className={`flex items-end justify-between gap-3 ${homeFav ? "text-foreground" : "text-muted-foreground"}`}>
          <span className="min-w-0 truncate font-serif text-xl tracking-[-0.035em]">
            {m.homePlayer}
            {homeEloVal != null && <span className="ml-2 font-sans text-[9px] tracking-normal text-muted-foreground tabular-nums">{homeEloVal}</span>}
          </span>
          <span className="text-xs"><BestPrice book={m.bestHomeBook} odds={m.bestHomeMl} decimal={m.bestHomeDecimal} dkOdds={m.dkHomeMl} /></span>
        </div>
        <div className={`flex items-end justify-between gap-3 ${!homeFav ? "text-foreground" : "text-muted-foreground"}`}>
          <span className="min-w-0 truncate font-serif text-xl tracking-[-0.035em]">
            {m.awayPlayer}
            {awayEloVal != null && <span className="ml-2 font-sans text-[9px] tracking-normal text-muted-foreground tabular-nums">{awayEloVal}</span>}
          </span>
          <span className="text-xs"><BestPrice book={m.bestAwayBook} odds={m.bestAwayMl} decimal={m.bestAwayDecimal} dkOdds={m.dkAwayMl} /></span>
        </div>
      </div>

      <div className="mt-4"><DivergingProbBar marketHome={m.homeWinProb} ourHome={m.ourProbHome} /></div>
      <div className="mt-2 flex items-center justify-between text-[9px] uppercase tracking-wide text-muted-foreground tabular-nums">
        <span>{secondary} · {m.nBooks ?? "—"} books</span>
        {edgeH != null && Math.abs(edgeH) > 0.01 && <span className="text-amber-500">{fmtSignedPp(edgeH)} home</span>}
      </div>
    </article>
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
  const featured = filtered[0] ?? null;
  const secondaryByDay = byDay
    .map(([day, matches]) => [day, matches.filter((m) => m.id !== featured?.id)] as const)
    .filter(([, matches]) => matches.length > 0);

  return (
    <div className="mx-auto max-w-7xl space-y-12 p-4 sm:p-6 lg:p-8">
      <header className="grid gap-5 border-b pb-7 sm:grid-cols-[1fr_auto] sm:items-end">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">
            Tennis market journal · {tour === "all" ? "ATP + WTA" : tour}
          </div>
          <h1 className="mt-3 font-serif text-5xl leading-[0.88] tracking-[-0.065em] sm:text-7xl">
            The court<br />is the model.
          </h1>
          <p className="mt-4 max-w-xl text-xs leading-relaxed text-muted-foreground sm:text-sm">
            One matchup at a time. Market consensus, model probability and surface Elo share a single visual field instead of competing in a dashboard.
          </p>
        </div>
        <div className="sm:text-right">
          <div className="text-[10px] font-semibold uppercase tracking-[0.16em] text-muted-foreground">
            {queryDate ?? "Upcoming board"}
          </div>
          <div className="mt-1 font-serif text-2xl tracking-[-0.04em]">{filtered.length} matches in view</div>
        </div>
      </header>

      <div className="flex items-start gap-3 border-y border-amber-500/30 py-3">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-500" />
        <p className="text-[11px] leading-relaxed text-muted-foreground">
          <strong className="font-semibold text-foreground">Calibration, not picks.</strong> Tennis moneyline has no confirmed edge; ratings are capped at 2★. Disagreement is a research signal, never a recommendation.
        </p>
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Featured court</div>
        <div className="flex gap-1.5">
          {([
            ["all", `All ${matchups.length}`],
            ["ATP", `ATP ${atpCount}`],
            ["WTA", `WTA ${wtaCount}`],
          ] as const).map(([key, label]) => (
            <button
              key={key}
              onClick={() => setTour(key as "all" | "ATP" | "WTA")}
              className={`rounded-full border px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wide transition ${
                tour === key
                  ? "border-foreground bg-foreground text-background"
                  : "border-border bg-background text-muted-foreground hover:text-foreground"
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {featured ? (
        <FeaturedMatch m={featured} />
      ) : (
        <div className="border-y py-10 text-sm text-muted-foreground">
          No matches in the feed. Run <code className="rounded bg-muted px-1 py-0.5">python -m ingest.tennis_schedule</code> to seed odds.
        </div>
      )}

      {bets.length > 0 && (() => {
        const top = bets.filter((b) => b.status === "pending" && b.stars >= 3 && (b.edge ?? 0) > 0).slice(0, 12);
        if (top.length === 0) return null;
        return (
          <section>
            <div className="mb-4 flex items-end justify-between gap-3 border-b pb-3">
              <div><div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Model notebook</div><h2 className="mt-1 font-serif text-3xl tracking-[-0.04em]">Rated observations.</h2></div>
              <span className="text-xs text-muted-foreground">{top.length} pending</span>
            </div>
            <div className="grid gap-x-5 gap-y-3 md:grid-cols-2">
              {top.map((b) => (
                <div key={b.id} className="flex flex-wrap items-center gap-x-4 gap-y-1 border-t pt-3">
                  <Stars n={b.stars} />
                  <div className="min-w-0 flex-1"><span className="font-serif text-lg">{b.selectionLabel}</span><span className="ml-2 text-[10px] text-muted-foreground">{b.fixture}</span></div>
                  <div className="flex items-center gap-2 text-[10px] tabular-nums"><span>{fmtPct(b.ourProb)}</span><span className="text-muted-foreground">{fmtPct(b.marketProb)}</span><span className="text-amber-500">{fmtSignedPp(b.edge)}</span><span>{fmtMl(b.marketOdds)}</span></div>
                </div>
              ))}
            </div>
          </section>
        );
      })()}

      {secondaryByDay.length > 0 && (
        <section className="space-y-7">
          <div className="border-b pb-3">
            <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Order of play</div>
            <h2 className="mt-1 font-serif text-4xl tracking-[-0.05em]">The next courts.</h2>
          </div>
          {secondaryByDay.map(([day, matches], dayIndex) => {
            const open = isDayOpen(day, dayIndex);
            return (
              <div key={day} className="space-y-4">
                <button onClick={() => toggleDay(day, dayIndex)} className="flex items-center gap-2 text-left text-[11px] font-semibold uppercase tracking-[0.14em] text-muted-foreground hover:text-foreground">
                  <ChevronRight className={`h-3.5 w-3.5 transition-transform ${open ? "rotate-90" : ""}`} />
                  {fmtDayHeading(day)} <span className="font-normal tabular-nums">{matches.length}</span>
                </button>
                {open && <div className="grid gap-x-6 gap-y-7 sm:grid-cols-2 xl:grid-cols-4">{matches.map((m) => <MatchCard key={m.id} m={m} />)}</div>}
              </div>
            );
          })}
        </section>
      )}

      <section className="space-y-6 border-t pt-10">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Evidence below the live board</div>
          <h2 className="mt-1 font-serif text-4xl tracking-[-0.05em]">The research desk.</h2>
        </div>
        <LineMovementPanel
          rows={sportsbookMovement}
          cadenceNote="the 6-hourly sportsbook captures"
          lane="sportsbook"
          researchOnly
        />
        <LineMovementPanel
          rows={polymarketMovement}
          cadenceNote="the independent Polymarket captures"
          lane="polymarket"
        />
        {(lineAlerts.length > 0 || lineAlertBacktest.length > 0) && <LineAlertsPanel alerts={lineAlerts} backtest={lineAlertBacktest} tennisResearch />}
        <DetectorHealthPanel health={detectorHealth} />
        <TennisSurfaceEvidence dashboard={eloDashboard} />
      </section>

      <section className="border-t pt-10">
        <TennisResults bets={bets} backtest={backtest} legacyBetSummary={legacyBetSummary} />
      </section>
    </div>
  );
}
