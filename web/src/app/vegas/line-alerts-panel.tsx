"use client";

import type { LineAlertRow, LineAlertBacktestRow } from "@/db/queries";
import { verdict as auditVerdict } from "@/lib/alert-audit-policy";

// Sharp line-movement alerts feed + its running audit (shared across the
// MLB / soccer / tennis vegas views). Every alert is an immutable ledger row
// graded on CLV (did the market close toward the flagged side?) and outcome
// (did the side win?). Sides are translated to actual team names and each
// alert carries an Action chip that says, in plain language, what a user can
// do with it — honestly graded: Pin divergence is a usable PRICE rule today;
// steam stays a WATCH signal until the audit shows it carries CLV.

const sideName = (a: LineAlertRow): string => {
  const [away, home] = a.matchup.split(" @ ");
  if (a.side === "home") return home ?? "home";
  if (a.side === "away") return away ?? "away";
  if (a.side === "draw") return "Draw";
  return a.side; // prop sides carry their own label ("Roki Sasaki K O4.5")
};

function ActionChip({ a, tennisResearch = false }: { a: LineAlertRow; tennisResearch?: boolean }) {
  const name = sideName(a);
  if (a.alertType === "prop_outlier") {
    const d = (a.details ?? {}) as {
      player?: string; dk_odds?: number; median_decimal?: number;
      edge_vs_median_pct?: number; n_books?: number;
    };
    const odds = d.dk_odds != null ? (d.dk_odds > 0 ? `+${d.dk_odds}` : `${d.dk_odds}`) : "?";
    return (
      <span
        className="cursor-help rounded-full bg-gray-200 px-2 py-0.5 text-[10px] font-semibold text-gray-600 line-through decoration-gray-400 whitespace-nowrap"
        title={`RETIRED 2026-08-13 — this detector is a confirmed loser and no longer fires. Settled record at frozen ` +
               `DK prices: n=101, 6 won (5.9%), -65.0u, ROI -64.4%, 95% CI [-85.6%, -44.7%] (entirely below zero). ` +
               `When DK priced a player longer than the normalized median book, DK was right. Historical rows are kept ` +
               `as audit history. Do not act on this chip.`}
      >
        RETIRED · DK {odds} on {d.player} ATGS · +{d.edge_vs_median_pct}% vs median
      </span>
    );
  }
  if (a.alertType === "dk_prop_value" || a.alertType === "prop_line_gap") {
    const d = (a.details ?? {}) as {
      player?: string; bet?: string; line?: number; pin_line?: number;
      dk_odds?: number; ev_pct?: number; gap?: number; market?: string;
    };
    const odds = d.dk_odds != null ? (d.dk_odds > 0 ? `+${d.dk_odds}` : `${d.dk_odds}`) : "?";
    const marketLabel =
      d.market === "pitcher_strikeouts" ? "Strikeouts"
      : d.market === "batter_total_bases" ? "Total Bases"
      : d.market === "pitcher_hits_allowed" ? "Hits Allowed"
      : d.market === "pitcher_earned_runs" ? "Earned Runs"
      : d.market === "pitcher_outs" ? "Outs Recorded"
      : d.market === "total_games" ? "Total Games"
      : d.market ?? "";
    if (a.alertType === "prop_line_gap") {
      return (
        <span
          className="cursor-help rounded-full bg-gray-200 px-2 py-0.5 text-[10px] font-semibold text-gray-600 whitespace-nowrap"
          title={`DEMOTED 2026-08-15 — CONTROL ONLY, DO NOT BET. Same-book same-line CLV over n=439 settled: ` +
                 `-0.13%, 95% CI [-0.25%, -0.04%] — entirely below zero. DraftKings' price moves AGAINST this side ` +
                 `by close, reliably. The detector cannot tell WHICH book is stale, so a large |line gap| carries no ` +
                 `directional information. Still computed and graded as a measurement control (it costs nothing — it ` +
                 `runs on already-captured data), never as a play. LINE disagreement, not a price comparison: DK's ` +
                 `${marketLabel} line (${d.line}) vs Pinnacle's (${d.pin_line}) are DIFFERENT propositions.`}
        >
          CONTROL · line gap: {d.player} {marketLabel} — DK {d.line} vs Pinnacle {d.pin_line}
        </span>
      );
    }
    return (
      <span
        className="cursor-help rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-800 whitespace-nowrap"
        title={`PRICE disagreement at the same line (${d.line}) — a valid same-proposition comparison. DK's price on ` +
               `${d.player} ${marketLabel} ${d.bet} is ${d.ev_pct}% above Pinnacle's vig-free fair value (threshold ≥3%). ` +
               `No model involved. UNPROVEN SIGNAL — pooled MLB prop ROI is +3.46% over n=558 with a 95% CI of ` +
               `[-3.98%, +11.19%], which includes zero. Watch, do not treat as a recommendation.`}
      >
        DK {odds} · {d.player} {marketLabel} {d.bet} {d.line} · +{d.ev_pct}% vs fair
      </span>
    );
  }
  if (a.alertType === "dk_value") {
    const d = (a.details ?? {}) as { dk_odds?: number; ev_pct?: number };
    const odds = d.dk_odds != null ? (d.dk_odds > 0 ? `+${d.dk_odds}` : `${d.dk_odds}`) : "?";
    return (
      <span
        className="cursor-help rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-800 whitespace-nowrap"
        title={`DraftKings is paying ${odds} on ${name} — ${d.ev_pct ?? "?"}% above Pinnacle's vig-free fair value ` +
               `(threshold ≥2%). No model, no prediction: DK's line lags the sharp book. Longshots (decimal ≥ 11) are ` +
               `excluded because de-vig skew fakes EV in the tail. UNPROVEN SIGNAL — the audit table above is the running ` +
               `verdict; it has not cleared a positive-return bar. Watch, do not treat as a recommendation.`}
      >
        DK {odds} · {name} · +{d.ev_pct ?? "?"}% vs fair
      </span>
    );
  }
  if (a.alertType === "pinnacle_divergence") {
    const gap = a.sharpProb != null && a.alertProb != null
      ? ((a.sharpProb - a.alertProb) * 100).toFixed(1) : "?";
    return (
      <span
        className="cursor-help rounded-full bg-indigo-100 px-2 py-0.5 text-[10px] font-semibold text-indigo-700 whitespace-nowrap"
        title={tennisResearch
          ? `RESEARCH ONLY. Pinnacle prices ${name} ${gap}pp above retail consensus, but this candidate did not qualify for the ` +
            `favorite-only prospective cohort with a frozen executable price. Do not treat it as a recommendation.`
          : `Pinnacle prices ${name} ${gap}pp above retail consensus — use this as a price-comparison signal and shop the best available retail quote.`}
      >
        {tennisResearch ? `Research · Pin gap on ${name}` : `Prefer ${name} @ retail`}
      </span>
    );
  }
  if (a.alertType === "pinnacle_favorite_forward") {
    const d = (a.details ?? {}) as {
      exec_book?: string; exec_odds?: number; gap_pp?: number;
      retail_books?: number; forward_test_target?: number;
    };
    const odds = d.exec_odds != null ? (d.exec_odds > 0 ? `+${d.exec_odds}` : `${d.exec_odds}`) : "?";
    const book = d.exec_book?.replaceAll("_", " ") ?? "retail";
    return (
      <span
        className="cursor-help rounded-full bg-violet-100 px-2 py-0.5 text-[10px] font-semibold text-violet-700 whitespace-nowrap"
        title={`PROSPECTIVE RESEARCH ONLY. Pinnacle prices favorite ${name} ${d.gap_pp ?? "?"}pp above a ${d.retail_books ?? "?"}-book retail consensus. ` +
               `The executable price was frozen at trigger time: ${book} ${odds}. This cohort requires ${d.forward_test_target ?? 100} graded, ` +
               `priced matches before review and is not a betting recommendation.`}
      >
        Forward test · {name} @ {book} {odds}
      </span>
    );
  }
  if (a.alertType === "pinnacle_polymarket_delta") {
    const gap = a.sharpProb != null && a.alertProb != null
      ? ((a.sharpProb - a.alertProb) * 100).toFixed(1) : "?";
    return (
      <span
        className="cursor-help rounded-full bg-fuchsia-100 px-2 py-0.5 text-[10px] font-semibold text-fuchsia-700 whitespace-nowrap"
        title={`Pinnacle prices ${name} ${gap}pp above Polymarket — the prediction market disagrees with the sharpest sportsbook. ` +
               `Polymarket is excluded from the retail consensus. This delta tracks whether Pinnacle or Polymarket is right more often.`}
      >
        Pin/Poly gap → {name}
      </span>
    );
  }
  return (
    <span
      className="cursor-help rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-700 whitespace-nowrap"
      title={`Multiple books moved toward ${name} at the same time — the signature of informed money. ` +
             `WATCH signal: don't bet it on faith. The audit above is measuring whether steam alerts keep beating the close; ` +
             `if the Avg CLV turns and stays positive, this chip graduates to actionable.`}
    >
      Watch {name} (steam)
    </span>
  );
}

export default function LineAlertsPanel({
  alerts,
  backtest,
  tennisResearch = false,
}: {
  alerts: LineAlertRow[];
  backtest: LineAlertBacktestRow[];
  tennisResearch?: boolean;
}) {
  const pp = (v: number) => `${v >= 0 ? "+" : ""}${v.toFixed(1)}pp`;
  const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
  const typeLabel = (t: string) =>
    t === "pinnacle_divergence" ? "Pin divergence"
    : t === "pinnacle_favorite_forward" ? "Pin favorite forward test"
    : t === "pinnacle_polymarket_delta" ? "Pin/Poly gap"
    : t === "steam" ? "Steam"
    : t === "walking" ? "Walking"
    : t === "dk_value" ? "DK value"
    : t === "dk_prop_value" ? "DK prop value"
    : t === "prop_line_gap" ? "Prop line gap"
    : t === "prop_outlier" ? "ATGS outlier"
    : t;

  return (
    <div className="rounded-lg border bg-white p-4">
      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-1">
        Sharp Line Alerts
      </h3>
      <p className="text-xs text-gray-500 mb-2">
        <span className="rounded bg-amber-100 px-1 font-semibold text-amber-800">UNPROVEN — RESEARCH ONLY</span>{" "}
        Nothing in this table is a recommendation to bet. Pooled MLB prop performance is +3.46% over n=558 settled with
        a 95% CI of [−3.98%, +11.19%] — it includes zero, so no detector here has demonstrated an edge.
        <br />
        <span className="font-medium">DK value</span> = DraftKings&rsquo; price beats Pinnacle&rsquo;s vig-free fair
        value by ≥2% EV on game lines, ≥3% on player props (longshots excluded; no prediction involved).{" "}
        <span className="font-medium">Line gap</span> = DK&rsquo;s prop line differs from Pinnacle&rsquo;s by ≥1.0 —
        a <em>different proposition</em>, so no price edge is computed for it.
        Alerts freeze the market state at trigger time and are audited below: <span className="font-medium">CLV</span> =
        did the market keep moving toward the flagged side after the alert (positive = the alert was early),{" "}
        <span className="font-medium">win rate vs implied</span> = did flagged sides win more often than their price
        said. An alert type with no positive CLV is noise — this table will say so, and that detector becomes a
        retirement candidate. Verdicts are withheld below 30 graded alerts.
      </p>

      {backtest.length > 0 && (
        <table className="w-full text-xs border-collapse mb-3">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">Audit</th>
              <th className="py-1 text-right">n</th>
              <th className="py-1 text-right">Avg CLV</th>
              <th className="py-1 text-right">Beat close</th>
              <th className="py-1 text-right" title="Settled record of the flagged side: wins-losses-pushes">W-L-P</th>
              <th className="py-1 text-right">Win rate</th>
              <th className="py-1 text-right">Implied</th>
              <th className="py-1 text-right" title="Cumulative units from staking 1u at the price FROZEN on each settled alert, for every alert type that carries one. Denominator is settled-alerts-with-a-frozen-price, shown as n. If more than one execution book contributed, the column says so — it is never labelled '@ DK' once it is mixed.">ROI @ frozen price</th>
              <th className="py-1 text-right">Verdict</th>
            </tr>
          </thead>
          <tbody>
            {backtest.map((b) => {
              // Floor and verdict come from @/lib/alert-audit-policy — the
              // same module the NFL table uses. They previously disagreed:
              // this panel enforced a floor, the NFL page's own inline copy
              // enforced none and published a rate off a 2-6 record. The
              // policy is no longer inlined anywhere, so they cannot diverge.
              const verdict = auditVerdict(b);
              return (
                <tr key={b.alertType} className="border-b border-gray-50">
                  <td className="py-1 font-medium">{typeLabel(b.alertType)}</td>
                  <td className="py-1 text-right text-gray-500">{b.n}</td>
                  <td className={`py-1 text-right tabular-nums ${
                    (b.avgClvPp ?? 0) > 0 ? "text-emerald-600" : (b.avgClvPp ?? 0) < 0 ? "text-red-500" : "text-gray-400"
                  }`}>
                    {b.nClv > 0 && b.avgClvPp != null ? pp(b.avgClvPp) : "—"}
                  </td>
                  <td className="py-1 text-right tabular-nums text-gray-500">{pct(b.beatClose)}</td>
                  <td className="py-1 text-right tabular-nums text-gray-600">
                    {b.nOutcomes > 0 ? `${b.wins}-${b.losses}${b.pushes > 0 ? `-${b.pushes}` : ""}` : "—"}
                  </td>
                  <td className="py-1 text-right tabular-nums">{pct(b.winRate)}</td>
                  <td className="py-1 text-right tabular-nums text-gray-400">{pct(b.impliedRate)}</td>
                  {/* Denominator MUST be nFrozenPrice (settled alerts carrying a
                      frozen price), not nOutcomes (all settled alerts) — dkUnits
                      is filtered to the former, so nOutcomes understates ROI
                      whenever any settled alert lacks a price. Positive is NOT
                      green: green is reserved for passed validation gates, and
                      no detector here has cleared one. */}
                  <td className={`py-1 text-right tabular-nums ${
                    b.dkUnits == null ? "text-gray-300" : b.dkUnits < 0 ? "text-red-500" : "text-gray-900 font-medium"
                  }`}
                      title={b.nExecBooks > 1
                        ? `Mixed-book: ${b.nExecBooks} different execution books contributed. Not a single-book ROI.`
                        : undefined}>
                    {b.dkUnits != null && b.nFrozenPrice > 0
                      ? `${b.dkUnits >= 0 ? "+" : ""}${b.dkUnits.toFixed(1)}u `
                        + `(${((b.dkUnits / b.nFrozenPrice) * 100).toFixed(0)}%, n=${b.nFrozenPrice}`
                        + `${b.nExecBooks > 1 ? `, ${b.nExecBooks} books` : ""})`
                      : "—"}
                  </td>
                  <td className="py-1 text-right">
                    <span title={verdict.tip}
                          className={`cursor-help rounded-full px-2 py-0.5 text-[10px] font-semibold ${verdict.cls}`}>
                      {verdict.label}
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      {alerts.length === 0 ? (
        <div className="rounded bg-gray-50 border px-3 py-2 text-xs text-gray-500">
          No alerts yet — the scanner runs after every odds capture.
        </div>
      ) : (
        <table className="w-full text-xs border-collapse">
          <thead>
            <tr className="border-b text-gray-500">
              <th className="py-1 text-left">When (UTC)</th>
              <th className="py-1 text-left">Game</th>
              <th className="py-1 text-left">Action</th>
              <th className="py-1 text-right">Retail</th>
              <th className="py-1 text-right">Sharp</th>
              <th className="py-1 text-right">CLV since alert</th>
              <th className="py-1 text-right">Result</th>
            </tr>
          </thead>
          <tbody>
            {alerts.map((a, i) => (
              <tr key={i} className={`border-b border-gray-50 ${a.outcome == null && a.clvPp == null ? "bg-amber-50/40" : ""}`}>
                <td className="py-1 text-gray-500 whitespace-nowrap">{a.createdAt.slice(5, 16)}</td>
                <td className="py-1">{a.matchup}</td>
                <td className="py-1">
                  <div className="flex items-center gap-1.5">
                    <ActionChip a={a} tennisResearch={tennisResearch} />
                    {(a.details as Record<string, unknown>)?.poly_confirmed === true && (
                      <span className="rounded-full bg-purple-100 px-1.5 py-0.5 text-[9px] font-bold text-purple-700" title="Polymarket independently confirms this direction — its price already moved the same way before this alert fired">Poly ✓</span>
                    )}
                    {(a.details as Record<string, unknown>)?.poly_confirmed === false && (
                      <span className="rounded-full bg-gray-100 px-1.5 py-0.5 text-[9px] font-medium text-gray-500" title="Polymarket does NOT confirm — its price disagrees with this alert's direction">Poly ✗</span>
                    )}
                  </div>
                </td>
                <td className="py-1 text-right tabular-nums"
                    title={`Retail consensus P(${sideName(a)}) when the alert fired`}>{pct(a.alertProb)}</td>
                <td className="py-1 text-right tabular-nums text-indigo-600"
                    title={a.alertType === "prop_line_gap"
                      ? `Pinnacle's vig-free probability AT ITS OWN LINE (${(a.details as Record<string, unknown>)?.pin_line}) — a different proposition than DK's line, shown for context only`
                      : `Pinnacle's vig-free P(${sideName(a)}) when the alert fired`}>
                  {a.alertType === "prop_line_gap" && a.details?.pin_line != null
                    ? `${pct(a.sharpProb)} @ ${String(a.details.pin_line)}`
                    : pct(a.sharpProb)}
                </td>
                <td className={`py-1 text-right tabular-nums ${
                  a.clvPp == null ? "text-gray-400" : a.clvPp > 0 ? "text-emerald-600" : "text-red-500"
                }`}
                    title={a.clvPp == null
                      ? "Grades at game start: positive = the market kept moving toward the flagged team after the alert"
                      : `The market ${a.clvPp > 0 ? "kept moving toward" : "moved away from"} ${sideName(a)} between the alert and the close`}>
                  {a.clvPp != null ? pp(a.clvPp) : "open"}
                </td>
                <td className={`py-1 text-right ${
                  a.outcome === "won" ? "text-emerald-600" : a.outcome === "lost" ? "text-red-500" : "text-gray-400"
                }`}
                    title={a.outcome ? `${sideName(a)} ${a.outcome} the game` : "Settles when the game finishes"}>
                  {a.outcome ?? "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
