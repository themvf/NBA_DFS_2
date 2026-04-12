"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type {
  VegasMatchupRow,
  OuHitRateRow,
  TeamTotalAccuracyRow,
  SpreadCoverageRow,
  VegasSummaryStatsRow,
} from "@/db/queries";
import { fetchVegasOdds } from "./actions";
import type { Sport } from "@/db/queries";

const fmt1 = (v: number | null | undefined) =>
  v == null ? "—" : v.toFixed(1);
const fmt0 = (v: number | null | undefined) =>
  v == null ? "—" : Math.round(v).toString();
const fmtPct = (v: number | null | undefined) =>
  v == null ? "—" : `${(v * 100).toFixed(1)}%`;
const fmtMl = (ml: number | null) => {
  if (ml == null) return "—";
  return ml > 0 ? `+${ml}` : String(ml);
};

function BiasChip({ bias }: { bias: number | null }) {
  if (bias == null) return <span className="text-gray-400">—</span>;
  const pos = bias > 0;
  return (
    <span className={pos ? "text-red-600" : "text-blue-600"}>
      {pos ? "+" : ""}
      {bias.toFixed(1)}
    </span>
  );
}

type Props = {
  matchups: VegasMatchupRow[];
  ouHitRate: OuHitRateRow[];
  teamTotalAccuracy: TeamTotalAccuracyRow[];
  spreadCoverage: SpreadCoverageRow[];
  vegasSummary: VegasSummaryStatsRow | null;
  queryDate: string;
  sport: Sport;
};

export default function VegasClient({
  matchups,
  ouHitRate,
  teamTotalAccuracy,
  spreadCoverage,
  vegasSummary,
  queryDate,
  sport,
}: Props) {
  const router = useRouter();
  const [dateInput, setDateInput] = useState(queryDate);
  const [isPending, startTransition] = useTransition();
  const [fetchMsg, setFetchMsg] = useState<{ ok: boolean; text: string } | null>(null);

  const handleFetchLines = () => {
    setFetchMsg(null);
    startTransition(async () => {
      const result = await fetchVegasOdds(queryDate, sport);
      setFetchMsg({ ok: result.ok, text: result.message });
      if (result.ok) router.refresh();
    });
  };

  const hasScores = ouHitRate.length > 0 || teamTotalAccuracy.length > 0;

  const handleDateChange = (d: string) => {
    setDateInput(d);
    router.push(`/vegas?date=${d}`);
  };

  // Compute overall O/U stats
  const totalN = ouHitRate.reduce((s, r) => s + r.n, 0);
  const totalOvers = ouHitRate.reduce((s, r) => s + r.overCount, 0);
  const totalUnders = ouHitRate.reduce((s, r) => s + r.underCount, 0);
  const overallOverRate = totalN > 0 ? totalOvers / totalN : null;

  return (
    <div className="space-y-8 p-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="flex flex-wrap items-end gap-4">
        <div>
          <h1 className="text-xl font-bold">Vegas Analysis — {sport.toUpperCase()}</h1>
          <p className="text-sm text-gray-500 mt-0.5">
            {sport === "mlb"
              ? "Matchup lines and historical O/U + run line calibration"
              : "Matchup lines and historical O/U + spread calibration"}
          </p>
        </div>
        <div className="flex items-center gap-2 ml-auto">
          <label className="text-xs text-gray-500">Date</label>
          <input
            type="date"
            value={dateInput}
            onChange={(e) => handleDateChange(e.target.value)}
            className="rounded border px-2 py-1 text-sm"
          />
          <button
            onClick={handleFetchLines}
            disabled={isPending}
            className="rounded border px-3 py-1 text-sm bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {isPending ? "Fetching…" : matchups.length === 0 ? "Fetch Lines" : "Refresh Lines"}
          </button>
        </div>
      </div>

      {/* ── Fetch feedback ───────────────────────────────────── */}
      {fetchMsg && (
        <div
          className={`rounded border px-3 py-2 text-sm ${
            fetchMsg.ok
              ? "border-green-200 bg-green-50 text-green-800"
              : "border-red-200 bg-red-50 text-red-800"
          }`}
        >
          {fetchMsg.text}
        </div>
      )}

      {/* ── Vegas MAE Summary ────────────────────────────────── */}
      {vegasSummary != null && vegasSummary.n > 0 && (
        <div className="rounded-lg border bg-card p-4">
          <h2 className="text-sm font-semibold mb-3">
            Season Vegas Accuracy — {sport.toUpperCase()}
            <span className="ml-2 text-xs font-normal text-gray-400">
              {vegasSummary.n} games with lines + scores
            </span>
          </h2>
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Game Total MAE</div>
              <div className="text-xl font-bold">
                {vegasSummary.gameTotalMae != null ? vegasSummary.gameTotalMae.toFixed(2) : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.gameTotalBias != null
                  ? `bias ${vegasSummary.gameTotalBias > 0 ? "+" : ""}${vegasSummary.gameTotalBias.toFixed(2)}`
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Team Total MAE</div>
              <div className="text-xl font-bold">
                {vegasSummary.teamTotalMae != null ? vegasSummary.teamTotalMae.toFixed(2) : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.teamTotalBias != null
                  ? `bias ${vegasSummary.teamTotalBias > 0 ? "+" : ""}${vegasSummary.teamTotalBias.toFixed(2)}`
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Over Rate</div>
              <div className={`text-xl font-bold ${vegasSummary.ouOverRate != null && vegasSummary.ouOverRate > 0.52 ? "text-green-700" : vegasSummary.ouOverRate != null && vegasSummary.ouOverRate < 0.48 ? "text-blue-600" : ""}`}>
                {vegasSummary.ouOverRate != null ? `${(vegasSummary.ouOverRate * 100).toFixed(1)}%` : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.ouOverRate != null
                  ? vegasSummary.ouOverRate > 0.52 ? "overs dominate" : vegasSummary.ouOverRate < 0.48 ? "unders dominate" : "balanced"
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Game Bias</div>
              <div className={`text-xl font-bold ${vegasSummary.gameTotalBias != null && vegasSummary.gameTotalBias > 0 ? "text-red-600" : "text-blue-600"}`}>
                {vegasSummary.gameTotalBias != null
                  ? `${vegasSummary.gameTotalBias > 0 ? "+" : ""}${vegasSummary.gameTotalBias.toFixed(2)}`
                  : "—"}
              </div>
              <div className="text-xs text-gray-400 mt-0.5">
                {vegasSummary.gameTotalBias != null
                  ? vegasSummary.gameTotalBias > 0 ? "actuals beat lines" : "lines beat actuals"
                  : ""}
              </div>
            </div>
            <div className="rounded border p-3 text-center">
              <div className="text-xs text-gray-500 mb-1">Games Tracked</div>
              <div className="text-xl font-bold">{vegasSummary.n}</div>
              <div className="text-xs text-gray-400 mt-0.5">w/ lines + scores</div>
            </div>
          </div>
        </div>
      )}

      {/* ── Today's Matchups ─────────────────────────────────── */}
      <div className="rounded-lg border bg-card p-4 text-sm">
        <h2 className="font-semibold mb-3">
          Matchups — {queryDate}
          {matchups.length === 0 && (
            <span className="ml-2 text-xs font-normal text-gray-400">
              No games found for this date
            </span>
          )}
        </h2>
        {matchups.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500">
                  <th className="py-1 text-left">Matchup</th>
                  <th className="py-1 text-right">Total</th>
                  <th className="py-1 text-right">Spread</th>
                  <th className="py-1 text-right">Home ML</th>
                  <th className="py-1 text-right">Away ML</th>
                  <th className="py-1 text-right">Home Imp</th>
                  <th className="py-1 text-right">Away Imp</th>
                  <th className="py-1 text-right">Home Win%</th>
                  <th className="py-1 text-right">Score</th>
                  <th className="py-1 text-right">O/U</th>
                </tr>
              </thead>
              <tbody>
                {matchups.map((m) => {
                  const actual = m.homeScore != null && m.awayScore != null
                    ? m.homeScore + m.awayScore
                    : null;
                  const ouResult = actual != null && m.vegasTotal != null
                    ? actual > m.vegasTotal ? "O" : actual < m.vegasTotal ? "U" : "P"
                    : null;
                  const ouColor = ouResult === "O" ? "text-green-700 font-semibold"
                    : ouResult === "U" ? "text-red-600 font-semibold"
                    : "";
                  const homeSpreadStr = m.homeSpread == null ? "—"
                    : m.homeSpread > 0 ? `+${m.homeSpread}` : String(m.homeSpread);
                  return (
                    <tr key={m.matchupId} className="border-b border-gray-50">
                      <td className="py-1.5 font-medium">
                        {m.awayAbbrev} @ {m.homeAbbrev}
                      </td>
                      <td className="py-1.5 text-right">{fmt1(m.vegasTotal)}</td>
                      <td className="py-1.5 text-right">{homeSpreadStr}</td>
                      <td className="py-1.5 text-right">{fmtMl(m.homeMl)}</td>
                      <td className="py-1.5 text-right">{fmtMl(m.awayMl)}</td>
                      <td className="py-1.5 text-right text-blue-700">{fmt1(m.homeImplied)}</td>
                      <td className="py-1.5 text-right text-blue-700">{fmt1(m.awayImplied)}</td>
                      <td className="py-1.5 text-right">
                        {m.homeWinProb != null ? `${(m.homeWinProb * 100).toFixed(0)}%` : "—"}
                      </td>
                      <td className="py-1.5 text-right">
                        {m.homeScore != null && m.awayScore != null
                          ? `${m.awayScore}–${m.homeScore}`
                          : "—"}
                      </td>
                      <td className={`py-1.5 text-right ${ouColor}`}>
                        {ouResult ?? "—"}
                        {actual != null && m.vegasTotal != null && (
                          <span className="ml-1 text-gray-400 font-normal">
                            ({actual > m.vegasTotal ? "+" : ""}
                            {(actual - m.vegasTotal).toFixed(1)})
                          </span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {!hasScores && (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800">
          <strong>No historical score data yet.</strong> Run{" "}
          <code className="font-mono bg-amber-100 px-1 rounded">
            python -m ingest.backfill_scores
          </code>{" "}
          once to populate final scores, then re-run{" "}
          <code className="font-mono bg-amber-100 px-1 rounded">
            python -m ingest.nba_schedule
          </code>{" "}
          after each game day to keep scores current.
        </div>
      )}

      {/* ── O/U Hit Rate ──────────────────────────────────────── */}
      {ouHitRate.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <div className="flex flex-wrap items-baseline gap-4">
            <h2 className="font-semibold">Over/Under Hit Rate by Total</h2>
            {overallOverRate != null && (
              <span className="text-xs text-gray-500">
                Overall: {fmtPct(overallOverRate)} over ({totalOvers}/{totalN} games)
              </span>
            )}
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Total Tier</th>
                  <th className="py-1">Games</th>
                  <th className="py-1">Overs</th>
                  <th className="py-1">Unders</th>
                  <th className="py-1">Pushes</th>
                  <th className="py-1">Over%</th>
                  <th className="py-1">Avg Line</th>
                  <th className="py-1">Avg Actual</th>
                  <th className="py-1">Avg Error</th>
                </tr>
              </thead>
              <tbody>
                {ouHitRate.map((row) => {
                  const avgError = row.avgActual != null && row.avgTotal != null
                    ? row.avgActual - row.avgTotal
                    : null;
                  const overColor = row.overRate == null ? ""
                    : row.overRate > 0.55 ? "text-green-700 font-semibold"
                    : row.overRate < 0.45 ? "text-red-600 font-semibold"
                    : "";
                  return (
                    <tr key={row.totalTier} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.totalTier}</td>
                      <td className="py-1 text-right">{row.n}</td>
                      <td className="py-1 text-right text-green-700">{row.overCount}</td>
                      <td className="py-1 text-right text-red-600">{row.underCount}</td>
                      <td className="py-1 text-right text-gray-400">{row.pushCount}</td>
                      <td className={`py-1 text-right ${overColor}`}>
                        {fmtPct(row.overRate)}
                      </td>
                      <td className="py-1 text-right">{fmt1(row.avgTotal)}</td>
                      <td className="py-1 text-right">{fmt1(row.avgActual)}</td>
                      <td className="py-1 text-right">
                        <BiasChip bias={avgError} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Spread Coverage ───────────────────────────────────── */}
      {spreadCoverage.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <h2 className="font-semibold">{sport === "mlb" ? "Run Line Coverage" : "Spread Coverage by Tier"}</h2>
          <p className="text-xs text-gray-500">
            {sport === "mlb"
              ? "Did the favorite cover the run line (±1.5)? Margin = avg actual run differential."
              : "Did the favorite cover? Margin = avg actual point differential."}
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Spread</th>
                  <th className="py-1">Games</th>
                  <th className="py-1">Covers</th>
                  <th className="py-1">Cover%</th>
                  <th className="py-1">Avg Spread</th>
                  <th className="py-1">Avg Margin</th>
                </tr>
              </thead>
              <tbody>
                {spreadCoverage.map((row) => {
                  const coverColor = row.coverRate == null ? ""
                    : row.coverRate > 0.55 ? "text-green-700 font-semibold"
                    : row.coverRate < 0.45 ? "text-red-600 font-semibold"
                    : "";
                  return (
                    <tr key={row.spreadTier} className="border-b border-gray-50">
                      <td className="py-1 font-medium">{row.spreadTier}</td>
                      <td className="py-1 text-right">{row.n}</td>
                      <td className="py-1 text-right">{row.coverCount}</td>
                      <td className={`py-1 text-right ${coverColor}`}>
                        {fmtPct(row.coverRate)}
                      </td>
                      <td className="py-1 text-right">{fmt1(row.avgSpread)}</td>
                      <td className="py-1 text-right">{fmt1(row.avgMargin)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Team Total Accuracy ───────────────────────────────── */}
      {teamTotalAccuracy.length > 0 && (
        <div className="rounded-lg border bg-card p-4 text-sm space-y-3">
          <h2 className="font-semibold">Team Implied Total Accuracy</h2>
          <p className="text-xs text-gray-500">
            Implied {sport === "mlb" ? "run total" : "total"} (derived from moneylines + O/U) vs actual team score.
            Bias: positive = market over-projected this team. Sorted by worst MAE.
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs border-collapse">
              <thead>
                <tr className="border-b text-gray-500 text-right">
                  <th className="py-1 text-left">Team</th>
                  <th className="py-1">Games</th>
                  <th className="py-1">Avg Implied</th>
                  <th className="py-1">Avg Actual</th>
                  <th className="py-1">MAE</th>
                  <th className="py-1">Bias</th>
                </tr>
              </thead>
              <tbody>
                {teamTotalAccuracy.map((row) => (
                  <tr key={row.teamAbbrev} className="border-b border-gray-50">
                    <td className="py-1 font-medium">
                      {row.teamAbbrev}
                      <span className="ml-1.5 text-gray-400 font-normal hidden sm:inline">
                        {row.teamName}
                      </span>
                    </td>
                    <td className="py-1 text-right text-gray-400">{row.n}</td>
                    <td className="py-1 text-right">{fmt1(row.avgImplied)}</td>
                    <td className="py-1 text-right">{fmt1(row.avgActual)}</td>
                    <td className="py-1 text-right font-medium">{fmt1(row.mae)}</td>
                    <td className="py-1 text-right">
                      <BiasChip bias={row.bias} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
