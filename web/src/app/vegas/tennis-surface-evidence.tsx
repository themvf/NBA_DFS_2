"use client";

import type { TennisEloDashboard, TennisEloEvidenceRow } from "@/db/queries";
import { canPromoteTennisSurfaceElo, tennisSurfaceActionMessage } from "@/lib/tennis-elo-policy";

const surfaceLabel: Record<string, string> = {
  hard: "Hard",
  clay: "Clay",
  grass: "Grass",
};

function signed(value: number, digits = 0) {
  return `${value >= 0 ? "+" : ""}${value.toFixed(digits)}`;
}

function percent(value: number | null, digits = 1) {
  return value == null ? "—" : `${(value * 100).toFixed(digits)}%`;
}

function compactDate(value: string | null) {
  if (!value) return "—";
  const date = new Date(`${value.slice(0, 10)}T00:00:00`);
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

function confidenceClass(label: string) {
  if (label === "established") return "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300";
  if (label === "developing") return "bg-amber-500/15 text-amber-700 dark:text-amber-300";
  return "bg-rose-500/15 text-rose-700 dark:text-rose-300";
}

function SurfaceEvidenceCard({ row }: { row: TennisEloEvidenceRow }) {
  const edgeDirection = row.surfaceDelta >= 0 ? "stronger" : "weaker";
  const hasPerformance = row.servePointsWonPct != null || row.returnPointsWonPct != null;

  return (
    <article className="rounded-xl border bg-card p-4 shadow-sm">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            {row.tour} · {surfaceLabel[row.surface] ?? row.surface}
          </div>
          <h3 className="text-lg font-bold">{row.player}</h3>
        </div>
        <span className={`rounded-full px-2.5 py-1 text-xs font-semibold ${confidenceClass(row.reliabilityLabel)}`}>
          {row.reliabilityLabel}
        </span>
      </div>

      <div className="mt-3 grid grid-cols-3 gap-2 text-center">
        <div className="rounded-lg bg-muted/50 p-2">
          <div className="text-[10px] uppercase text-muted-foreground">Overall</div>
          <div className="font-bold tabular-nums">{row.overallElo.toFixed(0)}</div>
        </div>
        <div className="rounded-lg bg-muted/50 p-2">
          <div className="text-[10px] uppercase text-muted-foreground">Surface</div>
          <div className="font-bold tabular-nums">{row.surfaceElo.toFixed(0)}</div>
        </div>
        <div className="rounded-lg bg-muted/50 p-2">
          <div className="text-[10px] uppercase text-muted-foreground">Blended</div>
          <div className="font-bold tabular-nums">{row.blendedSurfaceElo.toFixed(0)}</div>
        </div>
      </div>

      <div className="mt-3 rounded-lg border p-3">
        <div className="flex items-center justify-between gap-3">
          <span className="text-sm font-semibold">Surface Edge</span>
          <span className={`font-bold tabular-nums ${row.surfaceDelta >= 0 ? "text-emerald-600 dark:text-emerald-400" : "text-rose-600 dark:text-rose-400"}`}>
            {signed(row.surfaceDelta)} Elo
          </span>
        </div>
        <p className="mt-1 text-xs text-muted-foreground">
          {row.player} rates {Math.abs(row.surfaceDelta).toFixed(0)} Elo points {edgeDirection} on {row.surface}
          {" "}than overall, based on {row.surfaceMatches} surface matches since 2023.
        </p>
      </div>

      <div className="mt-3 grid gap-2 text-xs sm:grid-cols-2">
        <div>
          <div className="font-semibold">Sample &amp; Confidence</div>
          <div className="text-muted-foreground">
            {row.surfaceMatches} surface · {row.overallMatches} overall · {percent(row.surfaceReliability, 0)} reliability
          </div>
          <div className="text-muted-foreground">
            Last eligible match {compactDate(row.lastEligibleMatchDate)} · {row.inactivityDays} days ago
          </div>
        </div>
        <div>
          <div className="font-semibold">Form &amp; workload</div>
          <div className="text-muted-foreground">
            Prior 10 win rate {percent(row.recentForm)} · {row.recentMatchLoad ?? "—"} matches in prior 14 days
          </div>
          <div className="text-muted-foreground">Stats through {compactDate(row.statsThroughAt)}</div>
        </div>
      </div>

      <div className="mt-3 rounded-lg bg-muted/35 p-3 text-xs">
        <div className="font-semibold">Why the model differs</div>
        <p className="mt-1 text-muted-foreground">
          The surface adjustment moves the raw rating {signed(row.surfaceDelta)} points from overall.
          Sample reliability reduces that to a blended adjustment of {signed(row.blendedSurfaceElo - row.overallElo)} points.
          The validation gate did not pass, so this difference does not create a bet by itself.
        </p>
      </div>

      <div className="mt-3 text-xs">
        <div className="font-semibold">Data coverage</div>
        {hasPerformance ? (
          <div className="text-muted-foreground">
            ATP trailing serve points won {percent(row.servePointsWonPct)} · return points won {percent(row.returnPointsWonPct)}
          </div>
        ) : (
          <div className="text-muted-foreground">
            Serve/return detail is unavailable from the current {row.tour} historical source and remains blank.
          </div>
        )}
        <div className="text-muted-foreground">
          Match times: {row.startTimeAvailability.replaceAll("_", " ")} · historical odds: {row.marketHistoryAvailability.replaceAll("_", " ")}
        </div>
      </div>

      <div className="mt-4 overflow-x-auto">
        <div className="mb-1 text-xs font-semibold">Recent point-in-time rating history</div>
        <table className="w-full min-w-[480px] text-xs">
          <thead>
            <tr className="border-b text-muted-foreground">
              <th className="py-1 text-left font-medium">Date</th>
              <th className="py-1 text-left font-medium">Opponent</th>
              <th className="py-1 text-center font-medium">Result</th>
              <th className="py-1 text-right font-medium">Surface Elo</th>
              <th className="py-1 text-right font-medium">Pre-match %</th>
            </tr>
          </thead>
          <tbody>
            {row.history.map((point) => (
              <tr key={`${point.matchDate}-${point.opponent}`} className="border-b border-border/40 last:border-0">
                <td className="py-1.5 text-muted-foreground">{compactDate(point.matchDate)}</td>
                <td className="py-1.5">{point.opponent}</td>
                <td className={`py-1.5 text-center font-semibold ${point.won ? "text-emerald-600 dark:text-emerald-400" : "text-rose-600 dark:text-rose-400"}`}>
                  {point.won ? "W" : "L"}
                </td>
                <td className="py-1.5 text-right tabular-nums">
                  {point.surfaceBefore.toFixed(0)} → {point.surfaceAfter.toFixed(0)}
                </td>
                <td className="py-1.5 text-right tabular-nums text-muted-foreground">
                  {percent(point.expectedProbability, 0)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-3 border-t pt-2 text-[10px] text-muted-foreground">
        {row.algorithmVersion} · {row.featureVersion ?? "feature evidence unavailable"} · source {row.sourceChecksum.slice(0, 12)}
      </div>
    </article>
  );
}

export default function TennisSurfaceEvidence({ dashboard }: { dashboard: TennisEloDashboard }) {
  const promoted = canPromoteTennisSurfaceElo(dashboard.gates);

  return (
    <section className="space-y-4">
      <div className={`rounded-xl border p-4 ${promoted ? "border-emerald-500/40 bg-emerald-500/10" : "border-amber-500/50 bg-amber-500/10"}`}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div>
            <div className="text-xs font-bold uppercase tracking-wide text-muted-foreground">Surface Elo validation</div>
            <h2 className="text-lg font-bold">{promoted ? "Surface adjustment promoted" : "Surface adjustment not promoted"}</h2>
          </div>
          <span className={`rounded-full px-3 py-1 text-xs font-bold ${promoted ? "bg-emerald-600 text-white" : "bg-amber-500 text-black"}`}>
            {promoted ? "USE IN MODEL" : "DO NOT USE ALONE"}
          </span>
        </div>
        <p className="mt-2 text-sm">{tennisSurfaceActionMessage(dashboard.gates)}</p>
        <div className="mt-3 grid gap-2 sm:grid-cols-2">
          {dashboard.gates.map((gate) => (
            <div key={gate.tour} className="rounded-lg border bg-background/60 p-3 text-xs">
              <div className="flex items-center justify-between">
                <span className="font-bold">{gate.tour} · 2025 validation</span>
                <span className={gate.status === "PASS" ? "text-emerald-600" : "text-rose-600"}>{gate.status}</span>
              </div>
              <div className="mt-1 text-muted-foreground">
                n={gate.validationSampleSize.toLocaleString()} · log-loss change {gate.validationLogLossDelta == null ? "—" : signed(gate.validationLogLossDelta, 6)}
              </div>
              <div className="text-muted-foreground">
                95% tournament CI [{gate.bootstrapCiLow?.toFixed(6) ?? "—"}, {gate.bootstrapCiHigh?.toFixed(6) ?? "—"}]
              </div>
            </div>
          ))}
        </div>
      </div>

      <div>
        <h2 className="text-lg font-bold">Surface rating evidence</h2>
        <p className="text-sm text-muted-foreground">
          One high-sample ATP and WTA example for each surface. Every value is reconstructed immediately before or after an eligible match using 2023-present history only.
        </p>
      </div>

      {dashboard.evidence.length === 0 ? (
        <div className="rounded-xl border border-dashed p-5 text-sm text-muted-foreground">
          No completed Elo run is available. Run the immutable surface-Elo population before displaying rating evidence.
        </div>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          {dashboard.evidence.map((row) => (
            <SurfaceEvidenceCard key={`${row.tour}-${row.surface}-${row.playerId}`} row={row} />
          ))}
        </div>
      )}
    </section>
  );
}
