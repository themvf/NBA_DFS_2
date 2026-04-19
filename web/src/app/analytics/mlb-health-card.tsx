import {
  getCachedMlbBlowupCandidateReport,
  getCachedMlbPostmortemReport,
} from "@/db/analytics-cache";

// ── helpers ──────────────────────────────────────────────────────────────────

function fmt2(v: number | null | undefined) {
  return v == null ? "—" : v.toFixed(2);
}

function formatSignalName(s: string): string {
  return s.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase()).slice(0, 24);
}

// ── sub-component: a single KPI tile ─────────────────────────────────────────

function KpiTile({
  label,
  value,
  sub,
  trendLabel,
  trendGood,
}: {
  label: string;
  value: string;
  sub?: string;
  trendLabel?: string;
  trendGood?: boolean; // true = green, false = red, undefined = neutral
}) {
  const trendColor =
    trendGood === true
      ? "text-emerald-700"
      : trendGood === false
        ? "text-rose-700"
        : "text-slate-500";
  const arrow =
    trendGood === true ? "↓" : trendGood === false ? "↑" : "";

  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 p-3 space-y-0.5">
      <p className="text-[11px] font-medium text-slate-500 uppercase tracking-wide">{label}</p>
      <p className="text-xl font-bold text-slate-900">{value}</p>
      {trendLabel && (
        <p className={`text-xs ${trendColor}`}>
          {arrow} {trendLabel}
        </p>
      )}
      {sub && <p className="text-[11px] text-slate-400">{sub}</p>}
    </div>
  );
}

// ── main component ────────────────────────────────────────────────────────────

export default async function MlbHealthCard() {
  let postmortem = null;
  let blowup = null;
  try {
    [postmortem, blowup] = await Promise.all([
      getCachedMlbPostmortemReport(),
      getCachedMlbBlowupCandidateReport(),
    ]);
  } catch {
    return null;
  }

  if (!postmortem) return null;

  // ── KPI 1: Projection MAE trend ─────────────────────────────────────────────
  // projectionSummary rows are split by windowSort (lower = recent) and playerGroup.
  // Use the row with the most observations per window as the "all players" aggregate.
  const projRows = postmortem.projectionSummary;
  const windowSorts = [...new Set(projRows.map((r) => r.windowSort))].sort((a, b) => a - b);
  const recentSort = windowSorts[0] ?? 0;
  const priorSort = windowSorts[windowSorts.length - 1] ?? 1;

  const recentProjRow = projRows
    .filter((r) => r.windowSort === recentSort)
    .sort((a, b) => b.rows - a.rows)[0];
  const priorProjRow = projRows
    .filter((r) => r.windowSort === priorSort)
    .sort((a, b) => b.rows - a.rows)[0];

  const recentMae = recentProjRow?.finalMae ?? null;
  const priorMae = priorProjRow?.finalMae ?? null;
  const maeDelta = recentMae != null && priorMae != null ? recentMae - priorMae : null;
  // Negative delta = MAE improved (lower is better)
  const maeTrendGood = maeDelta == null ? undefined : maeDelta < -0.1;
  const maeTrendBad = maeDelta == null ? undefined : maeDelta > 0.1;
  const maeTrendLabel =
    maeDelta == null
      ? undefined
      : `${maeDelta > 0 ? "+" : ""}${maeDelta.toFixed(2)} vs prior ${priorProjRow?.windowLabel ?? "window"}`;

  // ── KPI 2: Ownership correlation ────────────────────────────────────────────
  const ownRows = postmortem.ownershipSummary;
  const recentOwnRow = ownRows
    .filter((r) => r.windowSort === recentSort)
    .sort((a, b) => b.rows - a.rows)[0];
  const ownCorr = recentOwnRow?.fieldCorr ?? null;
  const ownCorrGood = ownCorr == null ? undefined : ownCorr > 0.55;

  // ── KPI 3: Blowup 20+ hit rate (ranks 1–6) ──────────────────────────────────
  const topRanks = blowup?.rankSummary.filter((r) => r.candidateRank <= 6) ?? [];
  const blowup20Avg =
    topRanks.length > 0
      ? topRanks.reduce((sum, r) => sum + (r.hit20Rate ?? 0), 0) / topRanks.length
      : null;
  const blowup20Good = blowup20Avg == null ? undefined : blowup20Avg > 30;

  // ── KPI 4: Top signal by 25+ lift ───────────────────────────────────────────
  const topSignal = [...postmortem.signalFollowThrough]
    .filter((s) => s.lift25Rate != null)
    .sort((a, b) => (b.lift25Rate ?? 0) - (a.lift25Rate ?? 0))[0];

  return (
    <div className="mx-auto mt-8 max-w-5xl rounded-lg border bg-card p-4 text-slate-900">
      <div className="mb-3">
        <h2 className="text-sm font-semibold">Model Health</h2>
        <p className="text-xs text-slate-500 mt-0.5">
          Key signals at a glance — recent window vs prior.
          {postmortem.sample.latestSlateDate
            ? ` Latest completed slate: ${postmortem.sample.latestSlateDate}.`
            : ""}
        </p>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <KpiTile
          label="Proj MAE (recent)"
          value={fmt2(recentMae)}
          trendLabel={maeTrendLabel}
          trendGood={maeTrendGood === true ? true : maeTrendBad === true ? false : undefined}
          sub={`Prior: ${fmt2(priorMae)}`}
        />
        <KpiTile
          label="Own Correlation"
          value={fmt2(ownCorr)}
          trendGood={ownCorrGood}
          sub={`Recent window · ${recentOwnRow?.rows ?? 0} rows`}
        />
        <KpiTile
          label="Blowup 20+ (R1–6)"
          value={blowup20Avg != null ? `${blowup20Avg.toFixed(1)}%` : "—"}
          trendGood={blowup20Good}
          sub={`${blowup?.sample.slates ?? 0} tracked slates`}
        />
        <KpiTile
          label="Top Signal (25+)"
          value={
            topSignal
              ? `+${topSignal.lift25Rate!.toFixed(1)}%`
              : "—"
          }
          trendGood={topSignal ? true : undefined}
          sub={topSignal ? formatSignalName(topSignal.signal) : "No signal data"}
        />
      </div>
    </div>
  );
}
