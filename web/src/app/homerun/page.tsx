export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { getMlbHomerunBoard, type MlbHomerunCandidate } from "@/db/queries";

export const metadata: Metadata = {
  title: "MLB Homerun Board",
  description: "Top MLB hitters by 1+ home run probability.",
};

const fmtPct = (value: number | null | undefined, digits = 1) =>
  value == null ? "-" : `${(value * 100).toFixed(digits)}%`;

const fmtNum = (value: number | null | undefined, digits = 2) =>
  value == null ? "-" : value.toFixed(digits);

const fmtSalary = (salary: number) => `$${salary.toLocaleString()}`;

function displayPos(positions: string): string {
  return positions.replace(/^UTIL\/?/, "") || positions;
}

function cleanDate(value: string | string[] | undefined): string | null {
  const raw = Array.isArray(value) ? value[0] : value;
  return raw && /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
}

function cleanDkId(value: string | string[] | undefined): number | null {
  const raw = Array.isArray(value) ? value[0] : value;
  if (!raw) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

function confidence(candidate: MlbHomerunCandidate): { label: string; className: string } {
  const available = [
    candidate.battingOrder != null,
    candidate.lineupConfirmed === true,
    candidate.hitterHrPg != null,
    candidate.parkHrFactor != null,
    candidate.opposingPitcherName != null,
    candidate.opposingPitcherHrPer9 != null || candidate.opposingPitcherXfip != null,
    candidate.teamTotal != null,
    candidate.weatherTemp != null || candidate.windSpeed != null,
  ].filter(Boolean).length;

  if (available >= 6) return { label: "Strong", className: "border-emerald-200 bg-emerald-50 text-emerald-700" };
  if (available >= 4) return { label: "Good", className: "border-sky-200 bg-sky-50 text-sky-700" };
  return { label: "Thin", className: "border-amber-200 bg-amber-50 text-amber-700" };
}

function pitcherRisk(candidate: MlbHomerunCandidate): { label: string; className: string } {
  const hrPer9 = candidate.opposingPitcherHrPer9;
  const xfip = candidate.opposingPitcherXfip;
  if (hrPer9 == null && xfip == null) return { label: "Pitcher ?", className: "border-slate-200 bg-slate-50 text-slate-600" };
  if ((hrPer9 ?? 0) >= 1.35 || (xfip ?? 0) >= 4.7) {
    return { label: "Pitcher HR+", className: "border-rose-200 bg-rose-50 text-rose-700" };
  }
  if ((hrPer9 ?? 0) <= 0.85 && (xfip ?? 99) <= 3.8) {
    return { label: "Pitcher HR-", className: "border-blue-200 bg-blue-50 text-blue-700" };
  }
  return { label: "Pitcher Mid", className: "border-slate-200 bg-slate-50 text-slate-700" };
}

function parkRisk(candidate: MlbHomerunCandidate): { label: string; className: string } {
  const factor = candidate.parkHrFactor;
  if (factor == null) return { label: "Park ?", className: "border-slate-200 bg-slate-50 text-slate-600" };
  if (factor >= 1.06) return { label: `Park ${factor.toFixed(2)}x`, className: "border-rose-200 bg-rose-50 text-rose-700" };
  if (factor <= 0.94) return { label: `Park ${factor.toFixed(2)}x`, className: "border-blue-200 bg-blue-50 text-blue-700" };
  return { label: `Park ${factor.toFixed(2)}x`, className: "border-slate-200 bg-slate-50 text-slate-700" };
}

function lineupStatus(candidate: MlbHomerunCandidate): { label: string; className: string } {
  if (candidate.lineupConfirmed && candidate.battingOrder != null) {
    return { label: `Order ${candidate.battingOrder}`, className: "border-emerald-200 bg-emerald-50 text-emerald-700" };
  }
  if (candidate.battingOrder != null) {
    return { label: `Order ${candidate.battingOrder}`, className: "border-sky-200 bg-sky-50 text-sky-700" };
  }
  return { label: "Order ?", className: "border-amber-200 bg-amber-50 text-amber-700" };
}

function weatherLabel(candidate: MlbHomerunCandidate): string {
  const temp = candidate.weatherTemp != null ? `${candidate.weatherTemp}F` : null;
  const wind = candidate.windSpeed != null
    ? `${candidate.windSpeed} mph${candidate.windDirection ? ` ${candidate.windDirection}` : ""}`
    : null;
  return [temp, wind].filter(Boolean).join(" | ") || "Weather -";
}

function FactorChip({ label, className }: { label: string; className: string }) {
  return (
    <span className={`inline-flex items-center rounded border px-2 py-0.5 text-[11px] font-medium ${className}`}>
      {label}
    </span>
  );
}

function CandidateCard({ candidate, rank }: { candidate: MlbHomerunCandidate; rank: number }) {
  const conf = confidence(candidate);
  const pitch = pitcherRisk(candidate);
  const park = parkRisk(candidate);
  const lineup = lineupStatus(candidate);
  return (
    <article className="rounded-lg border bg-card p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="text-xs font-semibold text-slate-500">#{rank}</div>
          <h2 className="mt-1 truncate text-lg font-semibold text-slate-950">{candidate.name}</h2>
          <p className="mt-0.5 text-xs text-slate-500">
            {candidate.teamAbbrev}
            {candidate.opponentAbbrev ? ` vs ${candidate.opponentAbbrev}` : ""}
            {" | "}
            {displayPos(candidate.eligiblePositions)}
            {" | "}
            DK {candidate.dkPlayerId}
          </p>
        </div>
        <div className="text-right">
          <div className="text-3xl font-semibold text-rose-700">{fmtPct(candidate.hrProb1Plus)}</div>
          <div className="text-[11px] text-slate-500">1+ HR</div>
        </div>
      </div>

      <div className="mt-4 grid grid-cols-3 gap-2 text-xs">
        <div>
          <div className="text-slate-400">Exp HR</div>
          <div className="font-semibold text-slate-900">{fmtNum(candidate.expectedHr, 3)}</div>
        </div>
        <div>
          <div className="text-slate-400">HR/G</div>
          <div className="font-semibold text-slate-900">{fmtNum(candidate.hitterHrPg, 2)}</div>
        </div>
        <div>
          <div className="text-slate-400">Team Tot</div>
          <div className="font-semibold text-slate-900">{fmtNum(candidate.teamTotal, 1)}</div>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap gap-1.5">
        <FactorChip {...lineup} />
        <FactorChip {...park} />
        <FactorChip {...pitch} />
        <FactorChip label={conf.label} className={conf.className} />
      </div>
    </article>
  );
}

function CandidateRow({ candidate, rank }: { candidate: MlbHomerunCandidate; rank: number }) {
  const conf = confidence(candidate);
  const pitch = pitcherRisk(candidate);
  const park = parkRisk(candidate);
  const lineup = lineupStatus(candidate);
  return (
    <tr className="border-b border-slate-100 align-top hover:bg-slate-50">
      <td className="py-3 pr-3 text-sm font-semibold text-slate-500">{rank}</td>
      <td className="py-3 pr-3">
        <div className="font-medium text-slate-950">{candidate.name}</div>
        <div className="mt-0.5 text-xs text-slate-500">
          {candidate.teamAbbrev}
          {candidate.opponentAbbrev ? ` vs ${candidate.opponentAbbrev}` : ""}
          {" | "}
          {displayPos(candidate.eligiblePositions)}
          {" | "}
          {fmtSalary(candidate.salary)}
          {" | "}
          DK {candidate.dkPlayerId}
        </div>
      </td>
      <td className="py-3 pr-3 text-right">
        <div className="text-base font-semibold text-rose-700">{fmtPct(candidate.hrProb1Plus)}</div>
        <div className="text-xs text-slate-500">Exp {fmtNum(candidate.expectedHr, 3)}</div>
      </td>
      <td className="py-3 pr-3 text-right text-xs text-slate-700">
        <div>{candidate.hitterHrPg != null ? `${candidate.hitterHrPg.toFixed(2)} HR/G` : "-"}</div>
        <div>{candidate.hitterIso != null ? `${candidate.hitterIso.toFixed(3)} ISO` : "-"}</div>
      </td>
      <td className="py-3 pr-3 text-right text-xs text-slate-700">
        <div>{candidate.opposingPitcherName ?? "-"}</div>
        <div>
          {candidate.opposingPitcherHand ? `${candidate.opposingPitcherHand} | ` : ""}
          {candidate.opposingPitcherHrPer9 != null ? `${candidate.opposingPitcherHrPer9.toFixed(2)} HR/9` : "HR/9 -"}
        </div>
      </td>
      <td className="py-3 pr-3 text-right text-xs text-slate-700">
        <div>{candidate.ballpark ?? "-"}</div>
        <div>{weatherLabel(candidate)}</div>
      </td>
      <td className="py-3">
        <div className="flex max-w-56 flex-wrap justify-end gap-1.5">
          <FactorChip {...lineup} />
          <FactorChip {...park} />
          <FactorChip {...pitch} />
          <FactorChip label={conf.label} className={conf.className} />
        </div>
      </td>
    </tr>
  );
}

export default async function HomerunPage({
  searchParams,
}: {
  searchParams: Promise<{ sport?: string; date?: string | string[]; dkId?: string | string[] }>;
}) {
  const params = await searchParams;
  const date = cleanDate(params.date);
  const dkId = cleanDkId(params.dkId);
  if (params.sport !== "mlb") {
    const nextParams = new URLSearchParams({ sport: "mlb" });
    if (dkId != null) nextParams.set("dkId", String(dkId));
    if (date) nextParams.set("date", date);
    redirect(`/homerun?${nextParams.toString()}`);
  }

  const board = await getMlbHomerunBoard({ date, dkId });
  const podium = board.candidates.slice(0, 3);

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-4 border-b pb-5 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-rose-700">MLB Homerun Board</p>
          <h1 className="mt-1 text-3xl font-semibold tracking-tight text-slate-950">Top 15 by 1+ HR chance</h1>
          <p className="mt-1 text-sm text-slate-500">
            {board.slateDate ? `Slate ${board.slateDate}` : "No MLB slate found"}
            {board.requestedDkId != null && board.dkIdKind === "contest" ? ` | DK contest ${board.requestedDkId}` : ""}
            {board.dkDraftGroupId != null ? ` | DK draft group ${board.dkDraftGroupId}` : ""}
            {board.contestType ? ` | ${board.contestType}` : ""}
            {board.gameCount != null ? ` | ${board.gameCount} games` : ""}
            {board.totalQualified > 0 ? ` | ${board.totalQualified} qualified hitters` : ""}
          </p>
        </div>
        <form method="get" action="/homerun" className="flex items-end gap-2">
          <input type="hidden" name="sport" value="mlb" />
          <label className="block">
            <span className="mb-1 block text-xs text-slate-500">DK ID</span>
            <input
              type="number"
              name="dkId"
              min="1"
              inputMode="numeric"
              placeholder={board.dkDraftGroupId != null ? String(board.dkDraftGroupId) : "Contest or draft group"}
              defaultValue={board.requestedDkId ?? ""}
              className="h-9 w-40 rounded border px-2 text-sm"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs text-slate-500">Date</span>
            <input
              type="date"
              name="date"
              defaultValue={board.slateDate ?? board.requestedDate ?? ""}
              className="h-9 rounded border px-2 text-sm"
            />
          </label>
          <button className="h-9 rounded bg-slate-950 px-3 text-sm font-medium text-white hover:bg-slate-800">
            Load
          </button>
        </form>
      </div>

      {podium.length > 0 ? (
        <div className="grid gap-3 md:grid-cols-3">
          {podium.map((candidate, index) => (
            <CandidateCard key={candidate.id} candidate={candidate} rank={index + 1} />
          ))}
        </div>
      ) : (
        <div className="rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-800">
          {board.dkIdError ?? "No MLB hitters with home run probabilities were found for this slate."}
        </div>
      )}

      {board.candidates.length > 0 && (
        <div className="overflow-hidden rounded-lg border bg-card">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[980px] border-collapse text-sm">
              <thead>
                <tr className="border-b bg-slate-50 text-right text-xs uppercase tracking-wide text-slate-500">
                  <th className="py-2 pl-4 pr-3 text-left">Rank</th>
                  <th className="py-2 pr-3 text-left">Player</th>
                  <th className="py-2 pr-3">HR Chance</th>
                  <th className="py-2 pr-3">Power</th>
                  <th className="py-2 pr-3">Pitcher</th>
                  <th className="py-2 pr-3">Environment</th>
                  <th className="py-2 pr-4">Factors</th>
                </tr>
              </thead>
              <tbody>
                {board.candidates.map((candidate, index) => (
                  <CandidateRow key={candidate.id} candidate={candidate} rank={index + 1} />
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
