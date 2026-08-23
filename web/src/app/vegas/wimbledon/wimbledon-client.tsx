"use client";

import type {
  TennisBetRow,
  TennisBetBacktestRow,
  TennisLegacyBetSummary,
  TennisFavoriteDogRow,
  TennisFavoriteLossRow,
  TennisFavoriteCalibrationRow,
} from "@/db/queries";
import { TennisResults } from "../tennis-vegas-client";

const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);
const fmtMl = (ml: number | null) => (ml == null ? "—" : ml > 0 ? `+${ml}` : String(ml));

function FavoriteDogPanel({ rows }: { rows: TennisFavoriteDogRow[] }) {
  const find = (group: "market" | "picks", side: "favorite" | "dog") =>
    rows.find((r) => r.group === group && r.side === side);

  const cuts: { label: string; side: string; row: TennisFavoriteDogRow | undefined }[] = [
    { label: "Market", side: "Favorite", row: find("market", "favorite") },
    { label: "Market", side: "Dog", row: find("market", "dog") },
    { label: "Our Picks", side: "Favorite", row: find("picks", "favorite") },
    { label: "Our Picks", side: "Dog", row: find("picks", "dog") },
  ];
  const totalSettled = cuts
    .filter((c) => c.label === "Market")
    .reduce((s, c) => s + (c.row?.wins ?? 0) + (c.row?.losses ?? 0), 0);

  return (
    <div className="rounded-lg border bg-card p-4">
      <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-1">
        Favorite vs Dog — Wimbledon
      </h3>
      <p className="text-xs text-muted-foreground mb-3">
        &ldquo;Market&rdquo; = which side Vegas favored (market probability &ge; 50%) on settled moneyline
        bets. &ldquo;Our Picks&rdquo; = which side our_prob favored. Our probability equals the market
        consensus (no independent edge proven — see the tennis moneyline calibration finding), so these
        two cuts are expected to track closely, not diverge — shown side by side so that&rsquo;s visible
        rather than assumed.
      </p>
      {totalSettled === 0 ? (
        <div className="rounded border border-dashed bg-muted/20 p-4 text-center text-xs text-muted-foreground">
          No settled Wimbledon bets yet.
        </div>
      ) : (
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="border-b text-xs uppercase text-muted-foreground">
              <th className="py-1 text-left">Cut</th>
              <th className="py-1 text-left">Side</th>
              <th className="py-1 text-right">Won</th>
              <th className="py-1 text-right">Lost</th>
              <th className="py-1 text-right">Win%</th>
            </tr>
          </thead>
          <tbody>
            {cuts.map(({ label, side, row }, i) => (
              <tr key={i} className="border-b last:border-0">
                <td className="py-1.5">{label}</td>
                <td className="py-1.5">{side}</td>
                <td className="py-1.5 text-right tabular-nums text-emerald-500">{row?.wins ?? 0}</td>
                <td className="py-1.5 text-right tabular-nums text-rose-500">{row?.losses ?? 0}</td>
                <td className="py-1.5 text-right tabular-nums font-medium">{pct(row?.winRate ?? null)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function CalibrationTierTable({
  title,
  data,
  labelHeader,
}: {
  title: string;
  data: TennisFavoriteCalibrationRow[];
  labelHeader: string;
}) {
  return (
    <div>
      <h4 className="text-xs font-semibold text-muted-foreground uppercase tracking-wide mb-1">{title}</h4>
      {data.length === 0 ? (
        <p className="text-xs text-muted-foreground">No settled favorite bets yet.</p>
      ) : (
        <table className="w-full text-sm border-collapse">
          <thead>
            <tr className="border-b text-xs uppercase text-muted-foreground">
              <th className="py-1 text-left">{labelHeader}</th>
              <th className="py-1 text-right">n</th>
              <th className="py-1 text-right">Implied</th>
              <th className="py-1 text-right">Realized</th>
              <th className="py-1 text-right">Gap</th>
            </tr>
          </thead>
          <tbody>
            {data.map((r) => {
              const gap = r.winRate != null && r.avgImplied != null ? r.winRate - r.avgImplied : null;
              return (
                <tr key={r.label} className="border-b last:border-0">
                  <td className="py-1.5">{r.label}</td>
                  <td className="py-1.5 text-right tabular-nums text-muted-foreground">{r.n}</td>
                  <td className="py-1.5 text-right tabular-nums text-muted-foreground">{pct(r.avgImplied)}</td>
                  <td className="py-1.5 text-right tabular-nums font-medium">{pct(r.winRate)}</td>
                  <td
                    className={`py-1.5 text-right tabular-nums ${
                      gap == null ? "text-muted-foreground" : gap >= 0 ? "text-emerald-500" : "text-rose-500"
                    }`}
                  >
                    {gap == null ? "—" : `${gap >= 0 ? "+" : ""}${(gap * 100).toFixed(1)}pp`}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function FavoriteCalibrationPanel({ rows }: { rows: TennisFavoriteCalibrationRow[] }) {
  const tierOrder = ["50-60%", "60-70%", "70-80%", "80-90%", "90-100%"];
  const tiers = rows
    .filter((r) => r.cut === "tier")
    .sort((a, b) => tierOrder.indexOf(a.label) - tierOrder.indexOf(b.label));
  const tours = rows.filter((r) => r.cut === "tour").sort((a, b) => b.n - a.n);

  return (
    <div className="rounded-lg border bg-card p-4 space-y-4">
      <div>
        <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide mb-1">
          Where Favorite Losses Concentrate
        </h3>
        <p className="text-xs text-muted-foreground">
          &ldquo;Implied&rdquo; is the average market probability within the cut; &ldquo;Realized&rdquo; is
          how often that favorite actually won. A negative gap means favorites in that cut won less often
          than the market priced — could be normal variance at this sample size, not necessarily a real
          pattern. No round/seed/surface data is stored, so these two cuts (probability tier, tour) are the
          only breakdowns currently possible.
        </p>
      </div>
      <CalibrationTierTable title="By implied-probability tier" data={tiers} labelHeader="Tier" />
      <CalibrationTierTable title="By tour" data={tours} labelHeader="Tour" />
    </div>
  );
}

function FavoriteLossesPanel({ rows }: { rows: TennisFavoriteLossRow[] }) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2 mb-1">
        <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
          Favorite Losses (Upsets)
        </h3>
        <span className="text-xs text-muted-foreground">{rows.length} settled</span>
      </div>
      {rows.length === 0 ? (
        <p className="text-xs text-muted-foreground">No favorite losses yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm border-collapse min-w-[560px]">
            <thead>
              <tr className="border-b text-xs uppercase text-muted-foreground">
                <th className="py-1 text-left">Date</th>
                <th className="py-1 text-left">Tour</th>
                <th className="py-1 text-left">Favorite (lost)</th>
                <th className="py-1 text-right">Market%</th>
                <th className="py-1 text-right">Odds</th>
                <th className="py-1 text-left">Result</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.matchId} className="border-b last:border-0">
                  <td className="py-1.5 text-muted-foreground">{r.matchDate ?? "—"}</td>
                  <td className="py-1.5 text-muted-foreground">{r.tour}</td>
                  <td className="py-1.5 font-medium">{r.favorite}</td>
                  <td className="py-1.5 text-right tabular-nums">{pct(r.marketProb)}</td>
                  <td className="py-1.5 text-right tabular-nums">{fmtMl(r.marketOdds)}</td>
                  <td className="py-1.5 text-xs text-muted-foreground">{r.resultDetail ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default function WimbledonClient({
  bets,
  backtest,
  legacyBetSummary,
  favoriteDog,
  favoriteLosses,
  favoriteCalibration,
}: {
  bets: TennisBetRow[];
  backtest: TennisBetBacktestRow[];
  legacyBetSummary: TennisLegacyBetSummary;
  favoriteDog: TennisFavoriteDogRow[];
  favoriteLosses: TennisFavoriteLossRow[];
  favoriteCalibration: TennisFavoriteCalibrationRow[];
}) {
  return (
    <div className="space-y-6 p-6 max-w-5xl mx-auto">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h1 className="text-xl font-bold">Wimbledon 🎾 — Analytics &amp; Ledger</h1>
        <span className="text-xs text-muted-foreground">Scoped to tournament = Wimbledon only</span>
      </div>
      <FavoriteDogPanel rows={favoriteDog} />
      <FavoriteCalibrationPanel rows={favoriteCalibration} />
      <FavoriteLossesPanel rows={favoriteLosses} />
      <TennisResults bets={bets} backtest={backtest} legacyBetSummary={legacyBetSummary} />
    </div>
  );
}
