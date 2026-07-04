"use client";

import type { TennisBetRow, TennisBetBacktestRow, TennisFavoriteDogRow } from "@/db/queries";
import { TennisResults } from "../tennis-vegas-client";

const pct = (v: number | null) => (v == null ? "—" : `${(v * 100).toFixed(1)}%`);

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

export default function WimbledonClient({
  bets,
  backtest,
  favoriteDog,
}: {
  bets: TennisBetRow[];
  backtest: TennisBetBacktestRow[];
  favoriteDog: TennisFavoriteDogRow[];
}) {
  return (
    <div className="space-y-6 p-6 max-w-5xl mx-auto">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h1 className="text-xl font-bold">Wimbledon 🎾 — Analytics &amp; Ledger</h1>
        <span className="text-xs text-muted-foreground">Scoped to tournament = Wimbledon only</span>
      </div>
      <FavoriteDogPanel rows={favoriteDog} />
      <TennisResults bets={bets} backtest={backtest} />
    </div>
  );
}
