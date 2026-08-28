"use client";

import { useMemo, useState } from "react";
import type {
  SportBreakdownRow,
  WatchlistMeta,
  WatchlistPosition,
  WatchlistWallet,
} from "./queries";

const SPORT_ICON: Record<string, string> = {
  mlb: "⚾",
  "baseball-intl": "⚾",
  nba: "🏀",
  wnba: "🏀",
  nfl: "🏈",
  nhl: "🏒",
  soccer: "⚽",
  tennis: "🎾",
  crypto: "₿",
  politics: "🗳️",
  other: "🎲",
};

const MARKET_TYPE_LABEL: Record<string, string> = {
  moneyline: "Moneyline",
  spread: "Spread",
  total: "Total",
  prop: "Prop",
};

function pct(v: number | null | undefined, digits = 2): string {
  if (v == null || Number.isNaN(v)) return "—";
  return `${v >= 0 ? "+" : ""}${(v * 100).toFixed(digits)}%`;
}

function money(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—";
  const sign = v < 0 ? "-" : "";
  return `${sign}$${Math.abs(v).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function signedMoney(v: number | null | undefined): string {
  if (v == null || Number.isNaN(v)) return "—";
  return `${v >= 0 ? "+" : "-"}$${Math.abs(v).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function shortWallet(w: string): string {
  return w.length > 14 ? `${w.slice(0, 8)}…${w.slice(-4)}` : w;
}

function when(d: Date | null): string {
  if (!d) return "—";
  return new Date(d).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function WatchlistClient({
  wallets,
  positions,
  breakdown,
  meta,
}: {
  wallets: WatchlistWallet[];
  positions: WatchlistPosition[];
  breakdown: SportBreakdownRow[];
  meta: WatchlistMeta;
}) {
  const [scopeOnly, setScopeOnly] = useState(false);
  const [selected, setSelected] = useState<string | null>(null);

  const nameFor = useMemo(() => {
    const map = new Map<string, string>();
    for (const w of wallets) map.set(w.wallet, w.displayName || shortWallet(w.wallet));
    return map;
  }, [wallets]);

  const shownPositions = useMemo(() => {
    let rows = positions;
    if (scopeOnly) rows = rows.filter((p) => p.isInScope);
    if (selected) rows = rows.filter((p) => p.wallet === selected);
    return rows;
  }, [positions, scopeOnly, selected]);

  const breakdownFor = useMemo(() => {
    const map = new Map<string, SportBreakdownRow[]>();
    for (const row of breakdown) {
      const list = map.get(row.wallet) ?? [];
      list.push(row);
      map.set(row.wallet, list);
    }
    return map;
  }, [breakdown]);

  const inScopePct = meta.openTotal ? (meta.openInScope / meta.openTotal) * 100 : 0;

  return (
    <div className="mx-auto w-full max-w-[1400px] px-4 py-6 space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Polymarket wallet watchlist</h1>
        <p className="text-sm text-muted-foreground">
          Frozen cohort <code className="text-xs">{meta.cohortVersion}</code> · {meta.walletCount}{" "}
          selected (+{meta.controlCount} control) · frozen {when(meta.frozenAt)} · positions
          captured {when(meta.capturedAt)}
        </p>
      </header>

      {/* Trust state. Deliberately the first thing on the page. */}
      <section className="rounded-lg border border-amber-500/40 bg-amber-500/5 p-4 text-sm space-y-2">
        <div className="font-semibold text-amber-700 dark:text-amber-400">
          RESEARCH INSTRUMENT — not a signal to trade
        </div>
        <p className="text-muted-foreground">
          These wallets were selected on <strong>MLB head-to-head markets only</strong>, by
          closing-line value, in one exploratory scan. The result survived concentration,
          favourite-longshot and self-impact checks in MLB and <strong>failed in tennis</strong> —
          one sport out of two. It is a hypothesis, not a confirmed edge.
        </p>
        <p className="text-muted-foreground">
          It is also <strong>not actionable</strong>: Polymarket reports fills after the fact and
          this page refreshes on a cadence measured in hours, so every position below was taken
          before you could see it. That latency gap is independent of whether the wallets are good.
        </p>
        <p className="text-muted-foreground">
          The cohort is <strong>frozen</strong> so it can be scored forward. Membership never
          changes; a different list means a new cohort version, leaving this one intact.
        </p>
      </section>

      {/* Forward scorecard -- the only number that will eventually matter. */}
      <section className="rounded-lg border p-4">
        <h2 className="text-sm font-semibold mb-1">Forward test</h2>
        <p className="mb-2 text-sm text-muted-foreground">
          Scored against a frozen control group of{" "}
          <strong>{meta.controlCount} unselected wallets</strong>. The statistic is the{" "}
          <strong>selection gap</strong> — selected minus control — not the absolute level, which
          moves with whatever the market did in the window.
        </p>
        {meta.forwardScored === 0 ? (
          <p className="text-sm text-muted-foreground">
            No markets have started since the freeze yet, so there is nothing to score. This is the
            expected state immediately after freezing — the scorecard fills in as new games
            resolve, and only those games count.
          </p>
        ) : (
          <p className="text-sm text-muted-foreground">
            {meta.forwardScored} of {meta.walletCount} wallets scored on markets starting after the
            freeze. Per-wallet figures in the table below.
          </p>
        )}
      </section>

      {/* Scope summary */}
      <section className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Stat label="Open positions" value={String(meta.openTotal)} />
        <Stat
          label="In validated scope"
          value={`${meta.openInScope} (${inScopePct.toFixed(0)}%)`}
          tone={inScopePct < 50 && meta.openTotal > 0 ? "warn" : "normal"}
        />
        <Stat label="Value at risk" value={money(meta.valueTotal)} />
        <Stat label="Value in scope" value={money(meta.valueInScope)} />
      </section>

      {inScopePct < 50 && meta.openTotal > 0 && (
        <p className="rounded-md border border-rose-500/30 bg-rose-500/5 p-3 text-sm text-muted-foreground">
          <strong className="text-rose-600 dark:text-rose-400">Scope warning:</strong> only{" "}
          {inScopePct.toFixed(0)}% of current open positions are in the sport and market type these
          wallets were validated on. The rest is these wallets doing things we have{" "}
          <em>no evidence</em> about — different sports, spreads, totals and props. Read those rows
          as curiosity, not signal.
        </p>
      )}

      {/* Wallets */}
      <section className="space-y-2">
        <h2 className="text-sm font-semibold">Cohort</h2>
        <div className="overflow-x-auto rounded-lg border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 text-xs uppercase text-muted-foreground">
              <tr>
                <th className="px-3 py-2 text-left">#</th>
                <th className="px-3 py-2 text-left">Wallet</th>
                <th className="px-3 py-2 text-right">Dev CLV</th>
                <th className="px-3 py-2 text-right">Holdout CLV</th>
                <th className="px-3 py-2 text-right" title="Only markets starting after the freeze">
                  Forward CLV
                </th>
                <th className="px-3 py-2 text-right">Open</th>
                <th className="px-3 py-2 text-right">In scope</th>
                <th className="px-3 py-2 text-right">Value</th>
                <th className="px-3 py-2 text-right">Unrealised</th>
                <th className="px-3 py-2 text-left">Where they trade</th>
              </tr>
            </thead>
            <tbody>
              {wallets.map((w) => {
                const isSel = selected === w.wallet;
                const sports = (breakdownFor.get(w.wallet) ?? []).slice(0, 5);
                return (
                  <tr
                    key={w.wallet}
                    onClick={() => setSelected(isSel ? null : w.wallet)}
                    className={`cursor-pointer border-t transition-colors hover:bg-muted/40 ${
                      isSel ? "bg-muted/60" : ""
                    }`}
                  >
                    <td className="px-3 py-2 text-muted-foreground">{w.rankAtFreeze}</td>
                    <td className="px-3 py-2 font-medium">
                      {w.displayName || shortWallet(w.wallet)}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {pct(w.devClv)}
                      <span className="ml-1 text-xs text-muted-foreground">
                        n={w.devMarkets ?? "—"}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {pct(w.holdoutClvAtFreeze)}
                      <span className="ml-1 text-xs text-muted-foreground">
                        n={w.holdoutMarketsAtFreeze ?? "—"}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                      {w.forwardClv == null ? "not yet" : pct(w.forwardClv)}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">{w.openPositions}</td>
                    <td
                      className={`px-3 py-2 text-right tabular-nums ${
                        w.openPositions > 0 && w.openInScope === 0
                          ? "text-muted-foreground/60"
                          : ""
                      }`}
                    >
                      {w.openInScope}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">{money(w.openValue)}</td>
                    <td
                      className={`px-3 py-2 text-right tabular-nums ${
                        w.openPnl > 0
                          ? "text-emerald-600 dark:text-emerald-400"
                          : w.openPnl < 0
                            ? "text-rose-600 dark:text-rose-400"
                            : ""
                      }`}
                    >
                      {signedMoney(w.openPnl)}
                    </td>
                    <td className="px-3 py-2">
                      <div className="flex flex-wrap gap-1">
                        {sports.length === 0 ? (
                          <span className="text-xs text-muted-foreground">no open positions</span>
                        ) : (
                          sports.map((s) => (
                            <span
                              key={s.sport}
                              className={`rounded px-1.5 py-0.5 text-xs ${
                                s.sport === w.validatedSport
                                  ? "bg-sky-500/10 text-sky-700 dark:text-sky-300"
                                  : "bg-muted text-muted-foreground"
                              }`}
                            >
                              {SPORT_ICON[s.sport] ?? "•"} {s.sport} {s.positions}
                            </span>
                          ))
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-muted-foreground">
          Click a wallet to filter the positions below. Dev and holdout CLV are the figures the
          wallet was frozen on — they are in-sample for selection and cannot confirm anything.
        </p>
      </section>

      {/* Positions */}
      <section className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-sm font-semibold">
            Open positions{" "}
            <span className="font-normal text-muted-foreground">
              ({shownPositions.length}
              {selected ? ` · ${nameFor.get(selected)}` : ""})
            </span>
          </h2>
          <div className="flex items-center gap-3">
            {selected && (
              <button
                onClick={() => setSelected(null)}
                className="text-xs text-muted-foreground underline"
              >
                clear wallet filter
              </button>
            )}
            <label className="flex items-center gap-2 text-xs">
              <input
                type="checkbox"
                checked={scopeOnly}
                onChange={(e) => setScopeOnly(e.target.checked)}
              />
              validated scope only
            </label>
          </div>
        </div>
        <div className="overflow-x-auto rounded-lg border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 text-xs uppercase text-muted-foreground">
              <tr>
                <th className="px-3 py-2 text-left">Scope</th>
                <th className="px-3 py-2 text-left">Wallet</th>
                <th className="px-3 py-2 text-left">Market</th>
                <th className="px-3 py-2 text-left">Side</th>
                <th className="px-3 py-2 text-left">Type</th>
                <th className="px-3 py-2 text-right">Entry</th>
                <th className="px-3 py-2 text-right">Now</th>
                <th className="px-3 py-2 text-right">Value</th>
                <th className="px-3 py-2 text-right">Unrealised</th>
              </tr>
            </thead>
            <tbody>
              {shownPositions.length === 0 ? (
                <tr>
                  <td colSpan={9} className="px-3 py-6 text-center text-muted-foreground">
                    No positions match this filter.
                  </td>
                </tr>
              ) : (
                shownPositions.map((p, i) => (
                  <tr
                    key={`${p.wallet}-${p.title}-${p.outcome}-${i}`}
                    className={`border-t ${p.isInScope ? "" : "opacity-60"}`}
                  >
                    <td className="px-3 py-2">
                      {p.isInScope ? (
                        <span className="rounded bg-sky-500/10 px-1.5 py-0.5 text-xs text-sky-700 dark:text-sky-300">
                          in scope
                        </span>
                      ) : (
                        <span
                          className="rounded bg-muted px-1.5 py-0.5 text-xs text-muted-foreground"
                          title="Not the sport and market type this wallet was validated on"
                        >
                          unvalidated
                        </span>
                      )}
                    </td>
                    <td className="px-3 py-2 text-xs">
                      {p.displayName || shortWallet(p.wallet)}
                    </td>
                    <td className="px-3 py-2">{p.title}</td>
                    <td className="px-3 py-2">{p.outcome ?? "—"}</td>
                    <td className="px-3 py-2 text-xs text-muted-foreground">
                      {SPORT_ICON[p.sport ?? "other"] ?? "•"} {p.sport ?? "other"} ·{" "}
                      {MARKET_TYPE_LABEL[p.marketType ?? ""] ?? p.marketType ?? "unknown"}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {p.avgPrice?.toFixed(3) ?? "—"}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">
                      {p.curPrice?.toFixed(3) ?? "—"}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums">{money(p.currentValue)}</td>
                    <td
                      className={`px-3 py-2 text-right tabular-nums ${
                        (p.cashPnl ?? 0) > 0
                          ? "text-emerald-600 dark:text-emerald-400"
                          : (p.cashPnl ?? 0) < 0
                            ? "text-rose-600 dark:text-rose-400"
                            : ""
                      }`}
                    >
                      {signedMoney(p.cashPnl)}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

function Stat({
  label,
  value,
  tone = "normal",
}: {
  label: string;
  value: string;
  tone?: "normal" | "warn";
}) {
  return (
    <div className="rounded-lg border p-3">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div
        className={`text-lg font-semibold tabular-nums ${
          tone === "warn" ? "text-rose-600 dark:text-rose-400" : ""
        }`}
      >
        {value}
      </div>
    </div>
  );
}
