import "server-only";

import { db } from "@/db";
import { sql } from "drizzle-orm";
import { ensurePolymarketWatchlistTables } from "@/db/ensure-schema";

export interface WatchlistWallet {
  wallet: string;
  displayName: string | null;
  validatedSport: string;
  devClv: number | null;
  devMarkets: number | null;
  holdoutClvAtFreeze: number | null;
  holdoutMarketsAtFreeze: number | null;
  rankAtFreeze: number | null;
  frozenAt: Date;
  openPositions: number;
  openInScope: number;
  openValue: number;
  openPnl: number;
  forwardClv: number | null;
  forwardMarkets: number | null;
}

export interface WatchlistPosition {
  wallet: string;
  displayName: string | null;
  title: string;
  outcome: string | null;
  sport: string | null;
  marketType: string | null;
  isInScope: boolean;
  size: number | null;
  avgPrice: number | null;
  curPrice: number | null;
  currentValue: number | null;
  cashPnl: number | null;
  percentPnl: number | null;
  endDate: Date | null;
}

export interface SportBreakdownRow {
  wallet: string;
  sport: string;
  positions: number;
  value: number;
}

export interface WatchlistMeta {
  cohortVersion: string;
  frozenAt: Date | null;
  /** Display only -- millisecond precision is fine here. The microsecond-
   * accurate string lives in CaptureRef and never leaves the query layer. */
  capturedAt: Date | null;
  walletCount: number;
  openTotal: number;
  openInScope: number;
  valueTotal: number;
  valueInScope: number;
  forwardScored: number;
  controlCount: number;
}

export const COHORT_VERSION = "mlb-clv-v2-2026-08-28";

/** The newest COMPLETED capture.
 *
 * Two reasons this is not `MAX(captured_at)` over the positions table.
 * First, captures are append-only, so without a bound every past capture
 * would render as live. Second and worse: the ingester commits one row per
 * statement across minutes of throttled HTTP, so a bare MAX() flips to a
 * run that is still writing -- and a run that dies half way would hold the
 * highest timestamp forever, with the page unable to tell it from a good
 * one. The captures table carries completed_at, set only after the loop
 * finishes. */
export type CaptureRef = { raw: string; at: Date } | null;

/** Returned as the RAW Postgres string, not a Date.
 *
 * timestamptz carries microseconds (…:46.124503); a JS Date holds only
 * milliseconds, so round-tripping through one truncates to …:46.124 and the
 * `captured_at = $1` equality silently matches nothing. That rendered an
 * empty positions table against a capture holding 225 rows -- a wrong page,
 * not an error. The string is used for comparison; the Date is for display
 * only. */
export async function getLatestCaptureAt(): Promise<CaptureRef> {
  await ensurePolymarketWatchlistTables();
  const rows = await db.execute(sql`
    SELECT MAX(captured_at) AS ts
      FROM polymarket_watchlist_captures
     WHERE cohort_version = ${COHORT_VERSION}
       AND completed_at IS NOT NULL
  `);
  const ts = (rows.rows[0] as Record<string, unknown> | undefined)?.ts;
  if (!ts) return null;
  const raw = ts instanceof Date ? ts.toISOString() : String(ts);
  return { raw, at: new Date(raw) };
}

/** Positions belonging to wallets in the displayed 'selected' group.
 * The "selected only" invariant otherwise lives entirely in a Python
 * argument, so one changed call would silently start counting the 161
 * control wallets in stats labelled with a 20-wallet cohort. */
const SELECTED_ONLY = sql`EXISTS (
  SELECT 1 FROM polymarket_watchlist_wallets w2
   WHERE w2.wallet = p.wallet
     AND w2.cohort_version = p.cohort_version
     AND w2.cohort_group = 'selected'
)`;

export async function getWatchlistWallets(capturedAt: CaptureRef): Promise<WatchlistWallet[]> {
  await ensurePolymarketWatchlistTables();
  const rows = await db.execute(sql`
    WITH pos AS (
      SELECT wallet,
             COUNT(*)::int AS open_positions,
             COUNT(*) FILTER (WHERE is_in_scope)::int AS open_in_scope,
             COALESCE(SUM(current_value), 0) AS open_value,
             COALESCE(SUM(cash_pnl), 0) AS open_pnl
        FROM polymarket_watchlist_positions p
       WHERE cohort_version = ${COHORT_VERSION}
         AND captured_at = ${capturedAt?.raw ?? null}
         AND ${SELECTED_ONLY}
       GROUP BY wallet
    ),
    fwd AS (
      SELECT DISTINCT ON (wallet) wallet, clv, markets
        FROM polymarket_watchlist_forward
       WHERE cohort_version = ${COHORT_VERSION}
       ORDER BY wallet, scored_at DESC
    )
    SELECT w.wallet, w.display_name, w.validated_sport, w.dev_clv, w.dev_markets,
           w.holdout_clv_at_freeze, w.holdout_markets_at_freeze, w.rank_at_freeze,
           w.frozen_at,
           COALESCE(pos.open_positions, 0) AS open_positions,
           COALESCE(pos.open_in_scope, 0) AS open_in_scope,
           COALESCE(pos.open_value, 0) AS open_value,
           COALESCE(pos.open_pnl, 0) AS open_pnl,
           fwd.clv AS forward_clv, fwd.markets AS forward_markets
      FROM polymarket_watchlist_wallets w
      LEFT JOIN pos ON pos.wallet = w.wallet
      LEFT JOIN fwd ON fwd.wallet = w.wallet
     WHERE w.cohort_version = ${COHORT_VERSION}
       AND w.cohort_group = 'selected'
     ORDER BY w.rank_at_freeze
  `);
  return (rows.rows as Record<string, unknown>[]).map((r) => ({
    wallet: String(r.wallet),
    displayName: (r.display_name as string) ?? null,
    validatedSport: String(r.validated_sport),
    devClv: r.dev_clv == null ? null : Number(r.dev_clv),
    devMarkets: r.dev_markets == null ? null : Number(r.dev_markets),
    holdoutClvAtFreeze: r.holdout_clv_at_freeze == null ? null : Number(r.holdout_clv_at_freeze),
    holdoutMarketsAtFreeze:
      r.holdout_markets_at_freeze == null ? null : Number(r.holdout_markets_at_freeze),
    rankAtFreeze: r.rank_at_freeze == null ? null : Number(r.rank_at_freeze),
    frozenAt: new Date(r.frozen_at as string),
    openPositions: Number(r.open_positions),
    openInScope: Number(r.open_in_scope),
    openValue: Number(r.open_value),
    openPnl: Number(r.open_pnl),
    forwardClv: r.forward_clv == null ? null : Number(r.forward_clv),
    forwardMarkets: r.forward_markets == null ? null : Number(r.forward_markets),
  }));
}

export async function getWatchlistPositions(capturedAt: CaptureRef): Promise<WatchlistPosition[]> {
  await ensurePolymarketWatchlistTables();
  const rows = await db.execute(sql`
    SELECT p.wallet, w.display_name, p.title, p.outcome, p.sport, p.market_type,
           p.is_in_scope, p.size, p.avg_price, p.cur_price, p.current_value,
           p.cash_pnl, p.percent_pnl, p.end_date
      FROM polymarket_watchlist_positions p
      LEFT JOIN polymarket_watchlist_wallets w
        ON w.wallet = p.wallet AND w.cohort_version = p.cohort_version
     WHERE p.cohort_version = ${COHORT_VERSION}
       AND p.captured_at = ${capturedAt?.raw ?? null}
       AND ${SELECTED_ONLY}
     ORDER BY p.is_in_scope DESC, p.current_value DESC NULLS LAST
  `);
  return (rows.rows as Record<string, unknown>[]).map((r) => ({
    wallet: String(r.wallet),
    displayName: (r.display_name as string) ?? null,
    title: String(r.title ?? ""),
    outcome: (r.outcome as string) ?? null,
    sport: (r.sport as string) ?? null,
    marketType: (r.market_type as string) ?? null,
    isInScope: Boolean(r.is_in_scope),
    size: r.size == null ? null : Number(r.size),
    avgPrice: r.avg_price == null ? null : Number(r.avg_price),
    curPrice: r.cur_price == null ? null : Number(r.cur_price),
    currentValue: r.current_value == null ? null : Number(r.current_value),
    cashPnl: r.cash_pnl == null ? null : Number(r.cash_pnl),
    percentPnl: r.percent_pnl == null ? null : Number(r.percent_pnl),
    endDate: r.end_date ? new Date(r.end_date as string) : null,
  }));
}

export async function getSportBreakdown(capturedAt: CaptureRef): Promise<SportBreakdownRow[]> {
  await ensurePolymarketWatchlistTables();
  const rows = await db.execute(sql`
    SELECT p.wallet, COALESCE(p.sport, 'other') AS sport,
           COUNT(*)::int AS positions,
           COALESCE(SUM(p.current_value), 0) AS value
      FROM polymarket_watchlist_positions p
     WHERE p.cohort_version = ${COHORT_VERSION}
       AND p.captured_at = ${capturedAt?.raw ?? null}
       AND ${SELECTED_ONLY}
     GROUP BY p.wallet, COALESCE(p.sport, 'other')
     ORDER BY positions DESC
  `);
  return (rows.rows as Record<string, unknown>[]).map((r) => ({
    wallet: String(r.wallet),
    sport: String(r.sport),
    positions: Number(r.positions),
    value: Number(r.value),
  }));
}

export async function getWatchlistMeta(capturedAt: CaptureRef): Promise<WatchlistMeta> {
  await ensurePolymarketWatchlistTables();
  const rows = await db.execute(sql`
    WITH pos AS (
      SELECT p.is_in_scope, p.current_value
        FROM polymarket_watchlist_positions p
       WHERE p.cohort_version = ${COHORT_VERSION}
         AND p.captured_at = ${capturedAt?.raw ?? null}
         AND ${SELECTED_ONLY}
    )
    SELECT
      (SELECT MIN(frozen_at) FROM polymarket_watchlist_wallets
        WHERE cohort_version = ${COHORT_VERSION}) AS frozen_at,
      (SELECT COUNT(*)::int FROM polymarket_watchlist_wallets
        WHERE cohort_version = ${COHORT_VERSION}
          AND cohort_group = 'selected') AS wallet_count,
      (SELECT COUNT(*)::int FROM polymarket_watchlist_wallets
        WHERE cohort_version = ${COHORT_VERSION}
          AND cohort_group = 'control') AS control_count,
      (SELECT COUNT(*)::int FROM pos) AS open_total,
      (SELECT COUNT(*)::int FROM pos WHERE is_in_scope) AS open_in_scope,
      (SELECT COALESCE(SUM(current_value), 0) FROM pos) AS value_total,
      (SELECT COALESCE(SUM(current_value), 0) FROM pos WHERE is_in_scope) AS value_in_scope,
      (SELECT COUNT(DISTINCT wallet)::int FROM polymarket_watchlist_forward
        WHERE cohort_version = ${COHORT_VERSION}) AS forward_scored
  `);
  const r = (rows.rows[0] ?? {}) as Record<string, unknown>;
  return {
    cohortVersion: COHORT_VERSION,
    frozenAt: r.frozen_at ? new Date(r.frozen_at as string) : null,
    capturedAt: capturedAt?.at ?? null,
    walletCount: Number(r.wallet_count ?? 0),
    openTotal: Number(r.open_total ?? 0),
    openInScope: Number(r.open_in_scope ?? 0),
    valueTotal: Number(r.value_total ?? 0),
    valueInScope: Number(r.value_in_scope ?? 0),
    forwardScored: Number(r.forward_scored ?? 0),
    controlCount: Number(r.control_count ?? 0),
  };
}

