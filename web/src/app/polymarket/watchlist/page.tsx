export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import {
  getLatestCaptureAt,
  getSportBreakdown,
  getWatchlistMeta,
  getWatchlistPositions,
  getWatchlistWallets,
} from "./queries";
import { WatchlistClient } from "./watchlist-client";

export const metadata: Metadata = {
  title: "Polymarket Wallet Watchlist",
  description:
    "Frozen research cohort of Polymarket wallets with open positions and scope tagging.",
};

export default async function PolymarketWatchlistPage() {
  // Resolve the capture ONCE and pass it into every query. Each query used
  // to inline its own MAX() subquery, so a capture landing between the four
  // parallel round trips could leave the wallet table describing a different
  // snapshot than the positions table below it, with nothing on the page
  // saying so.
  const capturedAt = await getLatestCaptureAt();
  const [wallets, positions, breakdown, meta] = await Promise.all([
    getWatchlistWallets(capturedAt),
    getWatchlistPositions(capturedAt),
    getSportBreakdown(capturedAt),
    getWatchlistMeta(capturedAt),
  ]);
  return (
    <WatchlistClient
      wallets={wallets}
      positions={positions}
      breakdown={breakdown}
      meta={meta}
    />
  );
}
