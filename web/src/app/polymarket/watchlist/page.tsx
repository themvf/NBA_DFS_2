export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import {
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
  const [wallets, positions, breakdown, meta] = await Promise.all([
    getWatchlistWallets(),
    getWatchlistPositions(),
    getSportBreakdown(),
    getWatchlistMeta(),
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
