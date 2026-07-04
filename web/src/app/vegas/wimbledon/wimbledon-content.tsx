import { getTennisBets, getTennisBetBacktest, getTennisFavoriteDogBreakdown } from "@/db/queries";
import WimbledonClient from "./wimbledon-client";

// Dedicated Wimbledon-only view — the general /vegas?sport=tennis page covers
// all auto-discovered tournaments; this one stays scoped to Wimbledon even
// after the tour moves on, since tennis_matches.tournament already tags rows.
const TOURNAMENT = "Wimbledon";

export default async function WimbledonContent() {
  const [bets, backtest, favoriteDog] = await Promise.all([
    getTennisBets(500, TOURNAMENT),
    getTennisBetBacktest(TOURNAMENT),
    getTennisFavoriteDogBreakdown(TOURNAMENT),
  ]);
  return <WimbledonClient bets={bets} backtest={backtest} favoriteDog={favoriteDog} />;
}
