import type { Metadata } from "next";
import Link from "next/link";
import { getCfbAnalyticsGames } from "@/db/cfb-analytics";
import { AnalyticsShell, formatEt, GameCard, signed } from "./_components";
import styles from "./analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Analytics", description: "Standalone college football game and team context." };

export default async function CfbAnalyticsPage() {
  let games: Awaited<ReturnType<typeof getCfbAnalyticsGames>> = [];
  let unavailable = false;
  try { games = await getCfbAnalyticsGames(); }
  catch (error) { console.error("CFB analytics games unavailable", error); unavailable = true; }
  const upcoming = games.filter((game) => !game.completed);
  const mapped = upcoming.filter((game) => game.oddsEventMapped).length;
  const observed = upcoming.filter((game) => game.capturedAt).length;
  return <AnalyticsShell eyebrow="CFB · FOOTBALL CONTEXT" title="CFB Analytics" description="Independent game and team research connected to the Line Terminal. Scores-based features and market quotes stay separate until an independently validated forecast exists.">
    {unavailable && <div className={styles.note}><strong>Data temporarily unavailable</strong><p>The analytics game list could not be loaded. The Line Terminal may still be available.</p></div>}
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>Upcoming research window</h2><p>Next 14 days · Eastern time</p></div>
      <div className={styles.threeGrid}>
        <div className={styles.panel}><p className={styles.eyebrow}>SCHEDULED</p><strong>{upcoming.length}</strong><p className={styles.provenance}>Canonical CFBD games in this window</p></div>
        <div className={styles.panel}><p className={styles.eyebrow}>MAPPED</p><strong>{mapped}</strong><p className={styles.provenance}>Games linked to a sportsbook event</p></div>
        <div className={styles.panel}><p className={styles.eyebrow}>OBSERVED</p><strong>{observed}</strong><p className={styles.provenance}>Games with an accepted pregame capture</p></div>
      </div>
    </section>
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>Games to open</h2><p>Each page works on its own and links back to the market</p></div>
      {upcoming.length ? <div className={styles.threeGrid}>{upcoming.slice(0, 6).map((game) => <GameCard key={game.id} game={game} />)}</div> : <p className={styles.empty}>No upcoming games are in the current window.</p>}
    </section>
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>All upcoming games</h2><p>{upcoming.length} games · direct links</p></div>
      <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Kickoff ET</th><th>Matchup</th><th>Market spread</th><th>Market total</th><th>Coverage</th></tr></thead><tbody>
        {upcoming.map((game) => <tr key={game.id}><td>{game.kickoffTbd ? "Time TBD" : formatEt(game.kickoff, true)}</td><td><Link href={`/cfb/analytics/games/${game.id}`}>{game.away.name} at {game.home.name}</Link></td><td>{signed(game.homeSpread)}</td><td>{game.total == null ? "—" : game.total.toFixed(1)}</td><td>{game.capturedAt ? `${game.bookmakerCount} books` : game.oddsEventMapped ? "Mapped" : "No event"}</td></tr>)}
      </tbody></table></div>
    </section>
    <section className={styles.section}><div className={styles.note}><strong>Build on the same evidence</strong><p><Link className={styles.textLink} href="/cfb/analytics/teams">Browse standalone team profiles →</Link> · <Link className={styles.textLink} href="/cfb/analytics/evaluation">Track frozen forecast evaluation →</Link> · <Link className={styles.textLink} href="/cfb/analytics/methods">See the ten foundation layers and coverage →</Link></p></div></section>
  </AnalyticsShell>;
}
