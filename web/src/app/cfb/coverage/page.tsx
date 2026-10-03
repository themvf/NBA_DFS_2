import type { Metadata } from "next";
import Link from "next/link";
import { getCfbCoverage } from "@/db/cfb-coverage";
import type { MarketCoverage } from "@/lib/cfb-coverage";
import styles from "./coverage.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Line Coverage", description: "Upcoming CFB odds mapping, capture checkpoints, and sportsbook quote quality." };

const et = (value: string) => new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short",
}).format(new Date(value));

function marketLabel(value: MarketCoverage) {
  return <span title="Books quoting both sides / books updated within five minutes of the snapshot; shared books with the previous snapshot">
    <strong>{value.books}</strong> books · {value.freshAtCapture} fresh{value.sameBooksAsPrevious === null ? "" : ` · ${value.sameBooksAsPrevious} shared`}
  </span>;
}

export default async function CfbCoveragePage() {
  let data: Awaited<ReturnType<typeof getCfbCoverage>>;
  try { data = await getCfbCoverage(); }
  catch (error) {
    console.error("CFB coverage unavailable", error);
    return <main className={styles.page}><Link href="/cfb">← CFB terminal</Link><h1>Line coverage unavailable</h1><p>The live coverage check could not be loaded. Try again shortly.</p></main>;
  }
  const flagged = data.games.filter((game) => game.issues.length);
  const unmapped = data.games.filter((game) => !game.mapped).length;
  const overdue = data.games.filter((game) => game.issues.includes("Capture overdue") || game.issues.includes("Checkpoint due now")).length;
  return <main className={styles.page}>
    <div className={styles.links}><Link href="/cfb">← CFB terminal</Link><Link href="/cfb/analytics">CFB analytics →</Link></div>
    <header className={styles.header}><div><p className={styles.eyebrow}>LIVE OPERATIONS · NEXT 72 HOURS</p><h1>Line coverage</h1><p>See which games have an odds-event mapping, an accepted pregame capture, and comparable sportsbook quotes. This page refreshes when opened.</p></div><span>Checked {et(data.asOf)}</span></header>
    <section className={styles.summary} aria-label="Coverage summary">
      <div><strong>{data.games.length}</strong><span>upcoming games</span></div>
      <div><strong>{flagged.length}</strong><span>need review</span></div>
      <div><strong>{unmapped}</strong><span>unmapped</span></div>
      <div><strong>{overdue}</strong><span>capture due or overdue</span></div>
    </section>
    <p className={styles.note}>Market cells show books quoting both sides, books updated within five minutes of that capture, and books shared with the preceding capture. Counts describe recorded quotes; they do not establish an executable price.</p>
    {!data.games.length ? <p>No scheduled games in the next 72 hours.</p> : <div className={styles.tableWrap}><table className={styles.table}>
      <thead><tr><th>Game</th><th>Capture</th><th>Spread</th><th>Total</th><th>Moneyline</th><th>Next checkpoint / issue</th></tr></thead>
      <tbody>{data.games.map((game) => <tr key={game.id} className={game.issues.length ? styles.flagged : undefined}>
        <td><Link href={`/cfb?date=${game.gameDate}&game=${game.id}`}><strong>{game.awayTeam} at {game.homeTeam}</strong></Link><small>{et(game.kickoff)}</small></td>
        <td>{game.capturedAt ? et(game.capturedAt) : "None yet"}<small>{game.mapped ? "Event mapped" : "Event unmapped"}</small></td>
        <td>{marketLabel(game.markets.spread)}</td><td>{marketLabel(game.markets.total)}</td><td>{marketLabel(game.markets.moneyline)}</td>
        <td>{game.issues.length ? <ul>{game.issues.map((issue) => <li key={issue}>{issue}</li>)}</ul> : <span className={styles.good}>On track</span>}
          {game.nextCheckpoint && <small>Next: {et(game.nextCheckpoint.targetAt)}</small>}</td>
      </tr>)}</tbody>
    </table></div>}
  </main>;
}
