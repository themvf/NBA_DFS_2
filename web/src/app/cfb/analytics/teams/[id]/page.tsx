import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { getCfbAnalyticsTeam } from "@/db/cfb-analytics";
import { AnalyticsShell, formatEt, GameCard, TeamFeaturePanel } from "../../_components";
import styles from "../../analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Team Analysis" };

export default async function CfbAnalyticsTeamPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const teamId = Number(id);
  if (!Number.isSafeInteger(teamId) || teamId <= 0) notFound();
  const result = await getCfbAnalyticsTeam(teamId);
  if (!result) notFound();
  const { team, results, games } = result;
  return <AnalyticsShell eyebrow={`CFB · ${team.conference ?? "TEAM"}`} title={team.name} description="Standalone team history and current-season context. Follow a matchup to compare this profile with its opponent and the observed market.">
    <p className={styles.breadcrumb}><Link href="/cfb/analytics">Analytics</Link> / <Link href="/cfb/analytics/teams">Teams</Link> / {team.name}</p>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Current snapshot</h2><p>Scores-based features · descriptive</p></div><TeamFeaturePanel team={team} /></section>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Games in the next 14 days</h2><p>Open any matchup as a standalone page</p></div>{games.length ? <div className={styles.grid}>{games.map((game) => <GameCard key={game.id} game={game} />)}</div> : <p className={styles.empty}>No game is scheduled in the current research window.</p>}</section>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Recent completed games</h2><p>Final scores from CFBD · latest eight</p></div><div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Date</th><th>Opponent</th><th>Site</th><th>Result</th></tr></thead><tbody>{results.map((game) => <tr key={game.id}><td>{game.date}</td><td><Link href={`/cfb/analytics/games/${game.id}`}>{game.opponent}</Link></td><td>{game.home ? "Home" : "Away"}</td><td>{game.pointsFor}–{game.pointsAgainst}</td></tr>)}</tbody></table></div>{!results.length && <p className={styles.empty}>No completed results are available.</p>}</section>
    <section className={styles.section}><div className={styles.note}><strong>Feature timing</strong><p>The current card was built {team.feature ? formatEt(team.feature.asOf, true) : "at an unavailable time"}. Older game pages select only feature snapshots available before that game’s kickoff. Roster and returning-production context describe availability and continuity; they are not a game forecast.</p></div></section>
  </AnalyticsShell>;
}
