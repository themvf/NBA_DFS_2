import type { Metadata } from "next";
import { getCfbAnalyticsCoverage } from "@/db/cfb-analytics";
import { AnalyticsShell } from "../_components";
import styles from "../analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Analytics Methods & Coverage" };

const foundations = [
  ["Identity and availability", "CFBD game and team IDs anchor every view. A pregame feature must have been available by the relevant kickoff or analysis time."],
  ["Current-season plays", "The play feed includes PPA, down, distance, field position, and game state. Current-season play coverage must be checked before showing an efficiency trend."],
  ["Offense and defense efficiency", "Rolling play value and success profiles are a planned layer. The current team cards use final scores instead."],
  ["Opponent adjustment", "Current cards show an SRS-style margin adjustment from completed FBS games. Play-level opponent adjustment remains a separate future measure."],
  ["Pace and possessions", "Historical drives are stored. Competitive-state pace is not yet a current-season game projection."],
  ["Explosiveness and consistency", "A future profile can distinguish big-play dependence from repeatable efficiency using the stored play trail."],
  ["Field position and scoring", "Historical drive starts and outcomes are stored. They do not yet produce projected points per drive."],
  ["Situational matchups", "Down, distance, field position, and play type support future offense-versus-defense splits, once current data and sample checks are in place."],
  ["Roster and coaching context", "Prospective snapshots support continuity and returning-production displays. Missing player availability is not inferred from roster membership."],
  ["Market comparison and validation", "The Odds API supplies observed moneyline, spread, and total quotes. No independent CFBD-derived fair line or approved betting signal is shown."],
] as const;

export default async function CfbAnalyticsMethodsPage() {
  let coverage: Awaited<ReturnType<typeof getCfbAnalyticsCoverage>> = [];
  let unavailable = false;
  try { coverage = await getCfbAnalyticsCoverage(); }
  catch (error) { console.error("CFB analytics coverage unavailable", error); unavailable = true; }
  return <AnalyticsShell eyebrow="CFB · EVIDENCE CONTRACT" title="Methods & coverage" description="The ten reusable foundations behind this section. Available measurements are separated from planned analysis and from sportsbook prices.">
    <section className={styles.section}><div className={styles.note}><strong>What this section claims today</strong><p>Current team cards summarize scored FBS-versus-FBS games, opponent-adjusted margin, and point-in-time roster context. PPA is CFBD’s play value field; the returning-PPA percentage is a share of returning player production. Neither is an independent projected spread, total, or win probability.</p></div></section>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Foundation layers</h2><p>Designed to grow without changing the route structure</p></div><div className={styles.grid}>{foundations.map(([title, detail], index) => <article key={title} className={styles.foundation}><span>{String(index + 1).padStart(2, "0")}</span><div><h3>{title}</h3><p>{detail}</p></div></article>)}</div></section>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Live database coverage</h2><p>Stored rows, not a claim of pregame model readiness</p></div>{unavailable ? <p className={styles.empty}>Coverage counts are temporarily unavailable.</p> : <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Season</th><th>Plays</th><th>Plays with PPA</th><th>Drives</th><th>Games with team features</th><th>Roster teams</th></tr></thead><tbody>{coverage.map((row) => <tr key={row.season}><td>{row.season}</td><td>{row.playRows.toLocaleString()}</td><td>{row.ppaRows.toLocaleString()}</td><td>{row.driveRows.toLocaleString()}</td><td>{row.featureGames.toLocaleString()}</td><td>{row.rosterTeams.toLocaleString()}</td></tr>)}</tbody></table></div>}</section>
    <section className={styles.section}><div className={styles.note}><strong>Interpretation rules</strong><p>Historical CFBD line references are not verified sportsbook closes. A missing metric stays blank. New score or probability models should be trained and evaluated only with evidence available before each game, against a contemporaneous market baseline, before they appear beside observed lines.</p></div></section>
  </AnalyticsShell>;
}
