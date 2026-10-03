import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { getCfbAnalyticsGame } from "@/db/cfb-analytics";
import { AnalyticsShell, formatEt, signed, TeamFeaturePanel } from "../../_components";
import styles from "../../analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Game Analysis" };

export default async function CfbAnalyticsGamePage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  const gameId = Number(id);
  if (!Number.isSafeInteger(gameId) || gameId <= 0) notFound();
  const game = await getCfbAnalyticsGame(gameId);
  if (!game) notFound();
  const terminalHref = `/cfb?date=${encodeURIComponent(game.gameDate)}&game=${game.id}`;
  return <AnalyticsShell eyebrow={`CFB · GAME ${game.cfbdGameId}`} title={`${game.away.name} at ${game.home.name}`} description={`${game.kickoffTbd ? "Kickoff time to be confirmed" : formatEt(game.kickoff, true)} · Football context, observed sportsbook prices, and a separately evaluated research forecast.`}>
    <p className={styles.breadcrumb}><Link href="/cfb/analytics">Analytics</Link> / Game analysis</p>
    <section className={styles.section}><div className={styles.note}><strong>Linked market view</strong><p>See the complete bookmaker ladder, movement history, and capture quality in the <Link className={styles.textLink} href={terminalHref}>Line Terminal for {game.gameDate} →</Link></p></div></section>
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>Observed market</h2><p>{game.capturedAt ? `Captured ${formatEt(game.capturedAt, true)} · ${game.bookmakerCount} books` : game.oddsEventMapped ? "Provider event mapped; no accepted pregame quote yet" : "Provider event unavailable"}</p></div>
      <div className={styles.threeGrid}>
        <article className={styles.panel}><p className={styles.eyebrow}>MONEYLINE</p><div className={styles.gameNumbers}><span>{game.away.name} <strong>{signed(game.awayMoneyline, 0)}</strong></span><span>{game.home.name} <strong>{signed(game.homeMoneyline, 0)}</strong></span></div><p className={styles.provenance}>Observed sportsbook prices · separate from research forecast</p></article>
        <article className={styles.panel}><p className={styles.eyebrow}>HOME SPREAD</p><strong>{signed(game.homeSpread)}</strong><p className={styles.provenance}>Observed market line · no validated fair spread</p></article>
        <article className={styles.panel}><p className={styles.eyebrow}>GAME TOTAL</p><strong>{game.total == null ? "—" : game.total.toFixed(1)}</strong><p className={styles.provenance}>Observed market line · no validated fair total</p></article>
      </div>
    </section>
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>Research score forecast</h2><p>{game.forecast ? `${game.forecast.version} · frozen ${formatEt(game.forecast.generatedAt, true)}` : "Awaiting a pregame play and drive snapshot"}</p></div>
      {game.forecast ? <>
        <div className={styles.note}><strong>{game.forecast.status === "RESEARCH_ONLY" ? "Research only · benchmark not passed" : "Market benchmark passed · no betting signal"}</strong><p>This forecast uses earlier FBS game scores, plays with PPA, and drives. The model is evaluated separately from sportsbook prices. It is not a fair line or an approved bet. <Link className={styles.textLink} href="/cfb/analytics/methods">See validation results →</Link></p></div>
        <div className={styles.threeGrid}>
          <article className={styles.panel}><p className={styles.eyebrow}>MODEL SCORE</p><strong>{game.away.name} {game.forecast.awayPoints.toFixed(1)} · {game.home.name} {game.forecast.homePoints.toFixed(1)}</strong><p className={styles.provenance}>Independent score estimate</p></article>
          <article className={styles.panel}><p className={styles.eyebrow}>MODEL HOME WIN CHANCE</p><strong>{(game.forecast.homeWinProbability * 100).toFixed(1)}%</strong><p className={styles.provenance}>Research probability, not a fair moneyline</p></article>
          <article className={styles.panel}><p className={styles.eyebrow}>MODEL MARGIN / TOTAL</p><strong>{signed(game.forecast.homePoints - game.forecast.awayPoints)} / {(game.forecast.homePoints + game.forecast.awayPoints).toFixed(1)}</strong><p className={styles.provenance}>{game.home.name} scoring margin / combined points</p></article>
        </div>
        <p className={styles.empty}>Inputs: {game.away.name} {game.forecast.awayCurrentGames} FBS games and {game.forecast.awayPpaPlays} plays with PPA; {game.home.name} {game.forecast.homeCurrentGames} FBS games and {game.forecast.homePpaPlays} plays with PPA. Prior-season information is blended in. All inputs precede this game.</p>
      </> : <p className={styles.empty}>No frozen research forecast is available for this matchup. Missing play coverage is never treated as zero efficiency.</p>}
    </section>
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>Football context</h2><p>CFBD scores, opponent adjustment, and prospective roster snapshots</p></div>
      <div className={styles.grid}><TeamFeaturePanel team={game.away} /><TeamFeaturePanel team={game.home} /></div>
    </section>
    <section className={styles.section}><div className={styles.note}><strong>How to read these numbers</strong><p>Blended points and margin summarize FBS-versus-FBS results with a prior-season blend. The opponent-adjusted margin is a team rating; subtracting two ratings is not a calibrated spread. Returning PPA is a share of player production, not this season’s EPA per play. Missing features remain blank. <Link className={styles.textLink} href="/cfb/analytics/methods">Definitions and coverage →</Link></p></div></section>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Continue exploring</h2></div><div className={styles.grid}><Link className={styles.button} href={`/cfb/analytics/teams/${game.away.id}`}>{game.away.name} profile →</Link><Link className={styles.button} href={`/cfb/analytics/teams/${game.home.id}`}>{game.home.name} profile →</Link></div></section>
  </AnalyticsShell>;
}
