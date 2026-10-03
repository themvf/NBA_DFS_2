import type { Metadata } from "next";
import { getCfbAnalyticsCoverage, getCfbForecastValidation } from "@/db/cfb-analytics";
import { AnalyticsShell, formatEt } from "../_components";
import styles from "../analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Analytics Methods & Coverage" };

const foundations = [
  ["Identity and availability", "CFBD game and team IDs anchor every view. A pregame feature must have been available by the relevant kickoff or analysis time."],
  ["Current-season plays", "Completed 2026 games now have an incremental play and drive refresh. Missing completed-game feeds fail its coverage audit rather than silently becoming zero efficiency."],
  ["Offense and defense efficiency", "The research score model blends prior FBS game scores and play PPA for both teams. An opponent-adjusted play-level display remains future work."],
  ["Opponent adjustment", "Current cards show an SRS-style margin adjustment from completed FBS games. Play-level opponent adjustment remains a separate future measure."],
  ["Pace and possessions", "The research score model uses prior offensive and defensive drives per game. Competitive-state pace still needs a separate validated definition."],
  ["Explosiveness and consistency", "A future profile can distinguish big-play dependence from repeatable efficiency using the stored play trail."],
  ["Field position and scoring", "Historical drive starts and outcomes are stored. They do not yet produce projected points per drive."],
  ["Situational matchups", "Down, distance, field position, and play type support future offense-versus-defense splits, once current data and sample checks are in place."],
  ["Roster and coaching context", "Prospective snapshots support continuity and returning-production displays. Missing player availability is not inferred from roster membership."],
  ["Market comparison and validation", "The score model has a prior-season holdout and a current-season retrospective comparison with accepted pregame sportsbook captures. It remains research-only because it has not beaten the market benchmarks."],
] as const;

export default async function CfbAnalyticsMethodsPage() {
  let coverage: Awaited<ReturnType<typeof getCfbAnalyticsCoverage>> = [];
  let validation: Awaited<ReturnType<typeof getCfbForecastValidation>> = null;
  let coverageUnavailable = false;
  let validationUnavailable = false;
  try { coverage = await getCfbAnalyticsCoverage(); }
  catch (error) { console.error("CFB analytics coverage unavailable", error); coverageUnavailable = true; }
  try { validation = await getCfbForecastValidation(); }
  catch (error) { console.error("CFB forecast validation unavailable", error); validationUnavailable = true; }
  const better = (model: number | null, market: number | null) =>
    model == null || market == null ? "—" : model < market ? "Model" : "Market";
  return <AnalyticsShell eyebrow="CFB · EVIDENCE CONTRACT" title="Methods & coverage" description="The ten reusable foundations behind this section. Available measurements are separated from planned analysis and from sportsbook prices.">
    <section className={styles.section}><div className={styles.note}><strong>What this section claims today</strong><p>Team cards summarize scored FBS-versus-FBS games, opponent-adjusted margin, and point-in-time roster context. The separate score forecast uses earlier FBS games and is labeled research-only. PPA is CFBD’s play value field; returning-PPA percentage is a share of returning player production. The score forecast is not a fair betting line.</p></div></section>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Foundation layers</h2><p>Designed to grow without changing the route structure</p></div><div className={styles.grid}>{foundations.map(([title, detail], index) => <article key={title} className={styles.foundation}><span>{String(index + 1).padStart(2, "0")}</span><div><h3>{title}</h3><p>{detail}</p></div></article>)}</div></section>
    <section className={styles.section}><div className={styles.sectionHead}><h2>Live database coverage</h2><p>Stored rows, not a claim of pregame model readiness</p></div>{coverageUnavailable ? <p className={styles.empty}>Coverage counts are temporarily unavailable.</p> : <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Season</th><th>Plays</th><th>Plays with PPA</th><th>Drives</th><th>Games with team features</th><th>Roster teams</th></tr></thead><tbody>{coverage.map((row) => <tr key={row.season}><td>{row.season}</td><td>{row.playRows.toLocaleString()}</td><td>{row.ppaRows.toLocaleString()}</td><td>{row.driveRows.toLocaleString()}</td><td>{row.featureGames.toLocaleString()}</td><td>{row.rosterTeams.toLocaleString()}</td></tr>)}</tbody></table></div>}</section>
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>Score forecast validation</h2><p>{validation ? `${validation.version} · refreshed ${formatEt(validation.generatedAt, true)}` : "No published model run"}</p></div>
      {validation ? <>
        <div className={styles.note}><strong>{validation.status === "RESEARCH_ONLY" ? "Research only · market benchmark not passed" : "Market benchmark passed · no betting signal"}</strong><p>Trained on {validation.trainingGames.toLocaleString()} games from {validation.trainingSeasons[0] ?? "—"}–{validation.trainingSeasons.at(-1) ?? "—"}. The development holdout contained {validation.holdoutGames.toLocaleString()} games in {validation.holdoutSeason}. The {validation.forwardSeason} replay matched {validation.matchedMarketGames} eligible games to accepted pregame sportsbook observations. That replay is retrospective. {validation.prospectiveGames} games have settled from forecasts frozen before kickoff.</p></div>
        <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Measure</th><th>Research model</th><th>Observed market</th><th>Better</th></tr></thead><tbody>
          <tr><td>Spread error, points</td><td>{validation.modelMarginMae?.toFixed(2) ?? "—"}</td><td>{validation.marketMarginMae?.toFixed(2) ?? "—"}</td><td>{better(validation.modelMarginMae, validation.marketMarginMae)}</td></tr>
          <tr><td>Total error, points</td><td>{validation.modelTotalMae?.toFixed(2) ?? "—"}</td><td>{validation.marketTotalMae?.toFixed(2) ?? "—"}</td><td>{better(validation.modelTotalMae, validation.marketTotalMae)}</td></tr>
          <tr><td>Moneyline Brier score</td><td>{validation.modelBrier?.toFixed(3) ?? "—"}</td><td>{validation.marketBrier?.toFixed(3) ?? "—"}</td><td>{better(validation.modelBrier, validation.marketBrier)}</td></tr>
        </tbody></table></div>
        <p className={styles.empty}>Lower error is better. Spread uses predicted home margin against the pregame home spread; total uses combined score; moneyline uses vig-free market probability. Prospective comparisons use the latest market quote available when each forecast was frozen. The model needs at least 200 retrospective and 100 prospectively frozen matched games, with improvement on all three measures in both samples. That would still not establish a bet with positive expected value.</p>
        {validation.prospectiveGames > 0 && <><h3>Prospective frozen forecasts</h3><div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Measure</th><th>Research model</th><th>Observed market</th><th>Better</th></tr></thead><tbody>
          <tr><td>Spread error, points</td><td>{validation.prospectiveModelMarginMae?.toFixed(2) ?? "—"}</td><td>{validation.prospectiveMarketMarginMae?.toFixed(2) ?? "—"}</td><td>{better(validation.prospectiveModelMarginMae, validation.prospectiveMarketMarginMae)}</td></tr>
          <tr><td>Total error, points</td><td>{validation.prospectiveModelTotalMae?.toFixed(2) ?? "—"}</td><td>{validation.prospectiveMarketTotalMae?.toFixed(2) ?? "—"}</td><td>{better(validation.prospectiveModelTotalMae, validation.prospectiveMarketTotalMae)}</td></tr>
          <tr><td>Moneyline Brier score</td><td>{validation.prospectiveModelBrier?.toFixed(3) ?? "—"}</td><td>{validation.prospectiveMarketBrier?.toFixed(3) ?? "—"}</td><td>{better(validation.prospectiveModelBrier, validation.prospectiveMarketBrier)}</td></tr>
        </tbody></table></div></>}
      </> : <p className={styles.empty}>{validationUnavailable ? "Forecast validation is temporarily unavailable." : "A score forecast has not been published. Existing team and market views remain available."}</p>}
    </section>
    <section className={styles.section}><div className={styles.note}><strong>Interpretation rules</strong><p>Historical CFBD line references are not verified sportsbook closes. A missing metric stays blank. New score or probability models should be trained and evaluated only with evidence available before each game, against a contemporaneous market baseline, before they appear beside observed lines.</p></div></section>
  </AnalyticsShell>;
}
