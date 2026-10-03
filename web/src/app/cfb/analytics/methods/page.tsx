import type { Metadata } from "next";
import { getCfbAnalyticsCoverage, getCfbForecastValidation, getCfbChallengerValidation } from "@/db/cfb-analytics";
import { AnalyticsShell, formatEt } from "../_components";
import styles from "../analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Analytics Methods & Coverage" };

const foundations = [
  ["Identity and availability", "CFBD game and team IDs anchor every view. A pregame feature must have been available by the relevant kickoff or analysis time."],
  ["Current-season plays", "Completed 2026 games now have an incremental play and drive refresh. Missing completed-game feeds fail its coverage audit rather than silently becoming zero efficiency."],
  ["Offense and defense efficiency", "The original research model blends prior FBS scores and play PPA. A separate challenger predicts points per drive. Opponent-adjusted play PPA remains future work."],
  ["Opponent adjustment", "Team cards show an SRS-style margin adjustment. The score challenger adjusts earlier points per drive for opponents known before kickoff; its accuracy is graded separately."],
  ["Pace and possessions", "The challenger estimates offensive drives from each team's prior offense and opposing defense before projecting scoring per drive. Competitive-state pace still needs a validated definition."],
  ["Explosiveness and consistency", "A future profile can distinguish big-play dependence from repeatable efficiency using the stored play trail."],
  ["Field position and scoring", "Historical drive starts and outcomes are stored. The possession challenger predicts points per drive without a separate field-position adjustment."],
  ["Situational matchups", "Down, distance, field position, and play type support future offense-versus-defense splits, once current data and sample checks are in place."],
  ["Roster and coaching context", "Prospective snapshots support continuity and returning-production displays. Missing player availability is not inferred from roster membership."],
  ["Market comparison and validation", "The score model has a prior-season holdout and a current-season retrospective comparison with accepted pregame sportsbook captures. It remains research-only because it has not beaten the market benchmarks."],
] as const;

export default async function CfbAnalyticsMethodsPage() {
  let coverage: Awaited<ReturnType<typeof getCfbAnalyticsCoverage>> = [];
  let validation: Awaited<ReturnType<typeof getCfbForecastValidation>> = null;
  let challenger: Awaited<ReturnType<typeof getCfbChallengerValidation>> = null;
  let coverageUnavailable = false;
  let validationUnavailable = false;
  try { coverage = await getCfbAnalyticsCoverage(); }
  catch (error) { console.error("CFB analytics coverage unavailable", error); coverageUnavailable = true; }
  try { validation = await getCfbForecastValidation(); }
  catch (error) { console.error("CFB forecast validation unavailable", error); validationUnavailable = true; }
  try { challenger = await getCfbChallengerValidation(); }
  catch (error) { console.error("CFB challenger validation unavailable", error); }
  const better = (model: number | null, market: number | null) =>
    model == null || market == null ? "—" : model < market ? "Model" : "Market";
  const metric = (group: Record<string, { n: number; mean: number | null }>, key: string) => group[key]?.mean?.toFixed(key.includes("brier") ? 3 : 2) ?? "—";
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
    <section className={styles.section}>
      <div className={styles.sectionHead}><h2>Possession challenger validation</h2><p>{challenger ? `${challenger.version} · refreshed ${formatEt(challenger.generatedAt, true)}` : "Awaiting first frozen run"}</p></div>
      {challenger ? <>
        <div className={styles.note}><strong>Research only · market benchmark not passed</strong><p>The challenger adjusts prior scoring for opponent strength and predicts points per possession. It is evaluated on the same games as the original score model. The 2025 holdout has {challenger.holdoutGames} games; the 2026 retrospective replay has {challenger.forwardGames}. {challenger.prospectiveGames} challenger forecasts have settled after being frozen before kickoff.</p></div>
        <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Measure</th><th>2025 challenger</th><th>2025 original</th><th>2026 challenger</th><th>2026 original</th><th>2026 market</th></tr></thead><tbody>
          <tr><td>Spread error, points</td><td>{metric(challenger.holdout, "model_margin_error")}</td><td>{metric(challenger.holdout, "baseline_margin_error")}</td><td>{metric(challenger.forward, "market_model_margin_error")}</td><td>{metric(challenger.forward, "market_baseline_margin_error")}</td><td>{metric(challenger.forward, "market_margin_error")}</td></tr>
          <tr><td>Total error, points</td><td>{metric(challenger.holdout, "model_total_error")}</td><td>{metric(challenger.holdout, "baseline_total_error")}</td><td>{metric(challenger.forward, "market_model_total_error")}</td><td>{metric(challenger.forward, "market_baseline_total_error")}</td><td>{metric(challenger.forward, "market_total_error")}</td></tr>
          <tr><td>Moneyline Brier score</td><td>{metric(challenger.holdout, "model_brier")}</td><td>{metric(challenger.holdout, "baseline_brier")}</td><td>{metric(challenger.forward, "market_model_brier")}</td><td>{metric(challenger.forward, "market_baseline_brier")}</td><td>{metric(challenger.forward, "market_brier")}</td></tr>
        </tbody></table></div>
        <p className={styles.empty}>Lower is better. The 2026 replay is retrospective and cannot prove future performance. The market quotes are accepted pregame captures. The challenger stays research-only while it trails the market.</p>
        {challenger.prospectiveGames > 0 && <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Prospective measure</th><th>Challenger</th><th>Market at freeze</th></tr></thead><tbody>
          <tr><td>Spread error</td><td>{metric(challenger.prospective, "model_margin_error")}</td><td>{metric(challenger.prospective, "market_margin_error")}</td></tr>
          <tr><td>Total error</td><td>{metric(challenger.prospective, "model_total_error")}</td><td>{metric(challenger.prospective, "market_total_error")}</td></tr>
          <tr><td>Moneyline Brier score</td><td>{metric(challenger.prospective, "model_brier")}</td><td>{metric(challenger.prospective, "market_brier")}</td></tr>
        </tbody></table></div>}
      </> : <p className={styles.empty}>The challenger comparison will appear after its first pregame run.</p>}
    </section>
    <section className={styles.section}><div className={styles.note}><strong>Interpretation rules</strong><p>Historical CFBD line references are not verified sportsbook closes. A missing metric stays blank. New score or probability models should be trained and evaluated only with evidence available before each game, against a contemporaneous market baseline, before they appear beside observed lines.</p></div></section>
  </AnalyticsShell>;
}
