import type { Metadata } from "next";
import Link from "next/link";
import { getCfbProspectiveEvaluation } from "@/db/cfb-prospective-evaluation";
import { AnalyticsShell, formatEt } from "../_components";
import styles from "../analytics.module.css";

export const dynamic = "force-dynamic";
export const metadata: Metadata = { title: "CFB Forecast Evaluation" };

const versions = [
  ["cfb-score-context-v1", "Score context"],
  ["cfb-score-possession-v2", "Possession"],
  ["cfb-score-opponent-ppa-v3", "Adjusted play value"],
] as const;
const markets = ["spread", "total", "moneyline"] as const;
const label = (market: string) => market === "moneyline" ? "Moneyline" : market === "spread" ? "Spread" : "Total";
const error = (value: number | null | undefined, market: string) =>
  value == null ? "—" : value.toFixed(market === "moneyline" ? 3 : 2);
const difference = (value: number | null | undefined, market: string) =>
  value == null ? "—" : `${value > 0 ? "+" : ""}${error(value, market)}`;
const interval = (value: number[] | null | undefined, market: string) =>
  value?.length === 2 ? `${difference(value[0], market)} to ${difference(value[1], market)}` : "Collecting dates";

export default async function CfbEvaluationPage() {
  let report: Awaited<ReturnType<typeof getCfbProspectiveEvaluation>> = null;
  let unavailable = false;
  try { report = await getCfbProspectiveEvaluation(); }
  catch (cause) { console.error("CFB prospective evaluation unavailable", cause); unavailable = true; }
  const adjusted = report?.coverage["cfb-score-opponent-ppa-v3"];
  const now = report ? Date.parse(report.generated_at) : 0;
  const nearTerm = report?.games.filter((game) =>
    game.status !== "upcoming" || Date.parse(game.kickoff) <= now + 72 * 3_600_000).slice(0, 50) ?? [];
  return <AnalyticsShell eyebrow="CFB · PROSPECTIVE RESEARCH" title="Forecast evaluation"
    description="A standalone record of what each model knew before kickoff, which markets were usable, and how forecasts performed after official finals.">
    {unavailable ? <div className={styles.note}><strong>Evaluation temporarily unavailable</strong><p>The latest grading report could not be loaded.</p></div>
      : !report ? <div className={styles.note}><strong>Awaiting the first evaluation run</strong><p>Frozen forecasts are being collected. This page will populate after the grading workflow runs.</p></div>
        : <>
          <section className={styles.section}>
            <div className={styles.sectionHead}><h2>Current research cohort</h2><p>Season {report.season} · graded {formatEt(report.generated_at, true)}</p></div>
            <div className={styles.threeGrid}>
              <div className={styles.panel}><p className={styles.eyebrow}>FROZEN BEFORE KICKOFF</p><strong>{adjusted?.frozen ?? 0}</strong><p className={styles.provenance}>Adjusted play-value version</p></div>
              <div className={styles.panel}><p className={styles.eyebrow}>OFFICIAL FINALS</p><strong>{adjusted?.final ?? 0}</strong><p className={styles.provenance}>Ready for outcome grading</p></div>
              <div className={styles.panel}><p className={styles.eyebrow}>VERIFIED CLOSES</p><strong>{adjusted?.verified_close ?? 0}</strong><p className={styles.provenance}>Scheduled-boundary close records</p></div>
            </div>
            <p className={styles.empty}>Research only. A missing final or verified close stays missing. The market-anchored challenger blends 75% observed market with 25% opponent-adjusted football context using a fixed rule.</p>
          </section>
          <section className={styles.section}>
            <div className={styles.sectionHead}><h2>Each model against its frozen market</h2><p>Lower error is better · paired at that model&apos;s forecast time</p></div>
            <div className={styles.tableWrap}><table className={styles.table}>
              <thead><tr><th>Model</th><th>Market</th><th>Settled pairs</th><th>Model error</th><th>Market error</th><th>Difference</th><th>95% interval</th><th>Verified closes</th></tr></thead>
              <tbody>{versions.flatMap(([version, name]) => markets.map((market) => {
                const value = report.market_comparison[version]?.[market];
                return <tr key={`${version}:${market}`}><td>{name}</td><td>{label(market)}</td>
                  <td>{value?.n ?? 0}</td><td>{error(value?.model_error, market)}</td>
                  <td>{error(value?.market_error, market)}</td><td>{difference(value?.model_minus_market, market)}</td>
                  <td>{interval(value?.model_minus_market_ci95, market)}</td><td>{value?.verified_close_n ?? 0}</td></tr>;
              }))}</tbody>
            </table></div>
            <p className={styles.empty}>Differences are model error minus market error; negative favors the model. Intervals resample by game date and appear after 20 settled games across four dates. Moneyline error is Brier score; spread and total use mean absolute error in points.</p>
          </section>
          <section className={styles.section}>
            <div className={styles.sectionHead}><h2>Market-anchored challenger</h2><p>Fixed 25% football adjustment · no betting signal</p></div>
            <div className={styles.tableWrap}><table className={styles.table}>
              <thead><tr><th>Market</th><th>Settled pairs</th><th>Anchored error</th><th>Market error</th><th>Difference</th><th>95% interval</th></tr></thead>
              <tbody>{markets.map((market) => {
                const value = report.market_comparison["cfb-score-opponent-ppa-v3"]?.[market];
                return <tr key={market}><td>{label(market)}</td><td>{value?.anchor_n ?? 0}</td>
                  <td>{error(value?.anchor_error, market)}</td><td>{error(value?.anchor_market_error, market)}</td>
                  <td>{difference(value?.anchor_minus_market, market)}</td>
                  <td>{interval(value?.anchor_minus_market_ci95, market)}</td></tr>;
              })}</tbody>
            </table></div>
            <p className={styles.empty}>The anchored forecast is frozen with its pregame quote. Moneyline calibration bias and log loss are retained in the versioned report; these summary rows use Brier score.</p>
          </section>
          <section className={styles.section}>
            <div className={styles.sectionHead}><h2>Strict head-to-head</h2><p>Same game and odds capture ID · forecasts within 15 minutes</p></div>
            <div className={styles.tableWrap}><table className={styles.table}>
              <thead><tr><th>Market</th><th>Games</th><th>Score context</th><th>Possession</th><th>Adjusted play</th><th>Market</th><th>Adjusted minus possession</th></tr></thead>
              <tbody>{markets.map((market) => {
                const value = report.strict_same_capture[market];
                return <tr key={market}><td>{label(market)}</td><td>{value?.n ?? 0}</td>
                  <td>{error(value?.errors["cfb-score-context-v1"], market)}</td>
                  <td>{error(value?.errors["cfb-score-possession-v2"], market)}</td>
                  <td>{error(value?.errors["cfb-score-opponent-ppa-v3"], market)}</td>
                  <td>{error(value?.market_error, market)}</td>
                  <td>{difference(value?.v3_minus_v2, market)}</td></tr>;
              })}</tbody>
            </table></div>
            <p className={styles.empty}>Games with different sportsbook captures remain in each model&apos;s separate market comparison above.</p>
          </section>
          <section className={styles.section}>
            <div className={styles.sectionHead}><h2>Coverage and exclusions</h2><p>Adjusted play-value version</p></div>
            <div className={styles.tableWrap}><table className={styles.table}>
              <thead><tr><th>Market</th><th>Eligible frozen games</th><th>Excluded</th><th>Most common reason</th></tr></thead>
              <tbody>{markets.map((market) => {
                const reasons = Object.entries(adjusted?.reasons ?? {}).filter(([key]) => key.startsWith(`${market}:`))
                  .sort((a, b) => b[1] - a[1]);
                return <tr key={market}><td>{label(market)}</td><td>{adjusted?.eligible[market] ?? 0}</td>
                  <td>{adjusted?.excluded[market] ?? 0}</td>
                  <td>{reasons[0] ? `${reasons[0][0].slice(market.length + 1).replaceAll("_", " ")} (${reasons[0][1]})` : "—"}</td></tr>;
              })}</tbody>
            </table></div>
            <p className={styles.empty}>{adjusted?.final_without_verified_close ?? 0} final games lack a verified close. <Link className={styles.textLink} href="/cfb/coverage">Inspect capture windows →</Link></p>
          </section>
          <section className={styles.section}>
            <div className={styles.sectionHead}><h2>Recent and near-term games</h2><p>Official result and source status</p></div>
            {nearTerm.length ? <div className={styles.tableWrap}><table className={styles.table}>
              <thead><tr><th>Game</th><th>Kickoff ET</th><th>Status</th><th>Verified close</th><th>Usable markets</th></tr></thead>
              <tbody>{nearTerm.map((game) => <tr key={game.id}>
                <td><Link href={`/cfb/analytics/games/${game.id}`}>{game.game}</Link></td>
                <td>{formatEt(game.kickoff, true)}</td><td>{game.status.replaceAll("_", " ")}</td>
                <td>{game.verified_close ? "Yes" : "No"}</td>
                <td>{game.eligible_markets.length ? game.eligible_markets.map(label).join(", ") : "None"}</td>
              </tr>)}</tbody>
            </table></div> : <p className={styles.empty}>No recent or near-term frozen games.</p>}
          </section>
        </>}
  </AnalyticsShell>;
}
