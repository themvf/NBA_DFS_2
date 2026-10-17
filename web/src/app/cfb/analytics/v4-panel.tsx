import Link from "next/link";
import type { CfbV4Evidence } from "@/db/cfb-v4";
import { formatEt } from "./_components";
import styles from "./analytics.module.css";

export function V4Panel({ snapshot }: { snapshot: { frozenAt: string; evidence: CfbV4Evidence } | null }) {
  const e = snapshot?.evidence;
  return <section className={styles.section}>
    <div className={styles.sectionHead}><h2>Drive and market challenger</h2><p>{snapshot ? `Saved ${formatEt(snapshot.frozenAt, true)}` : "Awaiting a registered forecast"}</p></div>
    <div className={styles.note}><strong>Research only · no betting clearance</strong><p>This version audits offensive scoring and possessions, selects historical weighting on earlier games, and tests whether football information improves the observed market. <Link href="/cfb/analytics/research">Registration and evidence →</Link></p></div>
    {e && <>
      <div className={styles.threeGrid}>
        <article className={styles.panel}><p className={styles.eyebrow}>FOOTBALL ESTIMATE</p><strong>{e.away} {e.raw.away_points.toFixed(1)} · {e.home} {e.raw.home_points.toFixed(1)}</strong><p className={styles.provenance}>Audited offensive scoring, possessions and separate other scoring</p></article>
        <article className={styles.panel}><p className={styles.eyebrow}>MARKET CHALLENGER</p><strong>{e.forecast ? `${e.away} ${e.forecast.away_points.toFixed(1)} · ${e.home} ${e.forecast.home_points.toFixed(1)}` : "Fresh paired quote unavailable"}</strong><p className={styles.provenance}>Football weight can be zero when earlier evidence favors the market</p></article>
        <article className={styles.panel}><p className={styles.eyebrow}>INPUT COVERAGE</p><strong>{e.input_coverage.away_games} / {e.input_coverage.home_games} audited FBS games</strong><p className={styles.provenance}>{e.away} / {e.home} current-season history</p></article>
      </div>
      <p className={styles.freshnessWarning}>{e.context.complete ? "Context observations captured. Numerical availability and weather adjustments remain unvalidated." : "Availability or weather evidence is missing, stale or unresolved. Context effects are not fitted."}</p>
      {e.missing_history_games && (e.missing_history_games.home.length + e.missing_history_games.away.length > 0) && <p className={styles.freshnessWarning}>Some prior games failed scoring or drive checks. This forecast is excluded from the primary study.</p>}
      <p className={styles.empty}>{e.market ? `${e.market.book} quote captured ${formatEt(e.market.captured_at, true)}.` : "No fresh exact bookmaker comparison."} Forecast age and source completeness must both be checked.</p>
      {e.propositions && <div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Exploratory proposition</th><th>Line / price</th><th>Win</th><th>Push</th><th>Calibration</th></tr></thead><tbody>
        {Object.entries(e.propositions).map(([key,p]) => <tr key={key}><td>{key.replaceAll("_", " ")}</td><td>{p.line ?? "—"} / {p.price > 0 ? "+" : ""}{p.price}</td><td>{p.p_win == null ? "Unavailable" : `${(p.p_win*100).toFixed(1)}%`}</td><td>{p.p_push == null ? "Unavailable" : `${(p.p_push*100).toFixed(1)}%`}</td><td>{p.p_win == null ? "More held-out games needed" : "Exploratory · unvalidated"}</td></tr>)}
      </tbody></table></div>}
    </>}
  </section>;
}
