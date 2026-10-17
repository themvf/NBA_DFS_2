import { getCfbV4Study } from "@/db/cfb-v4";
import { AnalyticsShell, formatEt } from "../_components";
import styles from "../analytics.module.css";
export const dynamic = "force-dynamic";
const obj = (v: unknown): Record<string, unknown> => v && typeof v === "object" && !Array.isArray(v) ? v as Record<string, unknown> : {};
const num = (v: unknown) => typeof v === "number" ? v.toFixed(2) : "Unavailable";
export default async function RegisteredResearchPage() {
  const study = await getCfbV4Study();
  const report = study?.report ?? {}, fit = obj(report.market_fit), evaluation = obj(fit.evaluation);
  const source = obj(report.source_audit), football = obj(obj(report.football_fit).selected);
  const forward = study?.evaluation ?? {};
  return <AnalyticsShell eyebrow="CFB · REGISTERED RESEARCH" title="Drive and market challenger" description="A prospective test of corrected scoring, possession estimates and market calibration. No betting clearance.">
    <section className={styles.section}><div className={styles.note}><strong>Research only</strong><p>Statistical fitting and source checks do not establish a profitable betting strategy. Missing context and probabilities stay explicit. Earlier games are development evidence; confirmation requires a later untouched season.</p></div></section>
    <section className={styles.section}><h2>Registration</h2>{study ? <p className={styles.empty}>Registered {formatEt(study.registeredAt,true)} · source commit {study.gitSha.slice(0,8)} · protocol fingerprint {study.digest.slice(0,16)}. Only decisions saved after registration and before kickoff qualify.</p> : <p className={styles.empty}>Protocol has not yet been registered in this environment.</p>}</section>
    <section className={styles.section}><h2>Source and training coverage</h2><p className={styles.empty}>{String(source.usable ?? "—")} of {String(source.games ?? "—")} completed source games passed the scoring and possession audit. Prior strength selected: {String(football.prior_games ?? "—")} games; recency half-life: {String(football.half_life_games ?? "—")} games. Contradictory scoring and incomplete inputs remain excluded.</p><p className={styles.empty}>Market fitting: {String(fit.training_n ?? 0)} earlier games. Distribution calibration: {String(fit.calibration_n ?? 0)} later games. Development evaluation: {String(fit.evaluation_n ?? 0)} subsequent games. These inspected historical splits are not prospective confirmation.</p></section>
    <section className={styles.section}><h2>Development comparison</h2><div className={styles.tableWrap}><table className={styles.table}><thead><tr><th>Average error</th><th>Market</th><th>Football</th><th>Fixed 25% blend</th><th>Fitted challenger</th></tr></thead><tbody>{["margin","total"].map(k => { const m=obj(evaluation[k]); return <tr key={k}><td>{k}</td><td>{num(m.market_mae)}</td><td>{num(m.raw_mae)}</td><td>{num(m.fixed25_mae)}</td><td>{num(m.fitted_mae)}</td></tr>; })}</tbody></table></div></section>
    <section className={styles.section}><h2>New forward sample</h2><p className={styles.empty}>{String(forward.settled_games ?? 0)} settled games across {String(forward.game_dates ?? 0)} game dates. Review requires at least 200 games and 30 dates, followed by a separate confirmation sample. No automatic activation.</p><p className={styles.empty}>Quarterback, offensive-line, availability, continuity and weather observations are captured separately. Weather source: Open-Meteo. Their numerical effects require timestamped training evidence; unknown effects do not become zero-point adjustments.</p></section>
  </AnalyticsShell>;
}
