import type { CfbSignalBacktestRow, CfbStudyStatus } from "@/db/queries";
import styles from "./cfb-evidence-loop.module.css";

type SelectedQuote = {
  book: string;
  line: string;
  price: string;
  updatedAt: string | null;
  fresh: boolean;
} | null;

function eastern(value: string): string {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", month: "short", day: "numeric",
    hour: "numeric", minute: "2-digit", timeZoneName: "short",
  }).format(new Date(value));
}

export default function CfbEvidenceLoop({
  quote, closeQuality, backtest, study, auditUnavailable,
}: {
  quote: SelectedQuote;
  closeQuality: string | null;
  backtest: CfbSignalBacktestRow[];
  study: CfbStudyStatus | null;
  auditUnavailable: boolean;
}) {
  const observations = backtest.reduce((sum, row) => sum + row.observations, 0);
  const settled = backtest.reduce((sum, row) => sum + row.settled, 0);
  const pending = backtest.reduce((sum, row) => sum + row.pending, 0);
  const voided = backtest.reduce((sum, row) => sum + row.void, 0);
  const excluded = backtest.reduce((sum, row) => sum + row.excluded, 0);
  const window = study?.window;

  return <section className={styles.loop} aria-label="CFB evidence and decision status">
    <div className={styles.heading}><strong>EVIDENCE LOOP</strong><span>RESEARCH · NO EDGE CLAIM</span></div>
    <div className={styles.steps}>
      <article>
        <span>OBSERVE</span>
        <strong>{quote ? `${quote.book} ${quote.line} ${quote.price}` : "No selected quote"}</strong>
        <p>{quote?.updatedAt ? `Book updated ${eastern(quote.updatedAt)} · ${quote.fresh ? "fresh" : "stale"}` : "Select a captured book quote."} Game close: {closeQuality?.toUpperCase() ?? "unavailable"}; selected-book close may differ.</p>
      </article>
      <article>
        <span>ORIENT</span>
        <strong>{auditUnavailable ? "Audit unavailable" : `${settled} settled / ${observations} alerts`}</strong>
        <p>{auditUnavailable ? "The economic audit could not be loaded. Counts and returns are hidden." : `${pending} pending · ${voided} void · ${excluded} excluded. Returns use resolved stakes and profit. One game can produce multiple alerts.`}</p>
      </article>
      <article>
        <span>DECIDE</span>
        <strong>{study ? `Moneyline study v${study.studyVersion}: decision denied` : "Decision status unavailable"}</strong>
        <p>{window ? `${window.label.replaceAll("_", " ")} ${window.state.replaceAll("_", " ")} · ends ${eastern(window.endsAt)}` : "Research signals have no decision clearance."}</p>
      </article>
      <article>
        <span>ACT</span>
        <strong>{quote?.fresh ? "Paper observation available" : "Wait for a fresh quote"}</strong>
        <p>{quote?.fresh ? "Record a paper position to track this quote. No wager is placed." : "The paper entry control stays disabled until an observed quote is fresh."}</p>
      </article>
    </div>
  </section>;
}
