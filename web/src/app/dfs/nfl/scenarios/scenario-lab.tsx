"use client";

import Link from "next/link";
import { useEffect, useId, useMemo, useRef, useState } from "react";
import { ArrowLeft, ArrowRight, BarChart3, Check, Download, FlaskConical, Play, RotateCcw, Upload } from "lucide-react";
import type { ScenarioLabRequest, ScenarioLabResult } from "@/lib/nfl-dfs/scenario-lab";
import styles from "./scenario-lab.module.css";

const policyNames: Record<string, string> = { "additive-player-p90": "Sum of player P90s", "independent-lineup-p90": "Independent lineup P90", "joint-lineup-p90": "Joint lineup P90", "joint-target-probability": "Joint target probability" };
const pct = (n: number) => `${(100 * n).toFixed(1)}%`;
const points = (n: number) => n.toFixed(1);
const money = (n: number) => `$${n.toLocaleString("en-US")}`;
type Row = ScenarioLabResult["report"]["candidates"][number];

export default function ScenarioLab() {
  const [mode, setMode] = useState<"demo" | "files">("demo");
  const [format, setFormat] = useState<"classic" | "showdown">("classic");
  const [count, setCount] = useState(100);
  const [draws, setDraws] = useState(1000);
  const [seed, setSeed] = useState(20260905);
  const [target, setTarget] = useState(150);
  const [files, setFiles] = useState<Partial<Record<"salary" | "selection" | "evaluation", File>>>({});
  const [result, setResult] = useState<ScenarioLabResult | null>(null);
  const [selectedKey, setSelectedKey] = useState("");
  const [reviewKeys, setReviewKeys] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [runKey, setRunKey] = useState("");
  const worker = useRef<Worker | null>(null);
  const generation = useRef(0);
  const configKey = JSON.stringify({ mode, format, count, draws, seed, target, files: Object.entries(files).map(([k, f]) => [k, f?.name, f?.size, f?.lastModified]) });
  useEffect(() => () => { generation.current++; worker.current?.terminate(); }, []);
  const selected = result?.report.candidates.find((row) => row.key === selectedKey) ?? result?.report.candidates[0];
  const players = useMemo(() => new Map(result?.slate.players.map((p) => [p.dkPlayerId, p]) ?? []), [result]);
  const review = result?.report.candidates.filter((row) => reviewKeys.includes(row.key)) ?? [];
  const exposure = useMemo(() => {
    const totals = new Map<number, { n: number; cpt: number }>();
    for (const row of result?.report.candidates.filter((r) => reviewKeys.includes(r.key)) ?? []) {
      for (const entry of row.lineup) { const current = totals.get(entry.playerId) ?? { n: 0, cpt: 0 }; current.n++; if (entry.slot === "CPT") current.cpt++; totals.set(entry.playerId, current); }
    }
    return [...totals].sort((a, b) => b[1].n - a[1].n || a[0] - b[0]);
  }, [result, reviewKeys]);

  async function run(event: React.FormEvent) {
    event.preventDefault();
    const token = ++generation.current;
    worker.current?.terminate();
    setBusy(true); setError("");
    try {
      const request: ScenarioLabRequest = { mode, format, count, draws, seed, target };
      if (mode === "files") {
        if (!files.salary || !files.selection || !files.evaluation) throw new Error("Select all three input files before comparing.");
        if (Object.values(files).reduce((total, f) => total + (f?.size ?? 0), 0) > 40 * 1024 * 1024) throw new Error("Combined files exceed 40 MB. Use smaller scenario banks or the command-line harness.");
        const [salary, selection, evaluation] = await Promise.all([files.salary.text(), files.selection.text(), files.evaluation.text()]);
        request.files = { salary, selection, evaluation };
      }
      if (token !== generation.current) return;
      const active = new Worker(new URL("./scenario.worker.ts", import.meta.url), { type: "module" });
      worker.current = active;
      active.onmessage = (message: MessageEvent<{ ok: boolean; result?: ScenarioLabResult; error?: string }>) => {
        if (token !== generation.current) return;
        active.terminate(); worker.current = null; setBusy(false);
        if (!message.data.ok || !message.data.result) { setError(message.data.error ?? "Comparison failed."); return; }
        const next = message.data.result;
        setResult(next); setSelectedKey(next.report.selected.find((p) => p.policy === "joint-lineup-p90")!.candidateKey);
        setReviewKeys([]); setRunKey(configKey);
      };
      active.onerror = () => {
        if (token !== generation.current) return;
        active.terminate(); worker.current = null; setBusy(false); setError("The browser worker could not complete this run. Try fewer draws or reload the page.");
      };
      active.postMessage(request);
    } catch (cause) { if (token === generation.current) { setBusy(false); setError(cause instanceof Error ? cause.message : "Unable to read inputs."); } }
  }

  function cancel() { generation.current++; worker.current?.terminate(); worker.current = null; setBusy(false); setError("Run canceled. Previous results, if any, are retained."); }
  function download() {
    if (!result) return;
    const url = URL.createObjectURL(new Blob([JSON.stringify({ ...result, reviewSet: { candidateKeys: reviewKeys, description: "Manually selected research candidates; not an optimized portfolio" } }, null, 2)], { type: "application/json" }));
    const anchor = document.createElement("a"); anchor.href = url; anchor.download = `nfl-scenario-${result.slate.format}-${result.digests.candidates.slice(0, 10)}.json`; anchor.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  return <div className={styles.lab}>
    <nav className={styles.breadcrumb}><Link href="/dfs/nfl"><ArrowLeft size={14} /> NFL DFS</Link><span>/</span><Link href="/dfs/nfl/model">Model Lab</Link><span>/</span><span>Scenario Lab</span></nav>
    <header className={styles.hero}>
      <div><p className={styles.eyebrow}><FlaskConical size={15} /> NFL · DISTRIBUTION RESEARCH</p><h1>Scenario Lab<span>.</span></h1><p className={styles.intro}>One lineup. Many possible Sundays.<br />Explore how shared game outcomes change the shape of a lineup.</p></div>
      <div className={styles.heroAside}><span className={styles.researchBadge}>RESEARCH WORKSPACE</span><p>Complete-lineup distributions<br />Separate evaluation draws<br />Reproducible comparisons</p><div className={styles.heroLines} aria-hidden="true">{[30, 48, 79, 100, 91, 65, 39, 19].map((height, i) => <i key={i} style={{ height: `${height}%` }} />)}</div></div>
    </header>

    <div className={styles.workspace}>
      <aside className={styles.controls}>
        <form onSubmit={run}>
          <fieldset disabled={busy}><legend>01 <span>Set up a comparison</span></legend>
            <div className={styles.segment} aria-label="Input source"><button type="button" aria-pressed={mode === "demo"} onClick={() => setMode("demo")}>Demo slate</button><button type="button" aria-pressed={mode === "files"} onClick={() => setMode("files")}>My files</button></div>
            {mode === "demo" ? <><p className={styles.help}>Synthetic football events. Explore the mechanics without connecting data.</p><label>Contest format<select value={format} onChange={(e) => { setFormat(e.target.value as typeof format); setTarget(e.target.value === "classic" ? 150 : 90); }}><option value="classic">Classic · 9 players</option><option value="showdown">Showdown · CPT + 5 FLEX</option></select></label></> : <><p className={styles.help}>Files stay in this browser. Supply a DK salary CSV and two separate stat-scenario banks.</p>{([['salary', 'DraftKings salaries', '.csv'], ['selection', 'Selection scenarios', '.json'], ['evaluation', 'Evaluation scenarios', '.json']] as const).map(([key, label, accept]) => <label className={styles.fileLabel} key={key}><span><Upload size={13} /> {label}</span><input type="file" accept={accept} onChange={(e) => setFiles((previous) => ({ ...previous, [key]: e.target.files?.[0] }))} /></label>)}<p className={styles.help}>40 MB combined · up to 3,000 draws per bank. Format is read from the salary file.</p></>}
            <div className={styles.twoFields}><label>Candidates<input type="number" min={1} max={150} required value={count} onChange={(e) => setCount(e.target.valueAsNumber)} /></label><label>Score target<input type="number" min={-100} max={1000} step={.1} required value={target} onChange={(e) => setTarget(e.target.valueAsNumber)} /></label></div>
            <p className={styles.help}>Probability of reaching this fantasy-score target. It is not a contest cash line.</p>
            {mode === "demo" && <label>Draws per bank<select value={draws} onChange={(e) => setDraws(Number(e.target.value))}><option value={500}>500 · quick exploration</option><option value={1000}>1,000 · standard</option><option value={2000}>2,000 · finer comparison</option><option value={3000}>3,000 · extended</option></select></label>}
            <label>Reproducible seed<input type="number" min={0} max={4294967295} required value={seed} onChange={(e) => setSeed(e.target.valueAsNumber)} /></label>
            <button className={styles.runButton} type="submit"><Play size={15} />{busy ? "Comparing scenarios…" : "Compare scenarios"}<ArrowRight size={15} /></button>
          </fieldset>
        </form>
        {busy && <button className={styles.cancelButton} onClick={cancel}>Cancel run</button>}
        <p className={styles.status} role="status" aria-live="polite">{busy ? "Running in a background worker. You can keep browsing the results below." : result ? `${result.report.manifest.candidateCount} candidates compared · ${(result.execution.elapsedMs / 1000).toFixed(1)}s` : "Ready when you are."}</p>
        {error && <div className={styles.error} role="alert">{error}</div>}
        <div className={styles.method}><span>THE COMPARISON</span><ol><li>Keep the player outcomes fixed.</li><li>Change how outcomes combine.</li><li>Choose on selection draws.</li><li>Inspect separate evaluation draws.</li></ol><p>Ownership, payouts and Kelly allocation are not part of this experiment.</p></div>
      </aside>

      <div className={styles.results}>
        {!result || !selected ? <section className={styles.empty}><div className={styles.emptyIcon}><BarChart3 size={34} /></div><span className={styles.eyebrow}>SEE THE WHOLE LINEUP</span><h2>A ceiling isn’t a sum of ceilings.</h2><p>Start a demo to compare player-level P90, independent outcomes, and shared game scenarios side by side.</p><div className={styles.emptySteps}><span>100 candidates</span><ArrowRight size={16} /><span>Shared outcomes</span><ArrowRight size={16} /><span>A different picture</span></div><p className={styles.help}>No scores are shown until an experiment has run.</p></section> : <>
          <div className={styles.resultHeader}><div><span className={styles.sourceBadge}>{result.report.source === "synthetic" ? "SYNTHETIC DATA" : "SUPPLIED MODEL · UNVALIDATED"}</span><h2>{result.slate.format === "classic" ? "Classic" : "Showdown"} comparison</h2><p>{result.report.manifest.candidateCount} candidates · {result.report.manifest.selectionDraws.toLocaleString()} selection / {result.report.manifest.evaluationDraws.toLocaleString()} evaluation draws</p></div><button className={styles.outlineButton} onClick={download}><Download size={14} /> Export evidence</button></div>
          {runKey !== configKey && <div role="status" className={styles.notice}>Inputs changed. These results still describe the previous run.</div>}
          {result.execution.searchStatus !== "complete" && <div role="status" className={styles.notice}>Search reached its limit: {result.report.manifest.candidateCount} of {result.execution.requestedCount} requested candidates. This is not proof of infeasibility.</div>}
          <p className={styles.disclaimer}>{result.report.source === "synthetic" ? "Illustrative events, not historical NFL results or calibrated projections." : "Uploaded outcomes are not independently certified for football coherence or calibration."} Target probabilities are not cash rates or winning probabilities.</p>
          <section aria-label="Policy winners" className={styles.policyGrid}>{result.report.selected.map((policy) => <button key={policy.policy} className={styles.policyCard} aria-pressed={policy.candidateKey === selected.key} onClick={() => setSelectedKey(policy.candidateKey)}><span>{policyNames[policy.policy]}</span><strong>Candidate {result.report.candidates.findIndex((row) => row.key === policy.candidateKey) + 1}</strong><small>{policy.policy === "joint-target-probability" ? pct(policy.selectionObjective) : `${points(policy.selectionObjective)} pts`} <span>selection objective</span></small></button>)}</section>

          <section className={styles.panel}><div className={styles.panelHeading}><div><span className={styles.sectionNumber}>02 / EXPLORE THE CANDIDATES</span><h3>Expected score meets target probability</h3><p>Click a dot or use the candidate selector. Policy winners were chosen on selection draws; this view uses evaluation draws.</p></div></div><CandidateScatter rows={result.report.candidates} selectedKey={selected.key} onSelect={setSelectedKey} /></section>

          <section className={styles.panel}>
            <div className={styles.panelHeading}><div><span className={styles.sectionNumber}>03 / INSPECT A LINEUP</span><h3>How wide is the range?</h3></div><label className={styles.candidateSelect}>Candidate<select aria-label="Inspect candidate" value={selected.key} onChange={(e) => setSelectedKey(e.target.value)}>{result.report.candidates.map((row, i) => <option key={row.key} value={row.key}>Candidate {i + 1} · {money(row.salary)}</option>)}</select></label></div>
            <div className={styles.metrics}><Metric label="Mean score" value={points(selected.evaluation.joint.mean)} unit="FPTS" /><Metric label="P10 / P90" value={`${points(selected.evaluation.joint.p10)} / ${points(selected.evaluation.joint.p90)}`} unit="FPTS" /><Metric label={`Reaches ${result.report.manifest.target} points`} value={pct(selected.evaluation.joint.targetProbability)} unit={selected.evaluation.joint.monteCarlo95 ? `MC 95%: ${selected.evaluation.joint.monteCarlo95.map(pct).join("–")}` : "MC interval unavailable"} /></div>
            <div className={styles.chartHeading}><h4>Outcome distribution</h4><div className={styles.legend}><span><i />Joint game draws</span><span><i />Independent baseline</span></div></div>
            <OutcomeHistogram bins={result.histograms[selected.key]} target={result.report.manifest.target} />
            <p className={styles.help}>The interval describes simulation noise only. P10 and P90 are percentiles, not guaranteed limits.</p>
            <details className={styles.details}><summary>View distribution values</summary><div className={styles.tableScroll}><table><caption>Evaluation score distribution</caption><thead><tr><th>Score bin</th><th>Joint</th><th>Independent</th></tr></thead><tbody>{result.histograms[selected.key].map((bin, i, all) => <tr key={bin.start}><td>{bin.start} to {bin.end}{i === all.length - 1 ? " inclusive" : " exclusive"}</td><td>{pct(bin.joint)}</td><td>{pct(bin.independent)}</td></tr>)}</tbody></table></div></details>
            <div className={styles.rosterHeading}><h4>Candidate roster · {money(selected.salary)}</h4><button className={styles.outlineButton} disabled={!reviewKeys.includes(selected.key) && reviewKeys.length >= 20} onClick={() => setReviewKeys((previous) => previous.includes(selected.key) ? previous.filter((key) => key !== selected.key) : [...previous, selected.key])}>{reviewKeys.includes(selected.key) ? <Check size={14} /> : "+"}{reviewKeys.includes(selected.key) ? "In review set" : "Add to review set"}</button></div>
            <div className={styles.roster}>{selected.lineup.map((entry, index) => { const player = players.get(entry.playerId)!; return <div key={index}><span className={styles.slot}>{entry.slot}</span><strong>{player.name.replace(/^SYNTHETIC /, "")}</strong><span>{player.gameKey}</span><b>{money(entry.slot === "CPT" ? player.captain!.salary : player.salary)}</b></div>; })}</div>
          </section>

          <section className={styles.panel}><div className={styles.panelHeading}><div><span className={styles.sectionNumber}>04 / REVIEW CONCENTRATION</span><h3>Your review set <span className={styles.count}>{review.length}/20</span></h3><p>Manually selected candidates. Exposure constraints and joint portfolio optimization are not applied.</p></div>{review.length > 0 && <button className={styles.outlineButton} onClick={() => setReviewKeys([])}><RotateCcw size={13} /> Clear</button>}</div>
            {!review.length ? <p className={styles.reviewEmpty}>Add candidates from the roster panel to compare shared players and exposure.</p> : <div className={styles.reviewGrid}><div><h4>Player exposure</h4><div className={styles.exposure}>{exposure.map(([id, total]) => <div key={id}><div><span>{players.get(id)?.name.replace(/^SYNTHETIC /, "")}</span><b>{total.n}/{review.length} · {pct(total.n / review.length)}</b></div><div className={styles.track}><i style={{ width: `${100 * total.n / review.length}%` }} /></div>{total.cpt > 0 && <small>CPT in {total.cpt} of {review.length}</small>}</div>)}</div></div><div><h4>Shared players with selected candidate</h4><div className={styles.overlap}>{review.map((row) => { const overlap = row.lineup.filter((e) => selected.lineup.some((s) => s.playerId === e.playerId)).length; return <button key={row.key} aria-pressed={row.key === selected.key} onClick={() => setSelectedKey(row.key)} style={{ backgroundColor: `rgba(20,184,166,${.08 + overlap / row.lineup.length * .32})` }} title={`${overlap} shared players`}><span>#{result.report.candidates.findIndex((c) => c.key === row.key) + 1}</span><strong>{overlap}/{row.lineup.length}</strong></button>; })}</div><p className={styles.help}>CPT and FLEX count as the same underlying player for overlap.</p></div></div>}
          </section>
          <details className={styles.provenance}><summary>Run provenance & limitations</summary><dl><dt>Model</dt><dd>{result.report.manifest.selection.modelVersion}</dd><dt>Input snapshot</dt><dd>{result.report.manifest.selection.snapshotId}</dd><dt>Decision cutoff</dt><dd>{result.report.manifest.selection.decisionAt}</dd><dt>Scorer</dt><dd>{result.report.manifest.scorerVersion}</dd><dt>Candidate digest</dt><dd>{result.digests.candidates}</dd><dt>Selection / evaluation seeds</dt><dd>{result.report.manifest.selection.seed} / {result.report.manifest.evaluation.seed}</dd></dl><ul>{result.report.limitations.map((message) => <li key={message}>{message}</li>)}</ul>{result.slate.warnings.length > 0 && <p>Pool notes: {result.slate.warnings.join("; ")}</p>}</details>
        </>}
      </div>
    </div>
    <footer className={styles.footer}><FlaskConical size={14} /> NFL research · Scenario comparison precedes field modeling and bankroll allocation.</footer>
  </div>;
}

function Metric({ label, value, unit }: { label: string; value: string; unit: string }) { return <div><span>{label}</span><strong>{value}</strong><small>{unit}</small></div>; }

function CandidateScatter({ rows, selectedKey, onSelect }: { rows: Row[]; selectedKey: string; onSelect: (key: string) => void }) {
  const id = useId();
  const minX = Math.floor(Math.min(...rows.map((r) => r.evaluation.joint.mean)) / 5) * 5;
  const maxX = Math.max(minX + 5, Math.ceil(Math.max(...rows.map((r) => r.evaluation.joint.mean)) / 5) * 5);
  const maxY = Math.max(.05, Math.ceil(Math.max(...rows.map((r) => r.evaluation.joint.targetProbability)) * 20) / 20);
  const x = (value: number) => 58 + (value - minX) / (maxX - minX) * 614;
  const y = (value: number) => 230 - value / maxY * 190;
  return <svg className={styles.scatter} viewBox="0 0 710 282" aria-labelledby={id}><title id={id}>Candidate mean score versus probability of reaching the score target on evaluation draws. Use the candidate selector below for keyboard access.</title>
    {[0, .25, .5, .75, 1].map((f) => <g key={f}><line x1={58} x2={672} y1={y(f * maxY)} y2={y(f * maxY)} stroke="#e2e8f0" strokeDasharray="3 5" /><text x={48} y={y(f * maxY) + 4} textAnchor="end">{pct(f * maxY)}</text><text x={x(minX + f * (maxX - minX))} y={250} textAnchor="middle">{points(minX + f * (maxX - minX))}</text></g>)}
    <text x={58} y={19}>TARGET PROBABILITY</text><text x={365} y={275} textAnchor="middle">EXPECTED FANTASY POINTS</text>
    {[...rows.filter((row) => row.key !== selectedKey), ...rows.filter((row) => row.key === selectedKey)].map((row) => <circle key={row.key} role="button" tabIndex={-1} aria-label={`Inspect candidate ${rows.indexOf(row) + 1}`} onClick={() => onSelect(row.key)} cx={x(row.evaluation.joint.mean)} cy={y(row.evaluation.joint.targetProbability)} r={row.key === selectedKey ? 7 : 4} fill={row.key === selectedKey ? "#f97316" : "#0d9488"} opacity={row.key === selectedKey ? 1 : .55} stroke={row.key === selectedKey ? "#9a3412" : "white"} strokeWidth={row.key === selectedKey ? 2 : 1}><title>Candidate {rows.indexOf(row) + 1}: {points(row.evaluation.joint.mean)} FPTS · {pct(row.evaluation.joint.targetProbability)} target probability</title></circle>)}
  </svg>;
}

function OutcomeHistogram({ bins, target }: { bins: ScenarioLabResult["histograms"][string]; target: number }) {
  const id = useId(); const maximum = Math.max(.01, ...bins.flatMap((b) => [b.joint, b.independent]));
  const low = bins[0].start; const high = bins[bins.length - 1].end;
  const x = (score: number) => 48 + (score - low) / (high - low) * 625;
  return <svg className={styles.histogram} viewBox="0 0 710 245" role="img" aria-labelledby={id}><title id={id}>Joint versus independent evaluation outcomes. Exact values are available in the distribution table below.</title>
    {[0, .5, 1].map((f) => <g key={f}><line x1={48} x2={673} y1={200 - f * 160} y2={200 - f * 160} stroke="#e2e8f0" strokeDasharray="3 5" /><text x={40} y={204 - f * 160} textAnchor="end">{pct(f * maximum)}</text></g>)}
    {bins.map((bin) => <g key={bin.start}><rect x={x(bin.start) + 2} y={200 - bin.joint / maximum * 160} width={13} height={bin.joint / maximum * 160} rx={2} fill="#0d9488" /><rect x={x(bin.start) + 16} y={200 - bin.independent / maximum * 160} width={12} height={bin.independent / maximum * 160} rx={2} fill="#a78bfa" /><title>{bin.start}–{bin.end} points: joint {pct(bin.joint)}, independent {pct(bin.independent)}</title></g>)}
    {target >= low && target <= high && <g><line x1={x(target)} x2={x(target)} y1={28} y2={205} stroke="#ea580c" strokeWidth={2} strokeDasharray="5 4" /><text x={Math.min(625, Math.max(80, x(target)))} y={18} textAnchor="middle" fill="#c2410c">TARGET {target}</text></g>}
    {[0, .25, .5, .75, 1].map((f) => <text key={f} x={x(low + f * (high - low))} y={220} textAnchor="middle">{points(low + f * (high - low))}</text>)}<text x={360} y={241} textAnchor="middle">FANTASY POINTS</text>
  </svg>;
}
