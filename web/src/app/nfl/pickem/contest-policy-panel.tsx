"use client";
import { useEffect, useState, useTransition } from "react";
import { compareContestCards, EMPTY_POOL_CONFIG, type ContestComparison, type PoolConfig } from "@/lib/nfl/pickem-contest";
import type { Entry, FieldModel, PickemGame } from "@/lib/nfl/pickem-strategy";
import { savePickemPoolConfig } from "./actions";

const dollars = (v: number | null) => v == null ? "Unavailable" : `$${v.toFixed(2)}`;
export function ContestPolicyPanel({ games, future, model, poolId, initialConfig, onReport, onAdopt }: {
  games: PickemGame[]; future: PickemGame[]; model: FieldModel; poolId: number | null; initialConfig?: PoolConfig | null;
  onReport: (report: ContestComparison | null) => void; onAdopt: (entry: Entry) => void;
}) {
  const [config, setConfig] = useState<PoolConfig>(initialConfig ?? EMPTY_POOL_CONFIG);
  const [report, setReport] = useState<ContestComparison | null>(null);
  const [message, setMessage] = useState(""); const [pending, start] = useTransition();
  const [settledOwn, setSettledOwn] = useState(config.settledWeek?.ownPoints.toString() ?? "");
  const [settledRivals, setSettledRivals] = useState(config.settledWeek?.rivalPoints.join(", ") ?? "");
  const inputKey = JSON.stringify({ games, future, model });
  useEffect(()=>{setReport(null);onReport(null);},[inputKey,onReport]);
  const change = (patch: Partial<PoolConfig>) => { setConfig(p => ({ ...p, ...patch })); setReport(null); onReport(null); };
  const number = (key: "entries" | "ownScore" | "remainingWeeks", label: string) => <label className="grid gap-1">{label}
    <input className="rounded border p-2 bg-background" type="number" min="0" value={config[key] ?? ""}
      placeholder="Unknown" onChange={e => change({ [key]: e.target.value === "" ? null : Number(e.target.value) })} /></label>;
  const list = (key: "weeklyPayouts" | "seasonPayouts" | "rivalScores", label: string) => <label className="grid gap-1">{label}
    <input className="rounded border p-2 bg-background" defaultValue={config[key]?.join(", ") ?? ""} placeholder="Unknown"
      onBlur={e => change({ [key]: e.target.value.trim() ? e.target.value.split(",").map(x => Number(x.trim())) : null,
        ...(key === "rivalScores" ? { standingsCapturedAt: new Date().toISOString() } : {}) })} /></label>;
  const select = (key: "gameTieRule" | "prizeTieRule" | "lockRule" | "sharePopulation", label: string, options: [string, string][]) =>
    <label className="grid gap-1">{label}<select className="rounded border p-2 bg-background" value={config[key] ?? ""}
      onChange={e => change({ [key]: e.target.value || null })}><option value="">Unknown</option>
      {options.map(([v, text]) => <option key={v} value={v}>{text}</option>)}</select></label>;
  const compare = () => { try {
    const observed = games.map(g => ({ ...g, fieldObservation: g.fieldHomePct == null ? undefined : {
      population: config.sharePopulation, entryCount: config.entries == null ? null : config.sharePopulation === "rivals" ? config.entries - 1 : config.entries,
      observedOwnHomePick: g.fieldObservation?.observedOwnHomePick ?? null,
      capturedAt: g.fieldObservation?.capturedAt ?? null, source: g.fieldObservation?.source ?? "manual" } }));
    const next = compareContestCards(observed, future, config, model); setReport(next); onReport(next); setMessage("");
  } catch (e) { setMessage(e instanceof Error ? e.message : "Could not compare cards"); } };
  return <section className="rounded-lg border p-4 space-y-4" aria-label="Weekly and season prize comparison">
    <h2 className="text-lg font-semibold">Weekly and season prizes</h2>
    <p className="text-sm text-muted-foreground">Straight picks: one point per correct game. Enter known rules to compare prize strategies. Blank fields stay unknown. All prize results are field-model sensitivities. For per-game locks after the week starts, enter the actual points already earned by each entry; these stay fixed while remaining games are compared.</p>
    <div className="grid gap-3 md:grid-cols-3 text-sm">
      {number("entries", "Total entries including yours")}{number("ownScore", "Your season score before this week")}{number("remainingWeeks", "Weeks after this week")}
      {list("weeklyPayouts", "Weekly payouts by rank, separated by commas")}{list("seasonPayouts", "Season payouts by rank, separated by commas")}
      {list("rivalScores", "Each rival's season score, separated by commas")}
      {select("gameTieRule", "NFL tie scoring", [["zero", "Zero points / void"], ["half", "Half point each"], ["point", "One point each"]])}
      {select("prizeTieRule", "Tied standings", [["split", "Split tied rank prizes"], ["tiebreaker", "Separate tiebreaker"]])}
      {select("lockRule", "Pick lock", [["first_kickoff", "All picks at first kickoff"], ["per_game", "Each game at kickoff"]])}
      {select("sharePopulation", "Entered pick percentages describe", [["unknown", "Unknown population"], ["rivals", "Only the other entries"], ["all", "All entries, including mine"]])}
      <label className="grid gap-1">Same card counts for both prizes<select className="rounded border p-2 bg-background"
        value={config.sameCard == null ? "" : String(config.sameCard)} onChange={e => change({ sameCard: e.target.value === "" ? null : e.target.value === "true" })}>
        <option value="">Unknown</option><option value="true">Yes</option><option value="false">No</option></select></label>
    </div>
    {config.lockRule === "per_game" && <fieldset className="border rounded p-3 space-y-2 text-sm"><legend>Already-completed games this week</legend>
      <div className="grid gap-3 md:grid-cols-2"><label>Your points so far<input className="block rounded border p-2 bg-background" type="number" min="0" step="0.5" value={settledOwn} onChange={e=>setSettledOwn(e.target.value)} /></label>
      <label>Each rival&apos;s points so far, same rival order<input className="block rounded border p-2 bg-background" value={settledRivals} onChange={e=>setSettledRivals(e.target.value)} /></label></div>
      <button className="rounded border px-2 py-1" onClick={()=>{if(!settledOwn.trim() || (config.entries !== 1 && !settledRivals.trim())) { setMessage("Enter your points and every rival's points.");return; }
        change({settledWeek:{capturedAt:new Date().toISOString(),gameIds:games.filter(g=>g.completed).map(g=>g.gameId),ownPoints:Number(settledOwn),rivalPoints:settledRivals.trim()?settledRivals.split(",").map(x=>Number(x.trim())):[]}});setMessage("Completed-game points captured. Compare again to use them.");}}>Capture completed-game points</button>
      <button className="ml-2 underline" onClick={()=>change({settledWeek:null})}>Clear captured points</button>
      <p className="text-xs text-muted-foreground">Covers all {games.filter(g=>g.completed).length} completed games shown this week. A game still in progress prevents comparison until its result and entry scores are known.</p>
    </fieldset>}
    <p className="text-xs text-muted-foreground">For percentages including your entry, your selected side at the time you enter the percentage is frozen as your observed pick. Future weeks use stored forecast scenarios; future strategy assumes highest expected correct.</p>
    <div className="flex gap-3"><button className="rounded border px-3 py-2" onClick={compare}>Compare cards</button>
      <button className="rounded border px-3 py-2 disabled:opacity-50" disabled={poolId == null || pending} onClick={() => start(async () => {
        const r = await savePickemPoolConfig(poolId!, config); setMessage(r.ok ? r.message : r.error);
      })}>Save pool rules</button></div>
    {message && <p role="status">{message}</p>}
    {report && <><p>Highest expected correct: {report.baseline.expectedCorrect.toFixed(2)}. Weekly expected prize: {dollars(report.baseline.weeklyPayout)}. Season expected prize: {dollars(report.baseline.seasonPayout)}.</p>
      {report.candidates.length > 0 && <div className="overflow-x-auto"><table className="w-full text-sm"><thead><tr><th>Candidate</th><th>Weekly prize</th><th>Season prize</th><th>Expected correct sacrificed</th><th>Prize change, simulation interval</th><th /></tr></thead><tbody>
        {report.candidates.map(c => <tr key={c.objective}><td>{c.objective}</td><td>{dollars(c.evaluation.weeklyPayout)}</td><td>{dollars(c.evaluation.seasonPayout)}</td><td>{c.expectedCorrectCost.toFixed(3)}</td>
          <td>{dollars(c.pairedPayoutGain)} ({dollars(c.monteCarlo95[0])} to {dollars(c.monteCarlo95[1])})</td>
          <td><button className="underline" onClick={() => onAdopt(c.entry)}>Use this card</button></td></tr>)}</tbody></table></div>}
      <ul className="text-xs text-muted-foreground list-disc pl-5">{report.warnings.map((w, i) => <li key={i}>{w}</li>)}</ul>
      {report.fieldSensitivity.length>0 && <details className="text-xs"><summary>How the field assumptions change these cards</summary>
        {report.fieldSensitivity.map((s,i)=><p key={i}>{s.objective} card · {s.scenario}: {dollars(s.pairedPayoutGain)} versus the highest-expected-correct card.</p>)}</details>}
      <p className="text-xs text-muted-foreground">Selection and evaluation use independent draws. Intervals measure simulation noise, not uncertainty about the football or field model. A small difference is inconclusive.</p>
    </>}
  </section>;
}
