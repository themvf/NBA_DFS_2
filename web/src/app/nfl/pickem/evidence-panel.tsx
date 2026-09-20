"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import type { PickemSlateGame, PickemLedgerRow } from "@/db/queries";
import { evidenceGrades, marketReview, safeSourceUrl, scenarioDecision, timestamp,
  type GameEvidence, type PickemNews, type PickemScenario } from "@/lib/nfl/pickem-evidence";
import { savePickemNews } from "./actions";

const pct = (p: number) => `${(100 * p).toFixed(1)}%`;
function time(s: string | null) {
  return s && Number.isFinite(timestamp(s)) ? new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", month: "short", day: "numeric", hour: "numeric", minute: "2-digit", timeZoneName: "short",
  }).format(new Date(timestamp(s))) : "Unknown";
}
const signed = (n: number) => `${n > 0 ? "+" : ""}${n.toFixed(1)}`;

export function EvidencePanel({ game, evidence, now, pickHome, confidence, baselineConfidence,
  objective, manual, scenario, onScenario, peers }: {
  game: PickemSlateGame; evidence: GameEvidence; now: string; pickHome: boolean;
  confidence: number; baselineConfidence: number; objective: string; manual: boolean;
  scenario: PickemScenario | null; onScenario: (value: PickemScenario | null) => void;
  peers: Array<{ gameId: number; p: number }>;
}) {
  const review = marketReview(evidence, game.pHome, game.kickoff, now);
  const pick = pickHome ? game.homeAbbrev : game.awayAbbrev;
  const opponent = pickHome ? game.awayAbbrev : game.homeAbbrev;
  const p = pickHome ? game.pHome : 1 - game.pHome;
  const weights = scenarioDecision(scenario?.pHome ?? game.pHome, pickHome, confidence);
  const hypotheticalRank = 1 + peers.filter(g => g.gameId !== game.gameId && g.p > weights.p).length;
  const warning = !review.closed && (review.stale || review.probabilityConflict || review.favoriteChanged || review.newsAfterQuote > 0);
  return (
    <details className="mt-2 min-w-72 max-w-xl text-xs">
      <summary className={`cursor-pointer font-medium ${warning ? "text-amber-700 dark:text-amber-400" : "text-primary"}`}>
        {warning ? "Review before locking" : "Evidence & decision"} · {evidence.news.length} reports
      </summary>
      <div className="mt-2 space-y-4 rounded border bg-muted/20 p-3">
        <section className="space-y-1" aria-label="Market freshness">
          <h3 className="font-semibold">Market check</h3>
          <p>Probability calculated: {time(game.computedAt)}. Latest quote captured: {time(evidence.latest?.capturedAt ?? null)}.</p>
          {review.closed ? <p>Pregame evidence only; game has started.</p> : <>
            {review.stale && <p className="font-semibold text-amber-700 dark:text-amber-400">Quote missing or older than {review.maxAgeHours} hours. Refresh the odds pipeline before locking.</p>}
            {review.probabilityConflict && <p className="font-semibold text-rose-700 dark:text-rose-400">Displayed probability and latest market quote favor different teams.</p>}
            {review.favoriteChanged && <p className="font-semibold text-amber-700 dark:text-amber-400">Favorite changed since the first captured quote.</p>}
          </>}
          <div className="grid grid-cols-2 gap-2">
            {[{ label: "First captured", quote: evidence.opening }, { label: "Latest captured", quote: evidence.latest }].map(({ label, quote }) => (
              <div key={label} className="rounded border p-2">
                <strong>{label}</strong>
                <p>{quote?.pHome != null ? `${game.homeAbbrev} ${pct(quote.pHome)} · ${game.awayAbbrev} ${pct(1 - quote.pHome)}` : "Moneyline unavailable"}</p>
                <p>{quote?.homeSpread != null ? `${game.homeAbbrev} ${signed(quote.homeSpread)}` : "Spread unavailable"}</p>
                <p className="text-muted-foreground">{time(quote?.capturedAt ?? null)}</p>
              </div>
            ))}
          </div>
          <p className="text-muted-foreground">First captured is not necessarily the sportsbook opener. Quote age is measured separately from model calculation time.</p>
        </section>
        <section className="space-y-2 border-t pt-2" aria-label="Matchup news">
          <h3 className="font-semibold">News & availability</h3>
          {!evidence.news.length && <p>No reports captured for this matchup. This does not establish that either team is healthy.</p>}
          <div className="max-h-96 space-y-2 overflow-y-auto">{evidence.news.map(n => {
            const after = !evidence.latest || timestamp(n.publishedAt ?? n.observedAt) > timestamp(evidence.latest.capturedAt);
            return <article key={n.id} className="space-y-1 rounded border p-2">
              <p className="font-semibold">{n.team} · {n.headline}</p>
              <p className="text-muted-foreground">{n.category.replaceAll("-", " ")} · {n.status}</p>
              <p>{n.detail}</p>
              <p>{n.url && safeSourceUrl(n.url) ? <a href={n.url} target="_blank" rel="noreferrer" className="underline">{n.source}</a> : `${n.source} · article link unavailable`}</p>
              <p className="text-muted-foreground">Published: {time(n.publishedAt)} · Captured: {time(n.observedAt)}</p>
              <p className={after ? "text-amber-700 dark:text-amber-400" : "text-muted-foreground"}>
                {after ? "No quote captured after this report; refresh before relying on the price." : "Quote captured after this report; whether the news is priced in is unverified."}
              </p>
            </article>;
          })}</div>
          <p className="text-muted-foreground">Reviewed reports appear first, then recent availability observations. The player feed primarily covers fantasy positions; missing offensive-line or defensive reports do not mean those units are healthy. Reports do not automatically change win probabilities. Sources can disagree; use the linked report and final game status.</p>
          {!review.closed && <NewsForm game={game} />}
        </section>
        <section className="space-y-2 border-t pt-2" aria-label="Prior game performance">
          <h3 className="font-semibold">Behind the previous score</h3>
          {[game.homeAbbrev, game.awayAbbrev].map(team => {
            const own = evidence.performance.filter(x => x.team === team).sort((a, b) => b.week - a.week)[0];
            const opposing = own && evidence.performance.find(x => x.gameId === own.gameId && x.team !== team);
            const form = evidence.recentForm?.find(x => x.team === team);
            return <div key={team}>
              <strong>{team}{own ? ` · Week ${own.week}` : ""}</strong>
              {!own ? <p>Prior-game play data unavailable.</p> : <>
                <p>Offense: {own.plays} scrimmage plays · EPA/play {own.epaPerPlay?.toFixed(2) ?? "unknown"} · success {own.successRate == null ? "unknown" : pct(own.successRate)} · {own.rushYards ?? "unknown"} rushing yards.</p>
                <p>Defense allowed: EPA/play {opposing?.epaPerPlay?.toFixed(2) ?? "unknown"} · {opposing?.rushYards ?? "unknown"} rushing yards.</p>
                <p>Possession swings: {own.turnovers} giveaways · {opposing?.turnovers ?? "unknown"} takeaways. Special teams: {own.fieldGoalsMade} field goals made.</p>
                <p>Opponent return touchdowns: {own.defensiveReturnTdsAllowed} interception/fumble returns · {own.kickReturnTdsAllowed} punt/field-goal returns. These scores should not be blamed on the defense.</p>
              </>}
              {form && form.games > 1 ? <p className="mt-1">Recent {form.games} covered games (mean per game): offense EPA/play {form.offenseEpa?.toFixed(2) ?? "unknown"}; defense EPA/play {form.defenseEpa?.toFixed(2) ?? "unknown"}; rushing yards allowed {form.rushYardsAllowed?.toFixed(0) ?? "unknown"}. Compare with the previous game before treating it as a recurring problem.</p>
                : <p className="text-muted-foreground">Not enough covered prior games to distinguish a recurring problem from one-game noise.</p>}
            </div>;
          })}
          <p className="text-muted-foreground">Source: stored nflverse play and drive data. Positive offensive EPA is favorable; positive EPA allowed is unfavorable. One game does not establish a trend. Kickoff-return touchdowns and garbage-time splits are not available here; add a sourced report when relevant.</p>
        </section>
        <section className="space-y-2 border-t pt-2" aria-label="Pick decision">
          <h3 className="font-semibold">Why this pick</h3>
          <p>{manual ? "Your manual selection" : objective === "win" ? "Pool simulation selection" : "Maximum expected-points selection"}: {pick} at {pct(p)}, using the displayed forecast.
            {" "}Current weight {confidence}; baseline weight {baselineConfidence}.
            {" "}{objective === "win" ? "Confidence and side reflect the pool simulation before manual changes." : "Baseline confidence sorts games by win probability; stronger favorites receive more points."}</p>
          <p>Switching to {opponent} {p >= 0.5 ? "costs" : "gains"} {Math.abs(confidence * (2 * p - 1)).toFixed(3)} expected points at this weight.</p>
          <p>What changes the side: {opponent} exceeding 50% makes it the expected-points choice. News matters when a refreshed price or an explicit assumption changes that assessment.</p>
          {review.probabilityConflict && <p className="font-semibold text-amber-700 dark:text-amber-400">The latest captured market already disagrees with the forecast’s favored side. Review the market check above before using these expected-point calculations.</p>}
          <label className="flex items-center gap-2"><input type="checkbox" checked={scenario != null}
            onChange={e => onScenario(e.target.checked ? { pHome: game.pHome, reason: "" } : null)} /> Compare an availability scenario</label>
          {scenario && <div className="space-y-2 rounded border p-2">
            <label className="block">Assumption (required to freeze)
              <input aria-label={`Scenario assumption ${game.homeAbbrev} ${game.awayAbbrev}`} maxLength={500}
                placeholder="e.g. starting quarterback ruled out" value={scenario.reason}
                onChange={e => onScenario({ ...scenario, reason: e.target.value })}
                className="mt-1 w-full rounded border bg-background p-2" />
            </label>
            <label className="block">Assumed {game.homeAbbrev} win probability: {pct(scenario.pHome)}
              <input aria-label={`Assumed ${game.homeAbbrev} win probability`} type="range" min={1} max={99} step={0.1}
                value={scenario.pHome * 100} onChange={e => onScenario({ ...scenario, pHome: Number(e.target.value) / 100 })}
                className="mt-1 w-full" />
            </label>
            <p>{pick}: {pct(p)} → {pct(weights.p)}; expected points {(confidence * p).toFixed(3)} → {weights.expectedPoints.toFixed(3)}.
              {" "}Switching {weights.switchCost >= 0 ? "costs" : "gains"} {Math.abs(weights.switchCost).toFixed(3)} points.</p>
            <p>At this assumed probability, the selected side ranks {hypotheticalRank} of {peers.length} by win probability.</p>
            <p className="text-muted-foreground">Your assumption, not a calibrated injury adjustment. The scenario is saved as context; it does not change the card or its probabilities. Use the side selector to change your actual pick.</p>
          </div>}
        </section>
      </div>
    </details>
  );
}

function NewsForm({ game }: { game: PickemSlateGame }) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();
  const [message, setMessage] = useState("");
  return <details className="rounded border p-2">
    <summary className="cursor-pointer font-medium">Add sourced report</summary>
    <form className="mt-2 space-y-2" onSubmit={e => {
      e.preventDefault();
      setMessage("");
      const form = e.currentTarget;
      const data = new FormData(form);
      const value = (key: string) => String(data.get(key) ?? "");
      startTransition(async () => {
        const result = await savePickemNews({ gameId: game.gameId, team: value("team"),
          category: value("category") as PickemNews["category"], status: value("status") as PickemNews["status"],
          headline: value("headline"), detail: value("detail"), source: value("source"), url: value("url"),
          publishedAt: value("publishedAt") ? new Date(value("publishedAt")).toISOString() : null });
        setMessage(result.ok ? result.message : result.error);
        if (result.ok) { form.reset(); router.refresh(); }
      });
    }}>
      <label className="block">Team <select name="team" className="rounded border bg-background p-1">
        <option>{game.homeAbbrev}</option><option>{game.awayAbbrev}</option>
      </select></label>
      <label className="block">Category <select name="category" className="rounded border bg-background p-1">
        {["quarterback", "offensive-line", "playmaker", "defense", "weather", "coaching", "other"].map(c => <option key={c} value={c}>{c.replaceAll("-", " ")}</option>)}
      </select></label>
      <label className="block">Certainty <select name="status" className="rounded border bg-background p-1">
        <option value="reported">Reported</option><option value="confirmed">Confirmed</option><option value="uncertain">Uncertain</option>
      </select></label>
      {[{ name: "headline", label: "Headline", max: 240 }, { name: "source", label: "Source name", max: 120 },
        { name: "url", label: "Supporting HTTPS link", max: 2000 }].map(f => <label key={f.name} className="block">{f.label}
        <input name={f.name} type={f.name === "url" ? "url" : "text"} required maxLength={f.max} className="block w-full rounded border bg-background p-1" />
      </label>)}
      <label className="block">Published (your local time; leave blank if unknown)<input name="publishedAt" type="datetime-local" className="block w-full rounded border bg-background p-1" /></label>
      <label className="block">What happened and why it matters<textarea name="detail" maxLength={2000} className="block w-full rounded border bg-background p-1" /></label>
      <button disabled={pending} className="rounded border px-2 py-1">{pending ? "Saving…" : "Save report"}</button>
      <p role="status">{message}</p>
    </form>
  </details>;
}

export function EvidenceLedger({ ledger, poolId }: { ledger: PickemLedgerRow[]; poolId: number | null }) {
  const relevant = ledger.filter(r => (r.poolId ?? null) === poolId);
  const grade = evidenceGrades(relevant.flatMap(r => r.games));
  return <section className="space-y-3 rounded-lg border bg-card p-4">
    <h2 className="text-sm font-semibold">Evidence audit · selected pool</h2>
    <p className="text-xs text-muted-foreground">Only live frozen cards for this pool. Market comparisons require a captured moneyline for every game when frozen. Older cards without evidence remain unmeasured.</p>
    <div className="grid gap-3 text-sm sm:grid-cols-3">
      <p>Paired graded games: <strong>{grade.n}</strong></p>
      <p>Forecast / market Brier: <strong>{grade.brier?.toFixed(3) ?? "—"} / {grade.marketBrier?.toFixed(3) ?? "—"}</strong><span className="block text-xs text-muted-foreground">Lower is better; identical game sample.</span></p>
      <p>Card / market confidence points: <strong>{grade.n ? `${grade.points} / ${grade.marketPoints}` : "—"}</strong></p>
    </div>
    <div className="overflow-x-auto"><table className="w-full text-left text-xs"><caption className="mb-2 text-left">Do the room-read labels match entered pool pick shares?</caption>
      <thead><tr><th className="p-2">Label</th><th className="p-2">Observed pool-games</th><th className="p-2">Favorite pick share minus forecast</th></tr></thead>
      <tbody>{Object.entries(grade.narrative).map(([label, bucket]) => <tr key={label} className="border-t"><td className="p-2">{label}</td><td className="p-2">{bucket.n}</td><td className="p-2">{bucket.n ? `${signed(bucket.sum / bucket.n)} pp` : "Not measured"}</td></tr>)}</tbody>
    </table></div>
    <p className="text-xs text-muted-foreground">Crowded predicts positive excess backing of the favorite; contrarian predicts negative; quiet predicts little pressure. These are descriptive checks, not proof of an edge. Modeled pick shares are excluded.</p>
    <details><summary className="cursor-pointer text-xs font-medium">Inspect frozen evidence</summary>
      <div className="mt-2 space-y-2">{relevant.length === 0 && <p className="text-xs">No frozen cards for this pool.</p>}
        {relevant.map(r => <details key={r.id} className="rounded border p-2 text-xs"><summary className="cursor-pointer">Week {r.week} · frozen {time(r.frozenAt)}</summary>
          {r.games.map(g => <div key={g.gameId} className="mt-2 border-t pt-2"><strong>{g.awayAbbrev} at {g.homeAbbrev}</strong>
            {!g.evidence ? <p>No evidence snapshot on this older card.</p> : <>
              <p>Quote: {time(g.evidence.latest?.capturedAt ?? null)} · forecast calculated: {time(g.evidence.probabilityComputedAt)} · narrative: {g.evidence.narrative}</p>
              {g.evidence.scenario && <p>Scenario assumption: {g.evidence.scenario.reason} · {g.homeAbbrev} {pct(g.evidence.scenario.pHome)}</p>}
              {g.evidence.news.map(n => <p key={n.id}>{n.team}: {n.headline} · {n.status} · {time(n.publishedAt)} · {n.url && safeSourceUrl(n.url) ? <a href={n.url} className="underline" target="_blank" rel="noreferrer">{n.source}</a> : n.source}</p>)}
              <details><summary className="cursor-pointer">Full frozen snapshot</summary><pre className="max-h-72 overflow-auto whitespace-pre-wrap break-words p-2">{JSON.stringify(g.evidence, null, 2)}</pre></details>
            </>}
          </div>)}
        </details>)}
      </div>
    </details>
  </section>;
}
