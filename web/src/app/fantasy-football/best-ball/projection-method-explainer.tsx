const LIVE_STEPS = [
  { title: "Recent production", detail: "PPR points per game weighted 75% from 2025, 20% from 2024, and 5% from 2023. Missing seasons are excluded and the available weights are normalized." },
  { title: "Sample-aware regression", detail: "The player’s full historical game sample is blended with four equivalent position-prior games." },
  { title: "17 active games", detail: "The regressed points-per-game rate is extended across a full 17-game baseline." },
  { title: "Current role", detail: "A bounded depth-chart role factor adjusts opportunity. Availability is estimated separately." },
] as const;

const CHALLENGER_STEPS = [
  { title: "Team opportunity", detail: "Forecast team carries, running-back targets, and touchdowns with uncertainty." },
  { title: "Player allocation", detail: "Allocate early-down, receiving, and goal-line work across every relevant teammate." },
  { title: "Efficiency and TD share", detail: "Estimate yards and touchdowns from verified context without an automatic workload penalty." },
  { title: "Weekly outcomes", detail: "Generate weekly downside, median, and upside ranges with role and injury scenarios." },
  { title: "Best Ball impact", detail: "Measure spike games, counted weeks, bye coverage, and points added to this specific roster." },
] as const;

function StepFlow({ steps, tone }: { steps: readonly { title: string; detail: string }[]; tone: "live" | "challenger" }) {
  const live = tone === "live";
  return <ol className="mt-4 grid gap-2" aria-label={live ? "Current projection calculation" : "V2 challenger calculation"}>
    {steps.map((step, index) => <li key={step.title} className={`grid grid-cols-[2rem_1fr] gap-3 rounded-xl border p-3 ${live ? "border-blue-200 bg-white" : "border-amber-200 bg-white/80"}`}>
      <span className={`flex h-8 w-8 items-center justify-center rounded-full text-sm font-black ${live ? "bg-blue-700 text-white" : "bg-amber-200 text-amber-950"}`}>{index + 1}</span>
      <div><p className="font-black">{step.title}</p><p className="mt-0.5 text-sm text-muted-foreground">{step.detail}</p></div>
    </li>)}
  </ol>;
}

export default function ProjectionMethodExplainer() {
  return <section aria-labelledby="projection-method-heading" className="overflow-hidden rounded-2xl border bg-card shadow-sm">
    <div className="border-b bg-slate-950 px-5 py-4 text-white">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div><p className="text-xs font-bold uppercase tracking-[0.2em] text-blue-300">Projection method</p><h2 id="projection-method-heading" className="mt-1 text-2xl font-black">How to read our projected points</h2></div>
        <span className="rounded-full bg-blue-500 px-3 py-1 text-xs font-black">V1.6 IS LIVE</span>
      </div>
      <p className="mt-2 max-w-4xl text-sm text-slate-300">The number under <b className="text-white">Our 2026 PPR Base (V1.6)</b> is a 17-active-game season baseline. It is not yet produced by the V2 team-opportunity model.</p>
    </div>

    <div className="grid gap-4 p-5 xl:grid-cols-2">
      <article className="rounded-2xl border border-blue-300 bg-blue-50 p-4">
        <div className="flex flex-wrap items-center justify-between gap-2"><h3 className="text-lg font-black text-blue-950">V1.6 baseline</h3><span className="rounded-full bg-emerald-100 px-2.5 py-1 text-xs font-black text-emerald-800">LIVE NOW</span></div>
        <p className="mt-1 text-sm text-blue-950">This is the projection currently displayed and used to order the available-player board.</p>
        <StepFlow steps={LIVE_STEPS} tone="live" />
        <div className="mt-3 rounded-xl bg-blue-950 p-3 text-sm font-semibold text-white">Weighted PPG → regression → 17 active games → role adjustment</div>
      </article>

      <article className="rounded-2xl border border-dashed border-amber-400 bg-amber-50 p-4">
        <div className="flex flex-wrap items-center justify-between gap-2"><h3 className="text-lg font-black text-amber-950">V2 roster-aware challenger</h3><span className="rounded-full bg-amber-200 px-2.5 py-1 text-xs font-black text-amber-950">NOT ACTIVE</span></div>
        <p className="mt-1 text-sm text-amber-950">This approved architecture remains in source, history, and validation development. It does not generate today’s displayed points.</p>
        <StepFlow steps={CHALLENGER_STEPS} tone="challenger" />
        <div className="mt-3 rounded-xl bg-amber-950 p-3 text-sm font-semibold text-white">Team ceiling → role shares → weekly range → counted roster value</div>
      </article>
    </div>

    <div className="grid gap-3 border-t bg-muted/40 p-5 md:grid-cols-3">
      <div className="rounded-xl border bg-card p-3"><p className="text-xs font-black uppercase tracking-wide text-blue-700">Our PPR base</p><p className="mt-1 text-sm">Our live independent V1.6 projection. Open <b>How V1.6 projects</b> on any player for the actual inputs and arithmetic. RB/WR/TE rookies use a draft-pick value curve fit on 2023-2025 rookie outcomes, adjusted by current depth-chart role; QB/K/DST rookies use a position-and-draft-capital prior (a curve showed no improvement there).</p></div>
      <div className="rounded-xl border bg-card p-3"><p className="text-xs font-black uppercase tracking-wide text-violet-700">FantasyPros projection</p><p className="mt-1 text-sm">A separately sourced comparison. It is never blended into our projection.</p></div>
      <div className="rounded-xl border bg-card p-3"><p className="text-xs font-black uppercase tracking-wide text-emerald-700">ADP</p><p className="mt-1 text-sm">The market’s draft cost and timing signal. It does not manufacture player performance.</p></div>
    </div>
  </section>;
}
