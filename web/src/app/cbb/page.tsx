import Link from "next/link";
import type { Metadata } from "next";

export const metadata: Metadata = { title: "College Basketball", description: "College basketball DFS and projection research." };
export default function CollegeBasketballPage() {
  return <main className="mx-auto max-w-6xl space-y-6 p-4 md:p-8">
    <div className="rounded-2xl border bg-slate-950 p-8 text-white">
      <p className="text-sm font-semibold uppercase tracking-widest text-orange-300">College Basketball</p>
      <h1 className="mt-3 text-3xl font-bold">Build your college hoops slate.</h1>
      <p className="mt-3 max-w-2xl text-slate-300">DraftKings men’s basketball, with separate Classic and Showdown rules. Start with the salary file, review player projections, and build lineups.</p>
      <Link href="/cbb/dfs" className="mt-6 inline-block rounded-lg bg-orange-400 px-5 py-3 font-semibold text-slate-950">Open CBB DFS →</Link>
      <Link href="/cbb/props" className="ml-3 mt-6 inline-block rounded-lg border border-slate-600 px-5 py-3 font-semibold text-white">Player Props →</Link>
    </div>
    <div className="grid gap-4 md:grid-cols-3">
      {[
        ["Classic", "Eight players: three guards, three forwards, and two utility slots. $50,000 salary cap."],
        ["Showdown", "One Captain and five utility players from one game. Captain salary and fantasy points count at 1.5×."],
        ["Evidence first", "Imported projections retain their source and timestamp. Missing ownership stays unknown; the history baseline is exploratory."],
      ].map(([title, body]) => <section key={title} className="rounded-xl border bg-card p-5"><h2 className="text-lg font-semibold">{title}</h2><p className="mt-2 text-sm text-muted-foreground">{body}</p></section>)}
    </div>
    <section className="rounded-xl border p-6"><h2 className="text-xl font-semibold">How we’ll project the slate</h2>
      <p className="mt-3 text-sm leading-6 text-muted-foreground">Fantasy points start with expected minutes and production per minute, then need rotation, availability, pace, opponent, and role adjustments. The DFS page supports a transparent box-score baseline and explicit projection imports. Freshmen, transfers, and opening-night roles need separate inputs.</p>
      <p className="mt-3 text-sm leading-6 text-muted-foreground">Ownership is a separate contest-specific model. Historical DraftKings standings must be paired with frozen salaries and pregame projections. Classic selection share, Showdown utility share, and Captain share require separate evaluation. Automated ownership and a live availability feed are not connected yet.</p>
      <a href="https://api.collegebasketballdata.com/api/games" target="_blank" rel="noreferrer" className="mt-4 inline-block text-sm font-medium text-blue-600 underline">Candidate box-score source: CollegeBasketballData</a>
    </section>
  </main>;
}
