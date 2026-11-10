import type { TallyRow, TypeTally } from "@/lib/nfl-dfs/showdown-type-history";

/**
 * Which Showdown lineup types have won so far: each past slate's contest
 * winner, the split of its top 100, and the best lineup the real points
 * allowed. A running count of what happened.
 */
export function ShowdownTypeTally({ tally, error }: { tally: TypeTally | null; error?: string | null }) {
  return <section id="showdown-types" className="rounded-xl border border-violet-200 bg-white p-4 shadow-sm">
    <h2 className="font-bold">Showdown: which lineup types have won</h2>
    <p className="mt-1 max-w-3xl text-xs text-slate-500">
      Every past Showdown slate with an imported contest. <b>Winner</b> is the contest&apos;s first-place lineup (the largest contest whose top lineups are stored);
      <b> Best possible</b> is the best lineup the points DraftKings paid allowed; <b>Top-100 share</b> is the average split of each contest&apos;s top 100.
      &quot;Favored&quot; is the team our projections gave more points; a captain&apos;s role (WR1, WR2, RB1...) is his rank by DraftKings salary among his team&apos;s players at that position, a stand-in for the depth chart. With {tally?.slates.length ?? 0} slates this describes what happened; it is not a forecast.
    </p>
    {error ? <p role="alert" className="mt-2 text-xs font-semibold text-red-700">The tally could not be read: {error}</p> : null}
    {tally && tally.slates.length ? <>
      <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <Card title="Team split" rows={tally.splitKinds} tally={tally} showTop />
        <Card title="Which side had more players" rows={tally.sides} tally={tally} />
        <Card title="Captain position" rows={tally.captainPositions} tally={tally} />
        <Card title="Captain's projection rank" rows={tally.captainRanks} tally={tally} />
        <Card title="Captain role (depth on his team)" rows={tally.captainRoles.filter((r) => r.winners || r.best)} tally={tally} />
        <Card title="Captain's team" rows={tally.captainSides} tally={tally} />
        <Card title="Captain role and team" rows={tally.captainRoleSides} tally={tally} />
        <Card title="Quarterbacks" rows={tally.quarterbacks} tally={tally} />
        <Card title="Kicker" rows={tally.kicker} tally={tally} />
        <Card title="Defense" rows={tally.dst} tally={tally} />
      </div>
      <details className="mt-3">
        <summary className="cursor-pointer text-sm font-semibold text-blue-700">Slate by slate ({tally.slates.length})</summary>
        <div className="mt-2 overflow-auto">
          <table className="w-full text-left text-xs">
            <thead className="text-[10px] uppercase text-slate-500"><tr>
              <th className="p-1.5">Slate</th><th className="p-1.5">Contest</th><th className="p-1.5">Winner</th><th className="p-1.5">Best possible</th><th className="p-1.5">Top-100 3-3 / 4-2 / 5-1</th>
            </tr></thead>
            <tbody>{tally.slates.map((s) => <tr key={s.key} className="border-t align-top">
              <td className="p-1.5 font-semibold">{s.label}{s.favored ? <div className="font-normal text-slate-400">favored {s.favored}</div> : null}</td>
              <td className="p-1.5 text-slate-600">{s.contestName}<div className="text-slate-400">{s.entryCount.toLocaleString()} entries</div></td>
              <td className="p-1.5">{s.winner ? <Type t={s.winner} score={s.winner.score} /> : <span className="text-slate-400">not stored</span>}</td>
              <td className="p-1.5">{s.best ? <Type t={s.best} score={s.best.score} /> : <span className="text-slate-400">no points</span>}</td>
              <td className="p-1.5 tabular-nums text-slate-600">{s.topShares ? `${Math.round(s.topShares["3-3"])} / ${Math.round(s.topShares["4-2"])} / ${Math.round(s.topShares["5-1"])}%` : "—"}{s.topCount ? <span className="text-slate-400"> ({s.topCount})</span> : null}{s.unmatched ? <div className="text-amber-700">{s.unmatched} player slots not matched</div> : null}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </details>
    </> : !error ? <p className="mt-2 text-sm text-slate-500">No Showdown contest has been imported yet.</p> : null}
  </section>;
}

function Type({ t, score }: { t: { side: string; captain: string; captainRole: string; captainSide: string; captainRank: string; quarterbacks: string; kicker: boolean }; score: number | null }) {
  return <div>
    <b>{t.side}</b> · CPT {t.captain} <span className="text-slate-400">{t.captainRole} {t.captainSide.toLowerCase()}, {t.captainRank.toLowerCase()} overall</span>
    <div className="text-slate-500">{t.quarterbacks}{t.kicker ? " · kicker" : ""}{score != null ? ` · ${score.toFixed(1)} pts` : ""}</div>
  </div>;
}

function Card({ title, rows, tally, showTop }: { title: string; rows: TallyRow[]; tally: TypeTally; showTop?: boolean }) {
  const max = Math.max(1, ...rows.map((r) => Math.max(r.winners, r.best)));
  return <div className="rounded-lg border p-2">
    <h3 className="text-xs font-bold text-slate-700">{title}</h3>
    <table className="mt-1 w-full text-[11px]">
      <thead className="text-slate-400"><tr><th className="text-left font-normal" /><th className="text-right font-normal">Winner</th><th className="text-right font-normal">Best possible</th>{showTop ? <th className="text-right font-normal">Top-100 share</th> : null}</tr></thead>
      <tbody>{rows.map((r) => <tr key={r.key} className="border-t">
        <td className="py-1 pr-1 font-semibold text-slate-800">{r.key}
          <div className="mt-0.5 flex h-1 max-w-[120px] gap-px"><div className="rounded bg-violet-600" style={{ width: `${(100 * r.winners) / max}%` }} /></div></td>
        <td className="text-right tabular-nums">{r.winners}<span className="text-slate-400">/{tally.withWinner}</span></td>
        <td className="text-right tabular-nums">{r.best}<span className="text-slate-400">/{tally.withBest}</span></td>
        {showTop ? <td className="text-right tabular-nums text-slate-600">{r.topShare == null ? "—" : `${Math.round(r.topShare)}%`}</td> : null}
      </tr>)}</tbody>
    </table>
  </div>;
}
