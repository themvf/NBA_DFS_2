import { fairAmerican, type GameLeaders } from "@/lib/nba-game-leaders";

const DARK_BORDER = { "--border": "#1e293b" } as React.CSSProperties;
const SHOW = 5;

const tip = (s: string) => new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }).format(new Date(s));
const pct = (v: number) => (v < 0.005 ? "<1%" : `${(v * 100).toFixed(0)}%`);

/** Chance each quoted player scores the most in his own game. Server-rendered. */
export function GameLeadersPanel({ games, now }: { games: GameLeaders[]; now: number }) {
  const shown = games.filter((g) => g.players.length > 0);
  if (shown.length === 0) return null;
  return (
    <section style={DARK_BORDER} className="space-y-3 rounded-xl border border-slate-800 bg-[#0f1726] px-5 py-4 text-slate-200" aria-label="Game leaders">
      <div>
        <h2 className="text-lg font-bold text-white">Game leaders · most points in each game</h2>
        <p className="mt-1 text-sm text-slate-400">
          Chance each player outscores everyone else in his game, simulated from the books&apos; points lines and alternate lines.
          Fair price is where a bet would break even with no book margin; sportsbook &quot;Most Points&quot; markets carry a 13-20% margin,
          heaviest on the longshots.
        </p>
      </div>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {shown.map((g) => {
          const started = new Date(g.commenceTime).getTime() <= now;
          const rest = g.players.slice(SHOW).reduce((a, p) => a + p.chance, 0);
          return (
            <div key={g.eventId} className="rounded-lg border border-slate-800 bg-[#0b1220] p-3">
              <div className="flex items-baseline justify-between gap-2">
                <span className="font-semibold text-white">{g.game}</span>
                <span className="font-mono text-xs text-slate-400">{started ? "started" : `${tip(g.commenceTime)} ET`}</span>
              </div>
              <table className="mt-2 w-full text-sm">
                <thead>
                  <tr className="text-left font-mono text-[10px] uppercase text-slate-500">
                    <th className="font-normal">Player</th>
                    <th className="text-right font-normal">Line</th>
                    <th className="text-right font-normal">Leads</th>
                    <th className="text-right font-normal">Fair price</th>
                  </tr>
                </thead>
                <tbody>
                  {g.players.slice(0, SHOW).map((p, i) => (
                    <tr key={p.key} className="border-t border-slate-800/70">
                      <td className="py-1 pr-2">
                        <span className={i === 0 ? "font-semibold text-white" : ""}>{p.player}</span>
                        {p.books === 1 && (
                          <span className="ml-1 rounded border border-orange-400 px-1 font-mono text-[10px] text-orange-300" title="Only one book quotes this line; such players sat about 35% of the time.">1 BOOK</span>
                        )}
                      </td>
                      <td className="text-right font-mono text-slate-400">{p.line.toFixed(1)}</td>
                      <td className="text-right font-mono">
                        <span className="inline-block h-1.5 align-middle rounded bg-sky-500/70" style={{ width: `${Math.round(p.chance * 40)}px` }} />{" "}
                        {pct(p.chance)}
                      </td>
                      <td className="text-right font-mono text-slate-300">{fairAmerican(p.chance)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {g.players.length > SHOW && (
                <div className="mt-1 font-mono text-[11px] text-slate-500">{g.players.length - SHOW} more quoted players: {pct(rest)} combined</div>
              )}
            </div>
          );
        })}
      </div>
      <p className="text-xs text-slate-500">
        Checked on 123 games (Oct 20 - Nov 5): the biggest line led his game 43% of the time, and the leader was one of the top three
        lines 80% of the time. The simulation said 39% for the biggest line, so its favourite chances run a little low and its
        longshot chances a little high (the books&apos; alternate lines overstate each player&apos;s top end; this version corrects most
        of that, nba-points-sim-recal-v1). Not yet graded night by night.
      </p>
    </section>
  );
}
