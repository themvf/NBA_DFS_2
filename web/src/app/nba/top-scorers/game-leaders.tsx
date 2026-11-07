import { fairAmerican, type GameLeaders, type LeaderChance, type TeamRecord } from "@/lib/nba-game-leaders";

const DARK_BORDER = { "--border": "#1e293b" } as React.CSSProperties;
const SHOW = 5;

const tip = (s: string) => new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }).format(new Date(s));
const pct = (v: number) => (v < 0.005 ? "<1%" : `${(v * 100).toFixed(0)}%`);

const count = (v: number) => (Number.isInteger(v) ? String(v) : v.toFixed(1));
const surname = (name: string) => name.split(" ").filter((w) => !/^(jr|sr|ii|iii|iv)\.?$/i.test(w)).at(-1) ?? name;

/** Each team's biggest line: his chance to lead the team tonight, and how that has gone this season. */
function TeamRows({ players, records, teamLabels }: { players: LeaderChance[]; records: Record<string, TeamRecord>; teamLabels: Record<string, string> }) {
  const tops = new Map<string, LeaderChance>();
  for (const p of players) {
    if (p.team == null || p.teamChance == null) continue;
    const cur = tops.get(p.team);
    if (!cur || p.line > cur.line) tops.set(p.team, p);
  }
  if (tops.size === 0) return null;
  return (
    <div className="mt-2 space-y-1 border-t border-slate-800/70 pt-2 text-[12px]">
      <div className="font-mono text-[10px] uppercase text-slate-500">Leads his team</div>
      {[...tops.entries()].map(([team, p]) => {
        const rec = records[team];
        const mine = rec?.players[p.key];
        return (
          <div key={team} className="flex flex-wrap items-baseline justify-between gap-x-3">
            <span>
              <span className="font-mono text-slate-400">{teamLabels[team] ?? team}</span>{" "}
              <span className="text-slate-200">{p.player}</span>{" "}
              <span className="font-mono text-slate-300">{pct(p.teamChance!)}</span>
            </span>
            <span
              className="font-mono text-[11px] text-slate-500"
              title="This season, games where he (or anyone) held the team's biggest line, and how many times that player led the team. Ties split."
            >
              {rec
                ? `season: ${mine ? `${surname(p.player)} ${count(mine.led)}/${mine.games} · ` : ""}team ${count(rec.led)}/${rec.games}`
                : "no graded games yet"}
            </span>
          </div>
        );
      })}
    </div>
  );
}

/** Chance each quoted player scores the most in his own game. Server-rendered. */
export function GameLeadersPanel({ games, now, records, teamLabels }: {
  games: GameLeaders[]; now: number; records: Record<string, TeamRecord>; teamLabels: Record<string, string>;
}) {
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
              <TeamRows players={g.players} records={records} teamLabels={teamLabels} />
            </div>
          );
        })}
      </div>
      <p className="text-xs text-slate-500">
        Checked on 123 games (Oct 20 - Nov 5): the biggest line led his game 43% of the time, and the leader was one of the top three
        lines 80% of the time. The simulation said 39% for the biggest line, so its favourite chances run a little low and its
        longshot chances a little high (the books&apos; alternate lines overstate each player&apos;s top end; this version corrects most
        of that, nba-points-sim-recal-v1). Not yet graded night by night. &quot;Leads his team&quot; is the same simulation against his
        quoted teammates only; the record is this season&apos;s graded games (the team&apos;s biggest line led his team 54% of 246
        team-games; at about eight games a team, no team&apos;s record differs from the rest by more than chance).
      </p>
    </section>
  );
}
