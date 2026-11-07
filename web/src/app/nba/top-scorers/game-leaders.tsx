import { boomIdentity } from "@/lib/nba-boom-display";
import { teamKey } from "@/lib/nba-top-scorers";
import { BIG_SPREAD_AT, fairAmerican, gameTag, spreadLabel, teamTag, type GameLeaders, type GameLine, type LeaderChance, type TeamRecord } from "@/lib/nba-game-leaders";

const DARK_BORDER = { "--border": "#1e293b" } as React.CSSProperties;
const SHOW = 5;

const tip = (s: string) => new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" }).format(new Date(s));
const pct = (v: number) => (v < 0.005 ? "<1%" : `${(v * 100).toFixed(0)}%`);

const count = (v: number) => (Number.isInteger(v) ? String(v) : v.toFixed(1));
const surname = (name: string) => name.split(" ").filter((w) => !/^(jr|sr|ii|iii|iv)\.?$/i.test(w)).at(-1) ?? name;

const TAG = "rounded border px-1 font-mono text-[10px]";

/** Official injury-report status, when the latest observed report lists him (see the evidence times above). */
function Status({ status }: { status: string | undefined }) {
  if (!status || status === "unverified" || status === "Available") return null;
  const tone = status === "Out" || status === "Doubtful" ? "border-rose-400 text-rose-300" : "border-amber-400 text-amber-200";
  return <span className={`ml-1 ${TAG} ${tone}`} title="Latest observed official injury report.">{status.toUpperCase()}</span>;
}

/** Each team's biggest line: his chance to lead the team tonight, and how that has gone this season. */
function TeamRows({ players, records, teamLabels, status, eventId }: {
  players: LeaderChance[]; records: Record<string, TeamRecord>; teamLabels: Record<string, string>;
  status: Record<string, string>; eventId: string;
}) {
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
        const tag = teamTag(p, players);
        return (
          <div key={team} className="flex flex-wrap items-baseline justify-between gap-x-3">
            <span>
              <span className="font-mono text-slate-400">{teamLabels[team] ?? team}</span>{" "}
              <span className="text-slate-200">{p.player}</span>
              <Status status={status[boomIdentity(eventId, p.player)]} />{" "}
              <span className="font-mono text-slate-300">{pct(p.teamChance!)}</span>
              {tag?.kind === "clear" && (
                <span className={`ml-1 ${TAG} border-emerald-500 text-emerald-300`} title="65%+ to lead his team. Such players led 65% of 46 team-games (Oct 20 - Nov 6).">CLEAR</span>
              )}
              {tag?.kind === "toss_up" && (
                <span className={`ml-1 ${TAG} border-violet-400 text-violet-300`} title="Under 45% to lead his team. Such players led 47% of 81 team-games; he or the named teammate led 75% (Oct 20 - Nov 6).">
                  TOSS-UP{tag.rival ? ` w/ ${surname(tag.rival.player).toUpperCase()} ${pct(tag.rival.teamChance!)}` : ""}
                </span>
              )}
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
export function GameLeadersPanel({ games, now, records, teamLabels, status = {}, lines = {} }: {
  games: GameLeaders[]; now: number; records: Record<string, TeamRecord>; teamLabels: Record<string, string>;
  /** Spread and total by Odds API event id (newest pre-tip capture). */
  lines?: Record<string, GameLine>;
  /** Official injury-report status keyed by boomIdentity(eventId, player). */
  status?: Record<string, string>;
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
          const tag = gameTag(g.players);
          const top3 = g.players.slice(0, 3).reduce((a, p) => a + p.chance, 0);
          const line = lines[g.eventId];
          const spread = line ? spreadLabel(line, (t) => teamLabels[teamKey(t)] ?? t) : null;
          const big = line?.homeSpread != null && Math.abs(line.homeSpread) >= BIG_SPREAD_AT;
          return (
            <div key={g.eventId} className="rounded-lg border border-slate-800 bg-[#0b1220] p-3">
              <div className="flex items-baseline justify-between gap-2">
                <span className="font-semibold text-white">
                  {g.game}
                  {tag === "wide_open" && (
                    <span className={`ml-2 align-middle ${TAG} border-violet-400 text-violet-300`} title={`Nobody is 30% to lead. Such favourites led 26% of 29 games; the top three led 76% (Oct 20 - Nov 6). Top three tonight: ${pct(top3)}.`}>WIDE OPEN</span>
                  )}
                  {tag === "clear_favourite" && (
                    <span className={`ml-2 align-middle ${TAG} border-emerald-500 text-emerald-300`} title="One player 50%+ to lead. Such favourites led 57% of 27 games (Oct 20 - Nov 6).">CLEAR FAVOURITE</span>
                  )}
                </span>
                <span className="font-mono text-xs text-slate-400">{started ? "started" : `${tip(g.commenceTime)} ET`}</span>
              </div>
              {line && (
                <div className="mt-0.5 font-mono text-[11px] text-slate-400" title={`Median of ${line.books} books, captured ${new Date(line.capturedAt).toUTCString()}.`}>
                  {spread ?? "no spread"}{line.total != null ? ` · total ${line.total}` : ""}
                  {big && (
                    <span className={`ml-2 ${TAG} border-amber-400 text-amber-200`} title="Favourite by 10+. Not measured yet: in 20+ point games the favourite played about 5 fewer minutes and led 37% (Oct 20 - Nov 6); spreads are stored from Nov 7 to check whether a big spread flags those games.">BIG SPREAD</span>
                  )}
                </div>
              )}
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
                        <Status status={status[boomIdentity(g.eventId, p.player)]} />
                        {p.books === 1 && (
                          <span className={`ml-1 ${TAG} border-orange-400 text-orange-300`} title="Only one book quotes this line; such players sat about 35% of the time.">1 BOOK</span>
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
              <TeamRows players={g.players} records={records} teamLabels={teamLabels} status={status} eventId={g.eventId} />
            </div>
          );
        })}
      </div>
      <div className="space-y-1 text-xs text-slate-500">
        <p>
          <span className="text-violet-300">WIDE OPEN</span>: nobody 30% to lead; spread across the top three (they led 76% of 29 such games).{" "}
          <span className="text-emerald-300">CLEAR FAVOURITE</span>: one player 50%+ (led 57% of 27).{" "}
          <span className="text-violet-300">TOSS-UP</span>: team&apos;s biggest line under 45% to lead his team; he or the named teammate led 75% of 81.{" "}
          <span className="text-emerald-300">CLEAR</span>: 65%+ (led 65% of 46).{" "}
          <span className="text-amber-200">QUESTIONABLE</span>/<span className="text-rose-300">OUT</span>: latest observed official injury report.{" "}
          <span className="text-orange-300">1 BOOK</span>: only one book quotes him.{" "}
          <span className="text-amber-200">BIG SPREAD</span>: favourite by 10+ (not measured yet; spreads stored from Nov 7).
          The tags restate the chances in bands that held up on Oct 20 - Nov 6; they add no information beyond them.
        </p>
        <p>
          Checked on 132 games (Oct 20 - Nov 6): the biggest line led his game 42% of the time (the simulation said 39%), and the leader was one
          of the top three lines 78% of the time. The simulation still runs a little low on the favourite and high on the longshots: players outside the top
          five lines were given 16 wins and won 11 (nba-points-sim-recal-v1 corrects most of the alternate-line overstatement). When the
          favourite missed, it was usually his own short night (under his line in 73% of misses), or a 20+ point blowout cutting his
          minutes; recent form, rising shots or minutes, and missing teammates did not pick the winner better than the lines. Not yet graded
          night by night. A team&apos;s season record is history only: alternate games of the same team did not predict each other.
        </p>
      </div>
    </section>
  );
}
