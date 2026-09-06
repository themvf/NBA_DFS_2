/** Synthetic mechanics fixture ONLY. Rates here are arbitrary, not fitted NFL projections. */
import type { NflDkPlayer, NflDkSlate, NflPosition, NflSlateFormat } from "./dk-salary-csv";
import { nflRandom } from "./random";
import { emptyNflStats, type NflScenarioBank } from "./scenarios";
import type { NflStatLine } from "./scoring";

const ROLES: Array<[NflPosition, number]> = [
  ["QB", 6500], ["RB", 6000], ["RB", 4300], ["WR", 5900], ["WR", 4700], ["WR", 3400], ["TE", 3800], ["DST", 2700], ["K", 3200],
];
export function nflDemoSlate(format: NflSlateFormat = "classic"): NflDkSlate {
  const games = format === "classic" ? ["AAA@BBB", "CCC@DDD", "EEE@FFF"] : ["AAA@BBB"];
  const players: NflDkPlayer[] = [];
  for (const game of games) {
    const teams = game.split("@");
    teams.forEach((team, teamIndex) => {
      ROLES.forEach(([position, salary], roleIndex) => {
        if (format === "classic" && position === "K") return;
        const id = 1000 + players.length;
        players.push({ dkPlayerId: id, name: `SYNTHETIC ${team} ${position} ${roleIndex}`, position,
          rosterPositions: format === "showdown" ? ["FLEX"] : [position, ...(["RB", "WR", "TE"].includes(position) ? ["FLEX"] : [])],
          teamAbbrev: team, opponent: teams[1 - teamIndex], homeAway: teamIndex === 0 ? "away" : "home",
          gameKey: game, gameInfo: game, salary, avgFptsDk: null, status: null, isOut: false,
          captain: format === "showdown" ? { dkPlayerId: id + 100_000, salary: salary * 1.5 } : null,
        });
      });
    });
  }
  return { format, players, games, teams: games.flatMap((g) => g.split("@")), warnings: ["Synthetic mechanics fixture — not historical NFL data"] };
}

export function nflDemoBank(slate: NflDkSlate, seed: number, count: number, streamId: string): NflScenarioBank {
  if (!Number.isSafeInteger(count) || count < 2) throw new Error("Demo requires at least two draws");
  const random = nflRandom(seed);
  const integer = (low: number, high: number) => low + Math.floor(random() * (high - low + 1));
  const scenarios: NflScenarioBank["scenarios"] = [];
  for (let s = 0; s < count; s++) {
    const stats: Record<string, NflStatLine> = Object.fromEntries(slate.players.map((p) => [p.dkPlayerId, emptyNflStats(p.position)]));
    const add = (player: NflDkPlayer | undefined, key: keyof NflStatLine, amount: number) => {
      if (player) stats[player.dkPlayerId][key] = (stats[player.dkPlayerId][key] ?? 0) + amount;
    };
    for (const game of slate.games) {
      const teams = game.split("@");
      const roster = teams.map((team) => slate.players.filter((p) => p.teamAbbrev === team));
      const defenses = roster.map((players) => players.find((p) => p.position === "DST")!);
      const kickers = roster.map((players) => players.find((p) => p.position === "K"));
      const pace = [.65, 1, 1.35][integer(0, 2)]; // Shared game environment, deliberately illustrative.
      const pointAfter = (team: number) => {
        if (random() < .95) { add(kickers[team], "extraPointsMade", 1); add(defenses[1 - team], "pointsAllowed", 1); }
      };
      roster.forEach((players, team) => {
        const qb = players.find((p) => p.position === "QB")!;
        const receivers = players.filter((p) => ["WR", "TE", "RB"].includes(p.position));
        const runners = players.filter((p) => p.position === "RB");
        const completions = Math.floor(integer(14, 29) * pace);
        for (let n = 0; n < completions; n++) {
          const receiver = receivers[integer(0, receivers.length - 1)];
          const yards = integer(-3, 28);
          add(qb, "passYds", yards); add(receiver, "recYds", yards); add(receiver, "receptions", 1);
          if (yards > 0 && random() < .075) {
            add(qb, "passTds", 1); add(receiver, "recTds", 1);
            add(defenses[1 - team], "pointsAllowed", 6); pointAfter(team);
          }
        }
        const carries = integer(15, 28);
        for (let n = 0; n < carries; n++) {
          const runner = runners[integer(0, runners.length - 1)];
          const yards = integer(-3, 13);
          add(runner, "rushYds", yards);
          if (yards > 0 && random() < .035) {
            add(runner, "rushTds", 1); add(defenses[1 - team], "pointsAllowed", 6); pointAfter(team);
          }
        }
        const interceptions = integer(0, 2);
        add(qb, "interceptions", interceptions); add(defenses[1 - team], "dstInterceptions", interceptions);
        for (let n = 0; n < interceptions; n++) {
          if (random() < .12) {
            add(defenses[1 - team], "dstTds", 1);
            // Pick-six is NOT charged to the throwing team's DST, but the ensuing XP is.
            pointAfter(1 - team);
          }
        }
        add(defenses[1 - team], "sacks", integer(0, 4));
        const fieldGoals = integer(0, 3);
        for (let n = 0; n < fieldGoals; n++) {
          const bucket = integer(0, 2);
          const key = (["fgMade0to39", "fgMade40to49", "fgMade50Plus"] as const)[bucket];
          add(kickers[team], key, 1); add(defenses[1 - team], "pointsAllowed", 3);
        }
      });
    }
    scenarios.push({ id: `${streamId}:${s}`, weight: 1, stats });
  }
  return { schemaVersion: 1, runId: `synthetic-${streamId}-${seed}`, modelVersion: "synthetic-events-v1-unvalidated",
    snapshotId: `synthetic-${slate.format}-v1`, decisionAt: "2026-09-05T12:00:00Z", inputsCapturedAt: "2026-09-05T11:00:00Z",
    source: "synthetic", sampling: "iid", seed, streamId, scenarios };
}
