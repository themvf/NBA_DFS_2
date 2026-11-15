/**
 * The slate read: what the lines and the field's estimated ownership say about
 * a slate BEFORE any lineup is built (owner's request 2026-11-15). Descriptive:
 * it changes no projection and no lineup. The reference returns come from the
 * field's results on eight 2026 Sunday main slates (Classic) and 292 Showdown
 * games 2024-2026; they are leads from small samples, not rules.
 */
export const SLATE_READ_VERSION = "nfl-slate-read-v1";

export type GameLine = { home: string; away: string; total: number | null; homeSpread: number | null; capturedAt: string | null };

export type SlateReadPlayer = {
  name: string; position: string; team: string; gameKey: string | null; salary: number;
  ourProj: number | null; ownPct?: number | null; captainOwnPct?: number | null; isOut?: boolean;
};

const FRANCHISE: Record<string, string> = { WSH: "WAS", LA: "LAR", JAC: "JAX", AZ: "ARI" };
export const franchise = (team: string) => FRANCHISE[team.toUpperCase()] ?? team.toUpperCase();
const gameId = (away: string, home: string) => `${franchise(away)}@${franchise(home)}`;

export type TotalBand = "under 43" | "43-47.5" | "48+";
export const totalBand = (total: number): TotalBand => (total < 43 ? "under 43" : total < 48 ? "43-47.5" : "48+");

/** Field results per $1 entered, by the quarterback's game total (eight Sunday main slates, weeks 2-9 of 2026, Millionaire). */
export const CLASSIC_TOTAL_REFERENCE: Record<TotalBand, { fieldQbPct: number | null; returnPerDollar: number }> = {
  "under 43": { fieldQbPct: null, returnPerDollar: 0.52 },
  "43-47.5": { fieldQbPct: null, returnPerDollar: 1.22 },
  "48+": { fieldQbPct: 44, returnPerDollar: 1.04 },
};
/** Same slates: stack size and bring-back. */
export const CLASSIC_STACK_REFERENCE = { twoReceivers: { fieldPct: 31, returnPerDollar: 1.43 }, oneReceiver: { fieldPct: 51, returnPerDollar: 0.81 }, oneReceiverNoBringBack: { fieldPct: 26, returnPerDollar: 0.73 } };
/** 292 Showdown games, 2024-2026: the one split that lost in all three seasons. */
export const SHOWDOWN_SPLIT_REFERENCE_READ = { fiveOneUnderdog: 0.77 };

export type GameRead = {
  key: string; away: string; home: string;
  total: number | null; homeSpread: number | null; favorite: string | null; band: TotalBand | null;
  /** Share of the field's roster spots estimated to come from this game, 0..100; null without ownership estimates. */
  fieldSharePct: number | null;
  /** Share of the field's quarterbacks estimated to come from this game, 0..100. */
  fieldQbPct: number | null;
  /** The most-owned players of the game. */
  chalk: { name: string; position: string; ownPct: number }[];
};

export type ClassicSlateRead = { games: GameRead[]; evenSharePct: number; statements: string[]; linesAsOf: string | null; ownershipKnown: boolean };

const pct = (v: number) => `${Math.round(v)}%`;
const list = (items: string[]) => (items.length <= 1 ? items.join("") : `${items.slice(0, -1).join(", ")} and ${items[items.length - 1]}`);

function lineFor(key: string, lines: readonly GameLine[]): GameLine | null {
  const [away, home] = key.split("@");
  if (!away || !home) return null;
  return lines.find((l) => gameId(l.away, l.home) === gameId(away, home)) ?? null;
}

export function buildClassicSlateRead(players: readonly SlateReadPlayer[], lines: readonly GameLine[]): ClassicSlateRead {
  const active = players.filter((p) => !p.isOut && p.gameKey);
  const keys = [...new Set(active.map((p) => p.gameKey!))];
  const own = (p: SlateReadPlayer) => (p.ownPct != null && Number.isFinite(p.ownPct) ? Math.max(0, p.ownPct) : 0);
  const totalOwn = active.reduce((s, p) => s + own(p), 0);
  const qbOwn = active.filter((p) => p.position === "QB").reduce((s, p) => s + own(p), 0);
  const ownershipKnown = totalOwn > 0;
  const games: GameRead[] = keys.map((key) => {
    const [away, home] = key.split("@");
    const line = lineFor(key, lines);
    const mine = active.filter((p) => p.gameKey === key);
    const total = line?.total ?? null;
    const spread = line?.homeSpread ?? null;
    return {
      key, away, home, total, homeSpread: spread,
      favorite: spread == null || spread === 0 ? null : spread < 0 ? home : away,
      band: total == null ? null : totalBand(total),
      fieldSharePct: ownershipKnown ? (100 * mine.reduce((s, p) => s + own(p), 0)) / totalOwn : null,
      fieldQbPct: qbOwn > 0 ? (100 * mine.filter((p) => p.position === "QB").reduce((s, p) => s + own(p), 0)) / qbOwn : null,
      chalk: mine.filter((p) => own(p) > 0).sort((a, b) => own(b) - own(a)).slice(0, 3).map((p) => ({ name: p.name, position: p.position, ownPct: own(p) })),
    };
  }).sort((a, b) => (b.total ?? -1) - (a.total ?? -1) || a.key.localeCompare(b.key));

  const evenSharePct = keys.length ? 100 / keys.length : 0;
  const statements: string[] = [];
  const withTotal = games.filter((g) => g.total != null);
  if (withTotal.length) {
    const top = withTotal[0];
    const field = top.fieldQbPct != null && top.fieldSharePct != null
      ? ` We estimate ${pct(top.fieldQbPct)} of the field's quarterbacks and ${pct(top.fieldSharePct)} of all its roster spots come from this game; an even split over ${keys.length} games would be ${pct(evenSharePct)}.`
      : " No ownership estimate is loaded, so the field's share of this game is not known.";
    statements.push(`Highest total: ${top.away} at ${top.home}, ${top.total}.${field}`);
    const high = withTotal.filter((g) => g.band === "48+");
    const low = withTotal.filter((g) => g.band === "under 43");
    if (high.length) {
      const share = high.every((g) => g.fieldQbPct != null) ? ` Estimated field quarterbacks in ${high.length === 1 ? "it" : "them"} today: ${pct(high.reduce((s, g) => s + g.fieldQbPct!, 0))}.` : "";
      statements.push(`Totals of 48 or more: ${list(high.map((g) => `${g.away}@${g.home}`))}.${share} On eight past Sunday slates the field put ${CLASSIC_TOTAL_REFERENCE["48+"].fieldQbPct}% of its quarterbacks in these games and got ${CLASSIC_TOTAL_REFERENCE["48+"].returnPerDollar.toFixed(2)} per $1; quarterbacks in games totaled 43 to 47.5 returned ${CLASSIC_TOTAL_REFERENCE["43-47.5"].returnPerDollar.toFixed(2)}.`);
    }
    if (low.length) {
      const share = low.every((g) => g.fieldQbPct != null) ? ` Estimated field quarterbacks there today: ${pct(low.reduce((s, g) => s + g.fieldQbPct!, 0))}.` : "";
      statements.push(`Totals under 43: ${list(low.map((g) => `${g.away}@${g.home}`))}. Quarterbacks from games this low returned ${CLASSIC_TOTAL_REFERENCE["under 43"].returnPerDollar.toFixed(2)} per $1 on those slates.${share}`);
    }
  } else if (keys.length) statements.push("No game totals were found for this slate, so the games are listed without lines.");
  if (ownershipKnown && games.length) {
    const crowded = [...games].filter((g) => g.fieldSharePct != null).sort((a, b) => b.fieldSharePct! - a.fieldSharePct!)[0];
    if (crowded && crowded.fieldSharePct! >= evenSharePct * 1.4 && crowded.key !== withTotal[0]?.key) {
      statements.push(`Most crowded game by estimated ownership: ${crowded.away}@${crowded.home}, ${pct(crowded.fieldSharePct!)} of the field's roster spots.`);
    }
  }
  statements.push(`Stacks on those slates: the field stacked two receivers with its quarterback in ${CLASSIC_STACK_REFERENCE.twoReceivers.fieldPct}% of lineups (${CLASSIC_STACK_REFERENCE.twoReceivers.returnPerDollar.toFixed(2)} per $1) and one receiver in ${CLASSIC_STACK_REFERENCE.oneReceiver.fieldPct}% (${CLASSIC_STACK_REFERENCE.oneReceiver.returnPerDollar.toFixed(2)}); one receiver with nobody from the other team returned ${CLASSIC_STACK_REFERENCE.oneReceiverNoBringBack.returnPerDollar.toFixed(2)}.`);
  const linesAsOf = lines.map((l) => l.capturedAt).filter((v): v is string => Boolean(v)).sort().at(-1) ?? null;
  return { games, evenSharePct, statements, linesAsOf, ownershipKnown };
}

export type ShowdownSlateRead = {
  game: GameRead | null;
  captains: { name: string; position: string; team: string; captainPct: number }[];
  /** Estimated share of the field's roster spots on each team. */
  teamSharePct: { team: string; pct: number }[];
  statements: string[];
};

export function buildShowdownSlateRead(players: readonly SlateReadPlayer[], lines: readonly GameLine[]): ShowdownSlateRead {
  const classic = buildClassicSlateRead(players, lines);
  const game = classic.games[0] ?? null;
  const active = players.filter((p) => !p.isOut);
  const captains = active.filter((p) => (p.captainOwnPct ?? 0) > 0).sort((a, b) => b.captainOwnPct! - a.captainOwnPct!).slice(0, 6)
    .map((p) => ({ name: p.name, position: p.position, team: p.team, captainPct: p.captainOwnPct! }));
  const own = (p: SlateReadPlayer) => Math.max(0, p.ownPct ?? 0);
  const totalOwn = active.reduce((s, p) => s + own(p), 0);
  const teams = [...new Set(active.map((p) => p.team))];
  const teamSharePct = totalOwn > 0 ? teams.map((team) => ({ team, pct: (100 * active.filter((p) => p.team === team).reduce((s, p) => s + own(p), 0)) / totalOwn })).sort((a, b) => b.pct - a.pct) : [];
  const statements: string[] = [];
  if (game?.total != null) {
    const fav = game.favorite && game.homeSpread != null ? `${game.favorite} favored by ${Math.abs(game.homeSpread)}` : "no favorite";
    statements.push(`Line: total ${game.total}, ${fav}.`);
  } else statements.push("No line was found for this game.");
  if (captains.length) statements.push(`Field captains (estimated): ${captains.slice(0, 4).map((c) => `${c.name} ${pct(c.captainPct)}`).join(", ")}.`);
  if (teamSharePct.length === 2) statements.push(`Field roster spots (estimated): ${teamSharePct[0].team} ${pct(teamSharePct[0].pct)}, ${teamSharePct[1].team} ${pct(teamSharePct[1].pct)}. An average field lineup is close to ${teamSharePct[0].pct >= 58 ? "4-2" : "3-3"} toward ${teamSharePct[0].team}.`);
  const dog = game?.favorite ? (game.favorite === game.home ? game.away : game.home) : null;
  statements.push(`Across 292 past Showdowns the field's mix of 3-3, 4-2 and 5-1 paid about evenly. The one shape that lost every season was five players from the underdog${dog ? ` (${dog} today)` : ""}: ${SHOWDOWN_SPLIT_REFERENCE_READ.fiveOneUnderdog.toFixed(2)} per $1.`);
  return { game, captains, teamSharePct, statements };
}
