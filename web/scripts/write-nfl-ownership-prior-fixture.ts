/**
 * Regenerate `expected` in tests/fixtures/nfl_ownership_prior_showdown_fixture.json
 * from the TypeScript prior, the authority the Python mirror is pinned to.
 * Run from web/ only when the TS prior changes on purpose:
 *   node --import tsx ./scripts/write-nfl-ownership-prior-fixture.ts
 */
import { readFileSync, writeFileSync } from "node:fs";
import { projectOwnershipPrior, type OwnershipPriorPlayer, type OwnershipPriorVariant } from "../src/lib/nfl-dfs/ownership-prior";

const PATH = "../tests/fixtures/nfl_ownership_prior_showdown_fixture.json";
const fixture = JSON.parse(readFileSync(PATH, "utf8"));
const players = fixture.players as OwnershipPriorPlayer[];
fixture.expected = (fixture.scenarios as { name: string; variant: OwnershipPriorVariant; favoriteTeam: string | null; underdogTeam: string | null }[])
  .map((s) => {
    const r = projectOwnershipPrior(players, "showdown", { variant: s.variant, favoriteTeam: s.favoriteTeam, underdogTeam: s.underdogTeam });
    return { name: s.name, version: r.version, unallocatedPct: r.unallocatedPct, notes: r.notes ?? null,
      players: r.players.map((p) => ({ dkPlayerId: p.dkPlayerId, captainPct: p.captainPct, flexPct: p.flexPct })) };
  });
writeFileSync(PATH, `${JSON.stringify(fixture, null, 2)}\n`);
console.log(`wrote ${fixture.expected.length} scenarios x ${players.length} players to ${PATH}`);
