/**
 * Regenerate `expected` in the shared ownership-prior fixtures from the
 * TypeScript prior, the authority the Python mirrors are pinned to:
 *   tests/fixtures/nfl_ownership_prior_showdown_fixture.json (v3, v4, v5 Showdown)
 *   tests/fixtures/nfl_ownership_prior_classic_fixture.json  (v3, v4, v5 Classic)
 * Run from web/ only when the TS prior changes on purpose:
 *   node --import tsx ./scripts/write-nfl-ownership-prior-fixture.ts
 */
import { readFileSync, writeFileSync } from "node:fs";
import { projectOwnershipPrior, type OwnershipPriorPlayer, type OwnershipPriorVariant } from "../src/lib/nfl-dfs/ownership-prior";

const SHOWDOWN = "../tests/fixtures/nfl_ownership_prior_showdown_fixture.json";
const showdown = JSON.parse(readFileSync(SHOWDOWN, "utf8"));
const sdPlayers = showdown.players as OwnershipPriorPlayer[];
showdown.expected = (showdown.scenarios as { name: string; variant: OwnershipPriorVariant; favoriteTeam: string | null; underdogTeam: string | null }[])
  .map((s) => {
    const r = projectOwnershipPrior(sdPlayers, "showdown", { variant: s.variant, favoriteTeam: s.favoriteTeam, underdogTeam: s.underdogTeam });
    return { name: s.name, version: r.version, unallocatedPct: r.unallocatedPct, notes: r.notes ?? null,
      players: r.players.map((p) => ({ dkPlayerId: p.dkPlayerId, captainPct: p.captainPct, flexPct: p.flexPct })) };
  });
writeFileSync(SHOWDOWN, `${JSON.stringify(showdown, null, 2)}\n`);
console.log(`wrote ${showdown.expected.length} scenarios x ${sdPlayers.length} players to ${SHOWDOWN}`);

const CLASSIC = "../tests/fixtures/nfl_ownership_prior_classic_fixture.json";
const classic = JSON.parse(readFileSync(CLASSIC, "utf8"));
const clPlayers = classic.players as OwnershipPriorPlayer[];
classic.expected = (classic.scenarios as { name: string; variant: OwnershipPriorVariant }[]).map((s) => {
  const r = projectOwnershipPrior(clPlayers, "classic", { variant: s.variant });
  return { name: s.name, version: r.version, unallocatedPct: r.unallocatedPct, notes: r.notes ?? null,
    players: r.players.map((p) => ({ dkPlayerId: p.dkPlayerId, ownPct: p.ownPct })) };
});
writeFileSync(CLASSIC, `${JSON.stringify(classic, null, 2)}\n`);
console.log(`wrote ${classic.expected.length} scenarios x ${clPlayers.length} players to ${CLASSIC}`);
