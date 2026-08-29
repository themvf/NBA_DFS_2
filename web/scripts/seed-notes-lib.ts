// Shared seeding for the hand-written note lists.
//
// Each list is a separate script that supplies its category and its rows; this
// module owns the player resolution and the upsert, so a new list is a data
// file rather than a copy of the logic.
//
// Idempotent: ff_player_notes is UNIQUE(player_id, category), so re-running
// updates the same row rather than duplicating, and it will NOT clobber a note
// you have since edited unless you pass --force.

import { sql } from "drizzle-orm";

import { db } from "../src/db";
import { ensureFantasyFootballTables } from "../src/db/ensure-schema";
import {
  normalizeAnalystName,
  type AnalystVerdict,
  type NoteCategory,
} from "../src/lib/fantasy-football/analyst-notes";

export const SEED_SEASON = 2026;

export type SeedNote = {
  listRank: number;
  name: string;
  position: string;
  team: string;
  adp: number;
  verdict: AnalystVerdict;
  verdictLabel: string;
  note: string;
};

type BoardPlayer = { id: number; position: string; name: string };

export async function seedNotes(options: {
  category: NoteCategory;
  author: string;
  notes: SeedNote[];
}): Promise<void> {
  const force = process.argv.includes("--force");
  await ensureFantasyFootballTables();

  // Resolve against the CURRENT board, not all of ff_players. Duplicate player
  // rows genuinely exist (a stale orphan with no sleeper/gsis id alongside the
  // live row -- Puka Nacua and Trevor Lawrence both have one, and CLAUDE.md
  // already records the Lawrence case). Matching the whole table silently
  // attached those notes to the dead row, where no surface would ever render
  // them. Only players on the latest ranking set are eligible.
  const players = await db.execute(sql`
    SELECT p.id, p.normalized_name, p.position, p.canonical_name
    FROM ff_players p
    JOIN ff_player_rankings r ON r.player_id = p.id
    WHERE p.season = ${SEED_SEASON}
      AND r.ranking_set_id = (
        SELECT rs.id FROM ff_ranking_sets rs
        WHERE COALESCE(rs.scoring_profile->>'preset','PPR') = 'PPR'
        ORDER BY rs.created_at DESC LIMIT 1
      )
  `);

  const byName = new Map<string, BoardPlayer[]>();
  for (const row of players.rows as Array<Record<string, unknown>>) {
    const key = String(row.normalized_name);
    const entry = { id: Number(row.id), position: String(row.position), name: String(row.canonical_name) };
    const bucket = byName.get(key);
    if (bucket) bucket.push(entry);
    else byName.set(key, [entry]);
  }

  let inserted = 0;
  let updated = 0;
  let skipped = 0;
  const unmatched: string[] = [];

  for (const seed of options.notes) {
    const bucket = byName.get(normalizeAnalystName(seed.name)) ?? [];
    // Position disambiguates a shared name; with one candidate, take it -- the
    // note's position and the roster feed's can legitimately differ.
    const matches = bucket.length === 1 ? bucket : bucket.filter((row) => row.position === seed.position);
    if (matches.length !== 1) {
      // Refuse to guess. Silently taking the first is exactly how the notes for
      // Puka Nacua and Trevor Lawrence landed on rows nothing renders.
      unmatched.push(
        matches.length === 0
          ? `#${seed.listRank} ${seed.name} (${seed.position}) -- no board player`
          : `#${seed.listRank} ${seed.name} (${seed.position}) -- ambiguous, ${matches.length} board players`,
      );
      continue;
    }
    const player = matches[0];

    const result = await db.execute(sql`
      INSERT INTO ff_player_notes (
        player_id, season, normalized_name, position, category, verdict,
        verdict_label, note, list_rank, source_team, source_adp, author
      ) VALUES (
        ${player.id}, ${SEED_SEASON}, ${normalizeAnalystName(seed.name)}, ${seed.position},
        ${options.category}, ${seed.verdict}, ${seed.verdictLabel}, ${seed.note},
        ${seed.listRank}, ${seed.team}, ${seed.adp}, ${options.author}
      )
      ON CONFLICT (player_id, category) DO UPDATE SET
        verdict = CASE WHEN ${force} THEN EXCLUDED.verdict ELSE ff_player_notes.verdict END,
        verdict_label = CASE WHEN ${force} THEN EXCLUDED.verdict_label ELSE ff_player_notes.verdict_label END,
        note = CASE WHEN ${force} THEN EXCLUDED.note ELSE ff_player_notes.note END,
        updated_at = CASE WHEN ${force} THEN NOW() ELSE ff_player_notes.updated_at END
      RETURNING (xmax = 0) AS was_insert
    `);
    const row = (result.rows as Array<Record<string, unknown>>)[0];
    if (row?.was_insert === true || row?.was_insert === "t") inserted += 1;
    else if (force) updated += 1;
    else skipped += 1;
  }

  console.log(
    `seed [${options.category}] -> ${inserted} inserted, ${updated} overwritten, ${skipped} left alone (already present)`,
  );
  if (unmatched.length) {
    console.log(`${unmatched.length} note(s) matched no ${SEED_SEASON} board player and were skipped:`);
    for (const label of unmatched) console.log(`  - ${label}`);
  }
}

export function runSeed(promise: Promise<void>): void {
  promise.then(() => process.exit(0)).catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
