"use server";

/**
 * Server actions for survivor pools, picks, and the recommendation ledger.
 *
 * The ledger is the reason this page can ever be graded. A recommendation is
 * frozen with the evidence that produced it -- probability, provenance, the
 * whole planned path, the opportunity cost, and the field's pick share at that
 * moment -- and is then never rewritten. Changing your mind appends a new row
 * and supersedes the old one, so "what did we advise, and when" survives
 * intact. Settlement is Python's job (model/survivor_settlement.py).
 *
 * A pick locks at its own game's kickoff. Editing it afterwards is refused
 * here rather than in the UI, because a pick that can be changed after the
 * game starts is not a record of a decision.
 */

import { revalidatePath } from "next/cache";
import { db } from "@/db";
import { ensureSurvivorTables } from "@/db/ensure-schema";
import { sql } from "drizzle-orm";

export type ActionResult = { ok: true; message: string } | { ok: false; error: string };

export async function createPool(input: {
  name: string;
  season: number;
  poolSize: number | null;
  tieRule: "tie_loses" | "tie_survives";
  strikes: number;
  startWeek: number;
  endWeek: number;
  entryLabels: string[];
}): Promise<ActionResult> {
  const name = input.name.trim();
  if (!name) return { ok: false, error: "Name the pool so you can tell it from the others." };
  if (input.endWeek < input.startWeek) {
    return { ok: false, error: "The last week cannot come before the first." };
  }

  await ensureSurvivorTables();
  try {
    const inserted = await db.execute(sql`
      INSERT INTO survivor_pools
        (name, season, entry_count, pool_size, tie_rule, strikes, start_week, end_week)
      VALUES (${name}, ${input.season}, ${Math.max(input.entryLabels.length, 1)},
              ${input.poolSize}, ${input.tieRule}, ${input.strikes},
              ${input.startWeek}, ${input.endWeek})
      RETURNING id
    `);
    const poolId = Number((inserted.rows[0] as Record<string, unknown>).id);

    const labels = input.entryLabels.length ? input.entryLabels : ["Entry 1"];
    for (const label of labels) {
      await db.execute(sql`
        INSERT INTO survivor_entries (pool_id, label) VALUES (${poolId}, ${label.trim() || "Entry"})
        ON CONFLICT (pool_id, label) DO NOTHING
      `);
    }
    revalidatePath("/nfl/survivor");
    return { ok: true, message: `Created ${name} with ${labels.length} entr${labels.length === 1 ? "y" : "ies"}.` };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not create the pool." };
  }
}

export async function deletePool(poolId: number): Promise<ActionResult> {
  await ensureSurvivorTables();
  try {
    await db.execute(sql`DELETE FROM survivor_pools WHERE id = ${poolId}`);
    revalidatePath("/nfl/survivor");
    return { ok: true, message: "Pool deleted." };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not delete the pool." };
  }
}

export async function addEntry(poolId: number, label: string): Promise<ActionResult> {
  const trimmed = label.trim();
  if (!trimmed) return { ok: false, error: "Give the entry a label." };
  await ensureSurvivorTables();
  try {
    await db.execute(sql`
      INSERT INTO survivor_entries (pool_id, label) VALUES (${poolId}, ${trimmed})
      ON CONFLICT (pool_id, label) DO NOTHING
    `);
    await db.execute(sql`
      UPDATE survivor_pools
      SET entry_count = (SELECT COUNT(*) FROM survivor_entries WHERE pool_id = ${poolId}),
          updated_at = NOW()
      WHERE id = ${poolId}
    `);
    revalidatePath("/nfl/survivor");
    return { ok: true, message: `Added ${trimmed}.` };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not add the entry." };
  }
}

/** Commit a pick. Refuses to overwrite one whose game has already started. */
export async function commitPick(input: {
  entryId: number;
  season: number;
  week: number;
  teamId: number;
}): Promise<ActionResult> {
  await ensureSurvivorTables();
  try {
    const existing = await db.execute(sql`
      SELECT locked_at, result FROM survivor_entry_picks
      WHERE entry_id = ${input.entryId} AND week = ${input.week}
    `);
    const current = existing.rows[0] as Record<string, unknown> | undefined;
    if (current && (current.locked_at != null || current.result !== "pending")) {
      return { ok: false, error: `Week ${input.week} is locked — that game has already started.` };
    }

    const game = await db.execute(sql`
      SELECT g.id, g.kickoff, w.p_win, w.p_tie, w.provenance
      FROM nfl_season_games g
      LEFT JOIN nfl_game_win_probs w
        ON w.game_id = g.id AND w.team_id = ${input.teamId}
      WHERE g.season = ${input.season} AND g.week = ${input.week}
        AND (g.home_team_id = ${input.teamId} OR g.away_team_id = ${input.teamId})
      LIMIT 1
    `);
    const row = game.rows[0] as Record<string, unknown> | undefined;
    if (!row) return { ok: false, error: `That team has no week ${input.week} game — it is on bye.` };
    if (row.kickoff != null && new Date(String(row.kickoff)) <= new Date()) {
      return { ok: false, error: `Week ${input.week} kickoff has passed for that team.` };
    }

    await db.execute(sql`
      INSERT INTO survivor_entry_picks
        (entry_id, week, team_id, game_id, p_advance_at_pick, provenance_at_pick)
      VALUES (${input.entryId}, ${input.week}, ${input.teamId}, ${Number(row.id)},
              ${row.p_win != null ? Number(row.p_win) : null},
              ${row.provenance != null ? String(row.provenance) : null})
      ON CONFLICT (entry_id, week) DO UPDATE SET
        team_id = EXCLUDED.team_id,
        game_id = EXCLUDED.game_id,
        p_advance_at_pick = EXCLUDED.p_advance_at_pick,
        provenance_at_pick = EXCLUDED.provenance_at_pick,
        created_at = NOW()
    `);
    revalidatePath("/nfl/survivor");
    return { ok: true, message: `Week ${input.week} pick saved.` };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not save the pick." };
  }
}

export async function clearPick(entryId: number, week: number): Promise<ActionResult> {
  await ensureSurvivorTables();
  try {
    const result = await db.execute(sql`
      DELETE FROM survivor_entry_picks
      WHERE entry_id = ${entryId} AND week = ${week}
        AND locked_at IS NULL AND result = 'pending'
      RETURNING id
    `);
    if (result.rows.length === 0) {
      return { ok: false, error: `Week ${week} is locked or already settled.` };
    }
    revalidatePath("/nfl/survivor");
    return { ok: true, message: `Week ${week} cleared.` };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not clear the pick." };
  }
}

/**
 * Freeze this week's recommendation into the append-only ledger.
 *
 * Never updates in place. An earlier recommendation for the same entry and
 * week is marked superseded and kept, because the question the ledger has to
 * answer later is "what did it say, and when" -- not "what does it say now".
 */
export async function freezeRecommendation(input: {
  poolId: number | null;
  entryId: number | null;
  season: number;
  week: number;
  teamId: number;
  pAdvance: number | null;
  provenance: string | null;
  objectiveMode: string;
  path: Array<{ week: number; team: string }>;
  pathSurvivalProb: number | null;
  opportunityCost: number | null;
  pickPct: number | null;
  alternatives: Array<{ team: string; p: number; survivalCost: number }>;
  constraints: Record<string, unknown>;
  modelVersion: string;
}): Promise<ActionResult> {
  await ensureSurvivorTables();
  try {
    const game = await db.execute(sql`
      SELECT id, kickoff FROM nfl_season_games
      WHERE season = ${input.season} AND week = ${input.week}
        AND (home_team_id = ${input.teamId} OR away_team_id = ${input.teamId})
      LIMIT 1
    `);
    const row = game.rows[0] as Record<string, unknown> | undefined;
    if (!row) return { ok: false, error: "That team has no game that week." };
    if (row.kickoff != null && new Date(String(row.kickoff)) <= new Date()) {
      return {
        ok: false,
        error: "That game has kicked off — a recommendation frozen after the fact is hindsight, not advice.",
      };
    }

    const inserted = await db.execute(sql`
      INSERT INTO survivor_recommendations
        (pool_id, entry_id, season, week, recommended_team_id, game_id, p_advance,
         provenance, objective_mode, path_json, path_survival_prob, opportunity_cost,
         pick_pct_at_rec, alternatives_json, constraints_json, model_version, event_commence)
      VALUES (${input.poolId}, ${input.entryId}, ${input.season}, ${input.week},
              ${input.teamId}, ${Number(row.id)}, ${input.pAdvance}, ${input.provenance},
              ${input.objectiveMode}, ${JSON.stringify(input.path)}::jsonb,
              ${input.pathSurvivalProb}, ${input.opportunityCost}, ${input.pickPct},
              ${JSON.stringify(input.alternatives)}::jsonb,
              ${JSON.stringify(input.constraints)}::jsonb, ${input.modelVersion},
              ${row.kickoff != null ? String(row.kickoff) : null}::timestamptz)
      RETURNING id
    `);
    const newId = Number((inserted.rows[0] as Record<string, unknown>).id);

    await db.execute(sql`
      UPDATE survivor_recommendations
      SET superseded_by = ${newId}
      WHERE season = ${input.season} AND week = ${input.week}
        AND id <> ${newId} AND superseded_by IS NULL
        AND entry_id IS NOT DISTINCT FROM ${input.entryId}
    `);

    revalidatePath("/nfl/survivor");
    return { ok: true, message: `Week ${input.week} recommendation frozen.` };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Could not freeze the recommendation." };
  }
}
