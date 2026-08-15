"use server";

import { randomUUID } from "node:crypto";
import { sql } from "drizzle-orm";
import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { db } from "@/db";
import { ensureFantasyFootballTables } from "@/db/ensure-schema";
import { buildSnakeSlots } from "@/lib/fantasy-football/draft-engine";
import { calculateRosterSize, getRosterPreset, getScoringPreset, ROSTER_PRESETS, SCORING_PRESETS } from "@/lib/fantasy-football/league-config";
import { getFantasyPercentileProfile, type FantasyPercentileProfile } from "@/db/queries-fantasy-football";
import { queryRows } from "@/db/query-result";

export async function createFantasyDraft(formData: FormData): Promise<void> {
  await ensureFantasyFootballTables();
  const name = String(formData.get("name") || "My Draft").slice(0, 80);
  const rankingSetId = Number(formData.get("rankingSetId"));
  const teamCount = Number(formData.get("teamCount") || 12);
  const controlledSlot = Number(formData.get("controlledSlot") || 1);
  const rounds = Number(formData.get("rounds") || 15);
  const season = Number(formData.get("season") || 2026);
  const scoringPreset = String(formData.get("scoring") || "HALF");
  const rosterPreset = String(formData.get("roster") || "hood-rivals");
  
  if (!Number.isInteger(rankingSetId) || rankingSetId <= 0) throw new Error("Choose a ranking set");
  if (!(scoringPreset in SCORING_PRESETS)) throw new Error("Choose a valid scoring format");
  if (!(rosterPreset in ROSTER_PRESETS)) throw new Error("Choose a valid roster format");

  const rosterConfig = getRosterPreset(rosterPreset);
  const expectedRounds = calculateRosterSize(rosterConfig);
  if (rounds !== expectedRounds) throw new Error(`This roster format requires ${expectedRounds} draft rounds`);
  buildSnakeSlots(teamCount, rounds);
  if (controlledSlot < 1 || controlledSlot > teamCount) throw new Error("Draft slot is outside the league");

  const rankingSetResult = await db.execute(sql`SELECT season,COALESCE(scoring_profile->>'preset', 'PPR') AS scoring
    FROM ff_ranking_sets WHERE id=${rankingSetId}`);
  const rankingSet = queryRows<{ season: number; scoring: string }>(rankingSetResult)[0];
  if (!rankingSet) throw new Error("The selected ranking snapshot no longer exists");
  if (rankingSet.season !== season) throw new Error(`Choose a ${season} ranking snapshot`);
  if (rankingSet.scoring !== scoringPreset) {
    throw new Error(`Choose a ${SCORING_PRESETS[scoringPreset as keyof typeof SCORING_PRESETS].name} ranking snapshot`);
  }
  
  const scoringConfig = getScoringPreset(scoringPreset);
  
  const draftId = randomUUID();
  await db.execute(sql`WITH new_draft AS (
      INSERT INTO ff_draft_sessions
        (id,name,season,status,team_count,controlled_slot,round_count,roster_config,scoring_config,recommendation_config,ranking_set_id)
      VALUES (${draftId}::uuid,${name},${season},'active',${teamCount},${controlledSlot},${rounds},
        ${JSON.stringify(rosterConfig)}::jsonb,${JSON.stringify(scoringConfig)}::jsonb,
        ${JSON.stringify({ model: "ff-independent-v1.6" })}::jsonb,${rankingSetId}) RETURNING id
    ), teams AS (
      INSERT INTO ff_draft_teams(draft_id,slot,name,is_controlled)
      SELECT new_draft.id,slot,CASE WHEN slot=${controlledSlot} THEN 'My Team' ELSE 'Team '||slot END,slot=${controlledSlot}
      FROM new_draft CROSS JOIN generate_series(1,${teamCount}) slot RETURNING id,slot,draft_id
    ), board AS (
      INSERT INTO ff_draft_slots(draft_id,overall_pick,round,pick_in_round,draft_team_id)
      SELECT teams.draft_id,(round_no-1)*${teamCount}+pick_no,round_no,pick_no,teams.id
      FROM generate_series(1,${rounds}) round_no CROSS JOIN generate_series(1,${teamCount}) pick_no
      JOIN teams ON teams.slot=CASE WHEN round_no%2=1 THEN pick_no ELSE ${teamCount}-pick_no+1 END
    ) INSERT INTO ff_draft_events(draft_id,event_type,source,payload)
      SELECT id,'draft_started','system','{}'::jsonb FROM new_draft`);
  redirect(`/fantasy-football/draft/${draftId}`);
}

export async function recordFantasyPick(input: { draftId: string; playerId: number; revision: number }): Promise<{ ok: boolean; error?: string }> {
  await ensureFantasyFootballTables();
  try {
    const result = await db.execute(sql`WITH claimed AS (
        UPDATE ff_draft_sessions d SET current_pick=d.current_pick+1,revision=d.revision+1,
          status=CASE WHEN d.current_pick>=d.team_count*d.round_count THEN 'completed' ELSE 'active' END,
          completed_at=CASE WHEN d.current_pick>=d.team_count*d.round_count THEN NOW() ELSE NULL END,updated_at=NOW()
        WHERE d.id=${input.draftId}::uuid AND d.revision=${input.revision} AND d.status='active'
          AND NOT EXISTS (SELECT 1 FROM ff_draft_events pick LEFT JOIN ff_draft_events reversal ON reversal.reverses_event_id=pick.id
            WHERE pick.draft_id=d.id AND pick.player_id=${input.playerId} AND pick.event_type='pick_made' AND reversal.id IS NULL)
        RETURNING d.id,d.current_pick-1 AS overall_pick
      ), inserted AS (
        INSERT INTO ff_draft_events(draft_id,event_type,overall_pick,player_id,draft_team_id,source)
        SELECT claimed.id,'pick_made',claimed.overall_pick,${input.playerId},slot.draft_team_id,'manual'
        FROM claimed JOIN ff_draft_slots slot ON slot.draft_id=claimed.id AND slot.overall_pick=claimed.overall_pick RETURNING id
      ) SELECT COUNT(*)::int AS count FROM inserted`);
    if (Number((result as unknown as Array<{ count: number }>)[0]?.count) !== 1) {
      throw new Error("Draft changed, is complete, or the player was already selected. Refresh and retry.");
    }
    revalidatePath(`/fantasy-football/draft/${input.draftId}`);
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Pick failed" };
  }
}

export async function undoFantasyPick(input: { draftId: string; revision: number }): Promise<{ ok: boolean; error?: string }> {
  await ensureFantasyFootballTables();
  try {
    const result = await db.execute(sql`WITH target AS (
        SELECT pick.id,pick.overall_pick,pick.player_id,pick.draft_team_id
        FROM ff_draft_events pick LEFT JOIN ff_draft_events reversal ON reversal.reverses_event_id=pick.id
        JOIN ff_draft_sessions d ON d.id=pick.draft_id
        WHERE pick.draft_id=${input.draftId}::uuid AND d.revision=${input.revision}
          AND pick.event_type='pick_made' AND reversal.id IS NULL ORDER BY pick.overall_pick DESC LIMIT 1
      ), claimed AS (
        UPDATE ff_draft_sessions d SET current_pick=target.overall_pick,revision=d.revision+1,status='active',completed_at=NULL,updated_at=NOW()
        FROM target WHERE d.id=${input.draftId}::uuid AND d.revision=${input.revision} RETURNING d.id
      ), reversed AS (
        INSERT INTO ff_draft_events(draft_id,event_type,overall_pick,player_id,draft_team_id,source,reverses_event_id)
        SELECT claimed.id,'pick_reversed',target.overall_pick,target.player_id,target.draft_team_id,'manual',target.id
        FROM claimed CROSS JOIN target RETURNING id
      ) SELECT COUNT(*)::int AS count FROM reversed`);
    if (Number((result as unknown as Array<{ count: number }>)[0]?.count) !== 1) throw new Error("There is no current pick to undo, or the draft changed.");
    revalidatePath(`/fantasy-football/draft/${input.draftId}`);
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Undo failed" };
  }
}

// Fetched lazily when a percentile-profile chip is expanded, not baked into
// every rankings-table row -- computing this against the full RB/WR/TE pool
// is cheap per-call but unnecessary for players nobody looks at.
export async function fetchFantasyPercentileProfile(
  input: { playerId: number; season: number; scoring: string },
): Promise<{ ok: true; profile: FantasyPercentileProfile } | { ok: false; error: string }> {
  if (!Number.isInteger(input.playerId) || input.playerId <= 0) return { ok: false, error: "Invalid player." };
  if (!Number.isInteger(input.season)) return { ok: false, error: "Invalid season." };
  try {
    const profile = await getFantasyPercentileProfile(input.playerId, input.season, input.scoring);
    if (!profile) return { ok: false, error: "Player not found." };
    return { ok: true, profile };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to load percentile profile." };
  }
}
