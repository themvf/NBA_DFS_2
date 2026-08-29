"use server";

import { randomUUID } from "node:crypto";
import { sql } from "drizzle-orm";
import { redirect } from "next/navigation";
import { revalidatePath } from "next/cache";
import { db } from "@/db";
import { ensureFantasyFootballTables } from "@/db/ensure-schema";
import { buildSnakeSlots } from "@/lib/fantasy-football/draft-engine";
import { calculateRosterSize, getRosterPreset, getScoringPreset, ROSTER_PRESETS, SCORING_PRESETS, type RosterConfig } from "@/lib/fantasy-football/league-config";
import { getFantasyDraftState, getFantasyPercentileProfile, type FantasyPercentileProfile } from "@/db/queries-fantasy-football";
import { AUTO_DRAFT_VERSION, selectComputerPick, type AutoDraftPlayer } from "@/lib/fantasy-football/auto-draft";
import { isDraftStrategy } from "@/lib/fantasy-football/draft-strategy";
import { queryRows } from "@/db/query-result";
import { ANALYST_NOTE_STYLE, isAnalystVerdict, validateNoteInput } from "@/lib/fantasy-football/analyst-notes";

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
  const draftMode = String(formData.get("draftMode") || "simulator");
  const strategyMode = String(formData.get("strategyMode") || "balanced");
  
  if (!Number.isInteger(rankingSetId) || rankingSetId <= 0) throw new Error("Choose a ranking set");
  if (!isDraftStrategy(strategyMode)) throw new Error("Choose a valid draft strategy");
  if (!(["simulator", "manual"] as const).includes(draftMode as "simulator" | "manual")) throw new Error("Choose a valid draft mode");
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
  const simulator = {
    enabled: draftMode === "simulator",
    version: AUTO_DRAFT_VERSION,
    seed: draftId,
  };
  const recommendationConfig = { model: "ff-independent-v1.6", strategy: strategyMode, simulator };

  await db.execute(sql`WITH new_draft AS (
      INSERT INTO ff_draft_sessions
        (id,name,season,status,team_count,controlled_slot,round_count,roster_config,scoring_config,recommendation_config,ranking_set_id)
      VALUES (${draftId}::uuid,${name},${season},'active',${teamCount},${controlledSlot},${rounds},
        ${JSON.stringify(rosterConfig)}::jsonb,${JSON.stringify(scoringConfig)}::jsonb,
        ${JSON.stringify(recommendationConfig)}::jsonb,${rankingSetId}) RETURNING id
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
      SELECT id,'draft_started','system',${JSON.stringify({ draftMode, simulatorVersion: AUTO_DRAFT_VERSION })}::jsonb FROM new_draft`);
  redirect(`/fantasy-football/draft/${draftId}`);
}

export async function recordFantasyPick(input: { draftId: string; playerId: number; revision: number; decision?: { strategy: string; impactLabel: string; nextPick: number | null; score?: number } }): Promise<{ ok: boolean; error?: string }> {
  await ensureFantasyFootballTables();
  try {
    const result = await db.execute(sql`WITH claimed AS (
        UPDATE ff_draft_sessions d SET current_pick=d.current_pick+1,revision=d.revision+1,
          status=CASE WHEN d.current_pick>=d.team_count*d.round_count THEN 'completed' ELSE 'active' END,
          completed_at=CASE WHEN d.current_pick>=d.team_count*d.round_count THEN NOW() ELSE NULL END,updated_at=NOW()
        WHERE d.id=${input.draftId}::uuid AND d.revision=${input.revision} AND d.status='active'
          AND (COALESCE(d.recommendation_config->'simulator'->>'enabled','false')<>'true'
            OR EXISTS (SELECT 1 FROM ff_draft_slots turn_slot JOIN ff_draft_teams turn_team ON turn_team.id=turn_slot.draft_team_id
              WHERE turn_slot.draft_id=d.id AND turn_slot.overall_pick=d.current_pick AND turn_team.is_controlled))
          AND NOT EXISTS (SELECT 1 FROM ff_draft_events pick LEFT JOIN ff_draft_events reversal ON reversal.reverses_event_id=pick.id
            WHERE pick.draft_id=d.id AND pick.player_id=${input.playerId} AND pick.event_type='pick_made' AND reversal.id IS NULL)
        RETURNING d.id,d.current_pick-1 AS overall_pick
      ), inserted AS (
        INSERT INTO ff_draft_events(draft_id,event_type,overall_pick,player_id,draft_team_id,source,payload)
        SELECT claimed.id,'pick_made',claimed.overall_pick,${input.playerId},slot.draft_team_id,'manual',${JSON.stringify(input.decision ?? {})}::jsonb
        FROM claimed JOIN ff_draft_slots slot ON slot.draft_id=claimed.id AND slot.overall_pick=claimed.overall_pick RETURNING id
      ) SELECT COUNT(*)::int AS count FROM inserted`);
    if (Number(queryRows<{ count: number }>(result)[0]?.count) !== 1) {
      throw new Error("Draft changed, is complete, or the player was already selected. Refresh and retry.");
    }
    revalidatePath(`/fantasy-football/draft/${input.draftId}`);
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Pick failed" };
  }
}

export async function advanceComputerDraft(input: { draftId: string; revision: number }): Promise<{ ok: boolean; picks?: number; error?: string }> {
  await ensureFantasyFootballTables();
  try {
    const state = await getFantasyDraftState(input.draftId);
    if (!state) throw new Error("Draft not found.");
    if (state.draft.revision !== input.revision) throw new Error("Draft changed. Refresh and retry.");
    if (state.draft.status !== "active") return { ok: true, picks: 0 };
    const rawSimulator = state.draft.recommendationConfig.simulator;
    const simulator = rawSimulator && typeof rawSimulator === "object" ? rawSimulator as Record<string, unknown> : null;
    if (simulator?.enabled !== true) throw new Error("This draft is not in simulator mode.");
    const simulatorVersion = typeof simulator.version === "string" ? simulator.version : null;
    if (simulatorVersion !== AUTO_DRAFT_VERSION) {
      throw new Error(`This draft uses unsupported simulator version ${simulatorVersion ?? "unknown"}. Start a new mock draft.`);
    }
    const seed = typeof simulator.seed === "string" || typeof simulator.seed === "number" ? simulator.seed : state.draft.id;
    const currentSlot = state.board.find((slot) => slot.overallPick === state.draft.currentPick);
    if (!currentSlot || currentSlot.isControlled) return { ok: true, picks: 0 };

    const asAutoPlayer = (player: typeof state.available[number]): AutoDraftPlayer => ({
      playerId: player.playerId,
      name: player.name,
      position: player.position,
      adp: player.adp,
      ecr: player.ecr,
      ourRank: player.ourRank,
      projectedPoints: player.ourProjectedPoints,
    });
    let available = state.available.map(asAutoPlayer);
    const draftedIds = new Set(state.board.flatMap((slot) => slot.playerId ? [slot.playerId] : []));
    const rosterByTeam = new Map<number, AutoDraftPlayer[]>();
    for (const slot of state.board) {
      if (!rosterByTeam.has(slot.teamSlot)) rosterByTeam.set(slot.teamSlot, []);
      if (slot.playerId && slot.playerName && slot.position) {
        rosterByTeam.get(slot.teamSlot)!.push({
          playerId: slot.playerId, name: slot.playerName, position: slot.position,
          adp: null, ecr: null, ourRank: null, projectedPoints: slot.ourProjectedPoints,
        });
      }
    }

    const batch: Array<{ overall_pick: number; player_id: number; draft_team_id: number; payload: Record<string, unknown> }> = [];
    let overallPick = state.draft.currentPick;
    while (overallPick <= state.board.length) {
      const slot = state.board[overallPick - 1];
      if (!slot || slot.isControlled) break;
      const roster = rosterByTeam.get(slot.teamSlot) ?? [];
      const selection = selectComputerPick({
        availablePlayers: available,
        roster,
        rosterConfig: state.draft.rosterConfig as unknown as RosterConfig,
        teamCount: state.draft.teamCount,
        teamSlot: slot.teamSlot,
        overallPick,
        seed,
        draftedPlayerIds: [...draftedIds],
      });
      if (!selection) throw new Error(`No legal CPU selection is available at pick ${overallPick}.`);
      const playerId = Number(selection.player.playerId);
      batch.push({
        overall_pick: overallPick,
        player_id: playerId,
        draft_team_id: slot.teamId,
        payload: {
          simulatorVersion,
          seed,
          teamSlot: slot.teamSlot,
          score: selection.score,
          reasons: selection.reasons,
          adjustedAdp: selection.adjustedAdp,
          rankingSource: selection.rankingSource,
        },
      });
      draftedIds.add(playerId);
      roster.push(selection.player);
      rosterByTeam.set(slot.teamSlot, roster);
      available = available.filter((player) => Number(player.playerId) !== playerId);
      overallPick += 1;
    }
    if (batch.length === 0) return { ok: true, picks: 0 };

    const batchJson = JSON.stringify(batch);
    const result = await db.execute(sql`WITH requested AS (
        SELECT * FROM jsonb_to_recordset(${batchJson}::jsonb)
          AS x(overall_pick integer,player_id bigint,draft_team_id bigint,payload jsonb)
      ), summary AS (
        SELECT COUNT(*)::int AS n,COUNT(DISTINCT player_id)::int AS unique_players,
          MIN(overall_pick)::int AS first_pick,MAX(overall_pick)::int AS last_pick FROM requested
      ), valid AS (
        SELECT COUNT(*)::int AS n FROM requested req
        JOIN ff_draft_slots slot ON slot.draft_id=${input.draftId}::uuid
          AND slot.overall_pick=req.overall_pick AND slot.draft_team_id=req.draft_team_id
        JOIN ff_draft_teams team ON team.id=slot.draft_team_id AND team.is_controlled=FALSE
        WHERE NOT EXISTS (
          SELECT 1 FROM ff_draft_events existing
          LEFT JOIN ff_draft_events reversal ON reversal.reverses_event_id=existing.id
          WHERE existing.draft_id=${input.draftId}::uuid AND existing.event_type='pick_made'
            AND reversal.id IS NULL AND (existing.player_id=req.player_id OR existing.overall_pick=req.overall_pick)
        )
      ), claimed AS (
        UPDATE ff_draft_sessions d SET current_pick=${overallPick},revision=d.revision+1,
          status=CASE WHEN ${overallPick}>d.team_count*d.round_count THEN 'completed' ELSE 'active' END,
          completed_at=CASE WHEN ${overallPick}>d.team_count*d.round_count THEN NOW() ELSE NULL END,updated_at=NOW()
        WHERE d.id=${input.draftId}::uuid AND d.revision=${input.revision} AND d.current_pick=${state.draft.currentPick}
          AND d.status='active' AND COALESCE(d.recommendation_config->'simulator'->>'enabled','false')='true'
          AND (SELECT n=${batch.length} AND unique_players=${batch.length}
            AND first_pick=${state.draft.currentPick} AND last_pick=${overallPick - 1} FROM summary)
          AND (SELECT n=${batch.length} FROM valid)
        RETURNING d.id
      ), inserted AS (
        INSERT INTO ff_draft_events(draft_id,event_type,overall_pick,player_id,draft_team_id,source,payload)
        SELECT claimed.id,'pick_made',req.overall_pick,req.player_id,req.draft_team_id,'system',req.payload
        FROM requested req CROSS JOIN claimed ORDER BY req.overall_pick RETURNING id
      ) SELECT COUNT(*)::int AS count FROM inserted`);
    if (Number(queryRows<{ count: number }>(result)[0]?.count) !== batch.length) {
      throw new Error("Draft changed while computer teams were selecting. Refresh and retry.");
    }
    revalidatePath(`/fantasy-football/draft/${input.draftId}`);
    return { ok: true, picks: batch.length };
  } catch (error) {
    console.error("advanceComputerDraft failed", error);
    return { ok: false, error: error instanceof Error ? error.message : "Computer picks failed" };
  }
}

export async function undoFantasyPick(input: { draftId: string; revision: number }): Promise<{ ok: boolean; error?: string }> {
  await ensureFantasyFootballTables();
  try {
    const result = await db.execute(sql`WITH config AS (
        SELECT id,COALESCE(recommendation_config->'simulator'->>'enabled','false')='true' AS simulator
        FROM ff_draft_sessions WHERE id=${input.draftId}::uuid AND revision=${input.revision}
      ), active AS (
        SELECT pick.id,pick.overall_pick,pick.player_id,pick.draft_team_id,team.is_controlled
        FROM ff_draft_events pick
        JOIN ff_draft_teams team ON team.id=pick.draft_team_id
        LEFT JOIN ff_draft_events reversal ON reversal.reverses_event_id=pick.id
        WHERE pick.draft_id=${input.draftId}::uuid AND pick.event_type='pick_made' AND reversal.id IS NULL
      ), target_pick AS (
        SELECT CASE WHEN config.simulator
          THEN MAX(active.overall_pick) FILTER (WHERE active.is_controlled)
          ELSE MAX(active.overall_pick) END AS overall_pick,config.simulator
        FROM config LEFT JOIN active ON TRUE GROUP BY config.simulator
      ), targets AS (
        SELECT active.* FROM active CROSS JOIN target_pick
        WHERE target_pick.overall_pick IS NOT NULL
          AND ((target_pick.simulator AND active.overall_pick>=target_pick.overall_pick)
            OR (NOT target_pick.simulator AND active.overall_pick=target_pick.overall_pick))
      ), claimed AS (
        UPDATE ff_draft_sessions d SET current_pick=target_pick.overall_pick,revision=d.revision+1,
          status='active',completed_at=NULL,updated_at=NOW()
        FROM target_pick WHERE d.id=${input.draftId}::uuid AND d.revision=${input.revision}
          AND target_pick.overall_pick IS NOT NULL RETURNING d.id
      ), reversed AS (
        INSERT INTO ff_draft_events(draft_id,event_type,overall_pick,player_id,draft_team_id,source,reverses_event_id,payload)
        SELECT claimed.id,'pick_reversed',target.overall_pick,target.player_id,target.draft_team_id,'manual',target.id,
          jsonb_build_object('groupedSimulatorUndo',target_pick.simulator)
        FROM claimed CROSS JOIN targets target CROSS JOIN target_pick RETURNING id
      ) SELECT COUNT(*)::int AS count FROM reversed`);
    if (Number(queryRows<{ count: number }>(result)[0]?.count) < 1) throw new Error("There is no current pick to undo, or the draft changed.");
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

// --- Player notes -----------------------------------------------------------
//
// Editorial notes authored in /fantasy-football/notes and surfaced as the
// tooltip on the redraft board, the Best Ball board, and the Shadow panel.
// These write to ff_player_notes and to nothing else -- a note can never move a
// projection, rank, VOR, or ADP.
//
// NOTE ON ACCESS: these actions are deliberately ungated, per an explicit
// decision (2026-08-29) that the deployment is not publicly reachable. If that
// ever changes, gate HERE -- both actions and the page loader funnel through
// this file, so a single guard at the top of each covers every write path.
// Hiding the page's link would not be enough: server actions are callable
// directly whether or not a form renders.

export async function saveFantasyPlayerNote(input: {
  playerId: number;
  verdict: string;
  verdictLabel: string;
  note: string;
}): Promise<{ ok: boolean; error?: string }> {
  if (!Number.isInteger(input.playerId) || input.playerId <= 0) return { ok: false, error: "Pick a player first." };
  if (!isAnalystVerdict(input.verdict)) return { ok: false, error: "Pick a verdict." };

  const note = input.note.trim();
  const verdictLabel = input.verdictLabel.trim() || ANALYST_NOTE_STYLE[input.verdict].label;
  const invalid = validateNoteInput(note, verdictLabel);
  if (invalid) return { ok: false, error: invalid };

  try {
    await ensureFantasyFootballTables();
    const player = await db.execute(sql`
      SELECT id, season, normalized_name, position FROM ff_players WHERE id=${input.playerId}
    `);
    const row = (player.rows as Array<Record<string, unknown>>)[0];
    if (!row) return { ok: false, error: "That player is not in the database." };

    // Season/name/position are denormalized from the player row rather than
    // trusted from the client, so a stale form cannot mislabel a note.
    await db.execute(sql`
      INSERT INTO ff_player_notes (player_id, season, normalized_name, position, verdict, verdict_label, note, author)
      VALUES (${input.playerId}, ${Number(row.season)}, ${String(row.normalized_name)}, ${String(row.position)},
              ${input.verdict}, ${verdictLabel}, ${note}, ${"admin"})
      ON CONFLICT (player_id) DO UPDATE SET
        verdict = EXCLUDED.verdict,
        verdict_label = EXCLUDED.verdict_label,
        note = EXCLUDED.note,
        updated_at = NOW()
    `);

    // Every surface that renders a note reads the same rankings query.
    revalidatePath("/fantasy-football/notes");
    revalidatePath("/fantasy-football/rankings");
    revalidatePath("/fantasy-football/redraft");
    revalidatePath("/fantasy-football/best-ball");
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to save the note." };
  }
}

export async function deleteFantasyPlayerNote(input: { playerId: number }): Promise<{ ok: boolean; error?: string }> {
  if (!Number.isInteger(input.playerId) || input.playerId <= 0) return { ok: false, error: "Invalid player." };
  try {
    await ensureFantasyFootballTables();
    await db.execute(sql`DELETE FROM ff_player_notes WHERE player_id=${input.playerId}`);
    revalidatePath("/fantasy-football/notes");
    revalidatePath("/fantasy-football/rankings");
    revalidatePath("/fantasy-football/redraft");
    revalidatePath("/fantasy-football/best-ball");
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to delete the note." };
  }
}
