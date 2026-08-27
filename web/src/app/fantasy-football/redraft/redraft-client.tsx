"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import {
  REDRAFT_AUTO_DRAFT_ROSTER_CONFIG,
  REDRAFT_BENCH_SLOTS,
  REDRAFT_FLEX_SLOTS,
  REDRAFT_POSITIONS,
  REDRAFT_POSITION_LABEL,
  REDRAFT_ROSTER_SIZE,
  REDRAFT_ROUNDS,
  REDRAFT_SLOTS,
  REDRAFT_STARTER_COUNT,
  REDRAFT_STARTER_SLOTS,
  REDRAFT_TEAM_COUNT,
  canAddRedraftPlayer,
  getRedraftRosterStatus,
  parseRedraftState,
  type RedraftDraftState,
} from "@/lib/fantasy-football/redraft";
import { nextControlledPick } from "@/lib/fantasy-football/draft-engine";
import { computeAvailabilityOdds } from "@/lib/fantasy-football/availability-odds";
import { computeCpuDraftBatch, localAutoDraftSeed, mapRankingsToAutoDraftPlayers } from "@/lib/fantasy-football/local-auto-draft";
import { buildYahooRedraftStrategy, type YahooRedraftStrategyPlayer } from "@/lib/fantasy-football/yahoo-redraft-strategy";
import RedraftPlayerBoard from "./redraft-player-board";
import YahooRedraftStrategyPanel from "./yahoo-redraft-strategy-panel";

const TOTAL_PICKS = REDRAFT_TEAM_COUNT * REDRAFT_ROUNDS;
const EMPTY_DRAFT: RedraftDraftState = { userSlot: 1, playerIds: [], cpuEnabled: false };

/** Local-only draft persistence, deferred to idle time (same pattern as Best Ball). */
function useRedraftDraft(storageKey: string) {
  const [draft, setDraft] = useState<RedraftDraftState>(EMPTY_DRAFT);
  const pendingRef = useRef<RedraftDraftState | null>(null);
  const handleRef = useRef<number | null>(null);
  const modeRef = useRef<"idle" | "timeout" | null>(null);

  const cancelPending = useCallback(() => {
    if (handleRef.current === null) return;
    const cancelIdle = (window as unknown as { cancelIdleCallback?: Window["cancelIdleCallback"] }).cancelIdleCallback;
    if (modeRef.current === "idle" && cancelIdle) cancelIdle.call(window, handleRef.current);
    else window.clearTimeout(handleRef.current);
    handleRef.current = null;
    modeRef.current = null;
  }, []);

  const persistPending = useCallback(() => {
    if (pendingRef.current) localStorage.setItem(storageKey, JSON.stringify(pendingRef.current));
    pendingRef.current = null;
    handleRef.current = null;
    modeRef.current = null;
  }, [storageKey]);

  const schedulePersistence = useCallback((next: RedraftDraftState) => {
    pendingRef.current = next;
    cancelPending();
    const requestIdle = (window as unknown as { requestIdleCallback?: Window["requestIdleCallback"] }).requestIdleCallback;
    if (requestIdle) {
      modeRef.current = "idle";
      handleRef.current = requestIdle.call(window, persistPending, { timeout: 400 });
    } else {
      modeRef.current = "timeout";
      handleRef.current = window.setTimeout(persistPending, 0);
    }
  }, [cancelPending, persistPending]);

  useEffect(() => {
    const initialLoad = window.setTimeout(() => {
      setDraft(parseRedraftState(localStorage.getItem(storageKey) ?? ""));
    }, 0);
    const syncFromAnotherTab = (event: StorageEvent) => {
      if (event.key === storageKey) setDraft(parseRedraftState(event.newValue ?? ""));
    };
    window.addEventListener("storage", syncFromAnotherTab);
    return () => {
      window.clearTimeout(initialLoad);
      window.removeEventListener("storage", syncFromAnotherTab);
      cancelPending();
      persistPending();
    };
  }, [cancelPending, persistPending, storageKey]);

  const updateDraft = useCallback((updater: (current: RedraftDraftState) => RedraftDraftState) => {
    setDraft((current) => {
      const next = updater(current);
      if (next !== current) schedulePersistence(next);
      return next;
    });
  }, [schedulePersistence]);

  return { draft, updateDraft };
}

export default function RedraftClient({ rankings, rankingSetId }: { rankings: FantasyRankingRow[]; rankingSetId: number }) {
  const storageKey = `dfs-vegas:yahoo-redraft:v1:${rankingSetId}`;
  const [viewTeam, setViewTeam] = useState<number | null>(null);
  const { draft, updateDraft } = useRedraftDraft(storageKey);
  const playerById = useMemo(() => new Map(rankings.map((player) => [player.playerId, player])), [rankings]);
  const autoDraftPlayers = useMemo(() => mapRankingsToAutoDraftPlayers(rankings), [rankings]);
  const cpuSeed = useMemo(() => localAutoDraftSeed(storageKey, rankingSetId, draft.userSlot), [storageKey, rankingSetId, draft.userSlot]);

  // CPU opponents: same atomic-batch pattern as the Best Ball room -- apply
  // the whole run of consecutive CPU picks in one update, never a partial one.
  useEffect(() => {
    if (!draft.cpuEnabled) return;
    const onClockSlot = REDRAFT_SLOTS[draft.playerIds.length];
    if (!onClockSlot || onClockSlot.teamSlot === draft.userSlot) return;
    const batch = computeCpuDraftBatch({
      slots: REDRAFT_SLOTS,
      players: autoDraftPlayers,
      playerIds: draft.playerIds,
      userSlot: draft.userSlot,
      teamCount: REDRAFT_TEAM_COUNT,
      rosterConfig: REDRAFT_AUTO_DRAFT_ROSTER_CONFIG,
      seed: cpuSeed,
    });
    if (!batch.length) return;
    const basePlayerIds = draft.playerIds;
    updateDraft((latest) => latest.playerIds.length === basePlayerIds.length
      && basePlayerIds.every((id, index) => latest.playerIds[index] === id)
      ? { ...latest, playerIds: [...latest.playerIds, ...batch] }
      : latest);
  }, [draft.cpuEnabled, draft.playerIds, draft.userSlot, autoDraftPlayers, cpuSeed, updateDraft]);

  const currentSlot = REDRAFT_SLOTS[draft.playerIds.length] ?? null;
  const currentTeamSlot = currentSlot?.teamSlot ?? null;
  const targetOverallPick = currentSlot
    ? nextControlledPick(currentSlot.overallPick, draft.userSlot, REDRAFT_TEAM_COUNT, REDRAFT_ROUNDS)
    : null;

  const availabilityByPlayerId = useMemo(() => new Map(rankings.map((player) => [
    player.playerId,
    currentSlot && targetOverallPick !== null
      ? computeAvailabilityOdds(
        { adp: player.adp, adpStdev: player.adpStdev, adpSampleSize: player.adpSampleSize },
        { currentPick: currentSlot.overallPick, targetPick: targetOverallPick, teamCount: REDRAFT_TEAM_COUNT },
      )
      : null,
  ])), [rankings, currentSlot, targetOverallPick]);

  const rosters = useMemo(() => {
    const result = new Map<number, FantasyRankingRow[]>();
    for (let slot = 1; slot <= REDRAFT_TEAM_COUNT; slot += 1) result.set(slot, []);
    draft.playerIds.forEach((playerId, index) => {
      const player = playerById.get(playerId);
      const slot = REDRAFT_SLOTS[index];
      if (player && slot) result.get(slot.teamSlot)?.push(player);
    });
    return result;
  }, [draft.playerIds, playerById]);

  const displayTeam = viewTeam ?? currentTeamSlot ?? draft.userSlot;
  const displayRoster = useMemo(() => rosters.get(displayTeam) ?? [], [displayTeam, rosters]);
  const displayStatus = useMemo(() => getRedraftRosterStatus(displayRoster), [displayRoster]);
  const currentRoster = useMemo(() => currentTeamSlot ? rosters.get(currentTeamSlot) ?? [] : [], [currentTeamSlot, rosters]);
  const currentStatus = useMemo(() => getRedraftRosterStatus(currentRoster), [currentRoster]);
  const followingCurrentTeamPick = currentSlot && currentTeamSlot
    ? nextControlledPick(currentSlot.overallPick + 1, currentTeamSlot, REDRAFT_TEAM_COUNT, REDRAFT_ROUNDS)
    : null;
  const canDraftCurrentPick = Boolean(currentSlot)
    && currentStatus.size < REDRAFT_ROSTER_SIZE
    && (!draft.cpuEnabled || currentTeamSlot === draft.userSlot);
  const yahooStrategy = useMemo(() => {
    if (!currentSlot) return null;
    const toStrategyPlayer = (player: FantasyRankingRow): YahooRedraftStrategyPlayer => ({
      playerId: player.playerId,
      name: player.name,
      position: player.position,
      team: player.team,
      projectedPoints: player.ourProjectedPoints,
      expectedGames: player.expectedGames,
      ourRank: player.ourRank,
      yahooXRank: player.yahooXRank,
      yahooAdp: player.yahooAdp,
    });
    return buildYahooRedraftStrategy({
      roster: currentRoster.map(toStrategyPlayer),
      availablePlayers: rankings.filter((player) => !draft.playerIds.includes(player.playerId)).map(toStrategyPlayer),
      nextPick: currentSlot.overallPick,
      followingPick: followingCurrentTeamPick,
      teamCount: REDRAFT_TEAM_COUNT,
    });
  }, [currentSlot, currentRoster, rankings, draft.playerIds, followingCurrentTeamPick]);

  const draftPlayer = useCallback((playerId: number) => {
    updateDraft((latest) => {
      if (latest.playerIds.includes(playerId) || latest.playerIds.length >= TOTAL_PICKS) return latest;
      const slot = REDRAFT_SLOTS[latest.playerIds.length];
      const roster = latest.playerIds.flatMap((id, index) => REDRAFT_SLOTS[index]?.teamSlot === slot.teamSlot ? [playerById.get(id)] : [])
        .filter((player): player is FantasyRankingRow => Boolean(player));
      const player = playerById.get(playerId);
      return player && canAddRedraftPlayer(roster, player) ? { ...latest, playerIds: [...latest.playerIds, playerId] } : latest;
    });
  }, [playerById, updateDraft]);

  const undoLastPick = () => updateDraft((latest) => {
    if (!latest.playerIds.length) return latest;
    if (!latest.cpuEnabled) return { ...latest, playerIds: latest.playerIds.slice(0, -1) };
    // CPU mode: roll back to right before the user's own most recent pick,
    // taking every later (deterministic, otherwise instantly re-drafted) CPU
    // pick with it -- same grouped-undo behavior as the Best Ball room.
    let cutIndex = latest.playerIds.length - 1;
    while (cutIndex >= 0 && REDRAFT_SLOTS[cutIndex]?.teamSlot !== latest.userSlot) cutIndex -= 1;
    return cutIndex < 0 ? latest : { ...latest, playerIds: latest.playerIds.slice(0, cutIndex) };
  });
  const resetDraft = () => updateDraft((latest) => latest.playerIds.length ? { userSlot: latest.userSlot, cpuEnabled: latest.cpuEnabled, playerIds: [] } : latest);
  const setUserSlot = (userSlot: number) => {
    updateDraft((latest) => latest.userSlot === userSlot ? latest : { ...latest, userSlot });
    setViewTeam(null);
  };
  const setCpuEnabled = (cpuEnabled: boolean) => updateDraft((latest) => latest.cpuEnabled === cpuEnabled ? latest : { ...latest, cpuEnabled });

  return <div className="space-y-6">
    <section className="rounded-2xl border bg-slate-950 p-5 text-white">
      <div className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <p className="text-xs font-bold uppercase tracking-widest text-emerald-300">{REDRAFT_TEAM_COUNT}-team · {REDRAFT_ROUNDS}-round snake · PPR</p>
          <h2 className="mt-1 text-2xl font-black">{draft.cpuEnabled ? "Mock vs CPU" : "Mock draft room"}</h2>
          <p className="mt-1 text-sm text-slate-300">{draft.cpuEnabled ? "Computer opponents draft automatically, seeded and ADP/roster-aware, until your team is on the clock." : `You control all ${REDRAFT_TEAM_COUNT} teams. Every Draft records the current team's pick and advances the snake.`}</p>
        </div>
        <div className="flex flex-wrap items-end gap-3">
          <label className="text-xs font-bold uppercase tracking-wide text-slate-300">Opponents<span className="mt-1 flex items-center gap-2 rounded-lg border border-white/20 bg-slate-900 px-3 py-2"><input type="checkbox" checked={draft.cpuEnabled} onChange={(event) => setCpuEnabled(event.target.checked)} className="h-4 w-4" /><span className="text-sm font-bold text-white">CPU auto-draft</span></span></label>
          <label className="text-xs font-bold uppercase tracking-wide text-slate-300">My draft position<select value={draft.userSlot} onChange={(event) => setUserSlot(Number(event.target.value))} className="mt-1 block rounded-lg border border-white/20 bg-slate-900 px-3 py-2 text-sm font-bold text-white">{Array.from({ length: REDRAFT_TEAM_COUNT }, (_, index) => <option key={index + 1} value={index + 1}>Slot {index + 1}</option>)}</select></label>
        </div>
      </div>
      <div className="mt-5 grid gap-3 md:grid-cols-[1fr_auto] md:items-center">
        <div>{currentSlot ? <>
          <p className="text-sm text-slate-300">On the clock</p>
          <p className="text-3xl font-black">{currentSlot.teamSlot === draft.userSlot ? "My Team" : `Team ${currentSlot.teamSlot}${draft.cpuEnabled ? " (CPU)" : ""}`} <span className="text-lg text-emerald-300">· Pick {currentSlot.overallPick}/{TOTAL_PICKS} · Round {currentSlot.round}</span></p>
        </> : <>
          <p className="text-sm text-emerald-300">Draft complete</p>
          <p className="text-3xl font-black">{TOTAL_PICKS} picks recorded</p>
        </>}</div>
        <div className="flex gap-2">
          <button disabled={!draft.playerIds.length} onClick={undoLastPick} className="rounded-lg border border-white/20 px-3 py-2 text-sm font-semibold disabled:opacity-30">Undo last pick</button>
          <button disabled={!draft.playerIds.length} onClick={resetDraft} className="rounded-lg border border-red-400/40 px-3 py-2 text-sm font-semibold text-red-200 disabled:opacity-30">Reset draft</button>
        </div>
      </div>
      <div className="mt-5 grid grid-cols-5 gap-2 md:grid-cols-10">{Array.from({ length: REDRAFT_TEAM_COUNT }, (_, index) => {
        const slot = index + 1;
        const count = rosters.get(slot)?.length ?? 0;
        const isCurrent = slot === currentTeamSlot;
        const isMine = slot === draft.userSlot;
        return <button key={slot} onClick={() => setViewTeam(slot)} className={`rounded-lg border p-2 text-left text-xs ${isCurrent ? "border-emerald-300 bg-emerald-500/25 ring-1 ring-emerald-300" : isMine ? "border-emerald-400/60 bg-emerald-500/15" : "border-white/15 bg-white/5"}`}><b>{isMine ? "MY" : `T${slot}`}</b><span className="block text-slate-300">{count}/{REDRAFT_ROSTER_SIZE}</span></button>;
      })}</div>
    </section>

    <YahooRedraftStrategyPanel strategy={yahooStrategy} canDraft={canDraftCurrentPick} onDraft={draftPlayer} />

    <section className="grid gap-5 xl:grid-cols-[1fr_320px]">
      <div className="rounded-2xl border bg-card p-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div><p className="text-xs font-bold uppercase tracking-widest text-emerald-700">{viewTeam === null ? "Following the clock" : "Roster review"}</p><h2 className="text-2xl font-black">{displayTeam === draft.userSlot ? "My Team" : `Team ${displayTeam}`} roster</h2></div>
          <div className="flex items-center gap-2">
            {viewTeam !== null && <button onClick={() => setViewTeam(null)} className="rounded-lg border px-2 py-1 text-xs font-semibold">Follow clock</button>}
            <span className={`rounded-full px-3 py-1 text-xs font-bold ${displayStatus.canFieldLegalLineup ? "bg-emerald-100 text-emerald-800" : "bg-amber-100 text-amber-900"}`}>{displayStatus.size}/{REDRAFT_ROSTER_SIZE}</span>
          </div>
        </div>
        <div className="mt-4 grid grid-cols-3 gap-2 sm:grid-cols-6">{REDRAFT_POSITIONS.map((value) => <div key={value} className="rounded-xl bg-muted p-3 text-center"><p className="text-xs font-bold text-muted-foreground">{REDRAFT_POSITION_LABEL[value]}</p><p className="text-xl font-black">{displayStatus.counts[value]}<span className="text-xs font-normal text-muted-foreground"> / {REDRAFT_STARTER_SLOTS[value]}</span></p></div>)}</div>
        <p className="mt-2 text-xs text-muted-foreground">Counts show drafted vs. dedicated starting slots. Yahoo does not block an unbalanced roster, so neither does this room &mdash; these are guidance, not gates.</p>
        <div className="mt-4 flex flex-wrap gap-1.5">{displayStatus.gates.map((gate) => <span key={gate.code} className={`rounded-full px-2 py-1 text-[10px] font-bold ${gate.pass ? "bg-emerald-100 text-emerald-800" : "bg-slate-100 text-slate-600"}`}>{gate.pass ? "✓" : "○"} {gate.label}</span>)}</div>
        <div className="mt-5 max-h-72 space-y-1 overflow-auto">{displayRoster.length ? displayRoster.map((player, index) => <div key={player.playerId} className="flex items-center gap-3 rounded-lg border px-3 py-2 text-sm"><span className="w-6 text-xs font-bold text-muted-foreground">{index + 1}</span><div className="min-w-0 flex-1"><b>{player.name}</b><span className="ml-2 text-xs text-muted-foreground">{REDRAFT_POSITION_LABEL[player.position as keyof typeof REDRAFT_POSITION_LABEL] ?? player.position} · {player.team ?? "FA"} · Bye {player.byeWeek ?? "—"}</span></div></div>) : <p className="rounded-xl bg-muted p-5 text-center text-sm text-muted-foreground">No picks recorded for this team.</p>}</div>
      </div>

      <aside className="space-y-4">
        <div className="rounded-2xl border bg-card p-5">
          <p className="text-xs font-bold uppercase tracking-widest text-emerald-700">Current-team needs</p>
          <div className="mt-3 grid grid-cols-3 gap-2">{REDRAFT_POSITIONS.map((value) => <div key={value} className="text-center"><p className="text-xs text-muted-foreground">{REDRAFT_POSITION_LABEL[value]}</p><p className="font-black">{currentStatus.counts[value]}/{REDRAFT_STARTER_SLOTS[value]}</p></div>)}</div>
        </div>
        <div className="rounded-2xl border bg-slate-950 p-5 text-white">
          <p className="text-xs font-bold uppercase tracking-widest text-emerald-300">Weekly starting lineup</p>
          <p className="mt-2 text-2xl font-black">1 QB · 2 RB · 2 WR</p>
          <p className="text-lg font-bold">1 TE · {REDRAFT_FLEX_SLOTS} W/R/T · 1 K · 1 DEF</p>
          <p className="mt-2 text-sm text-slate-300">{REDRAFT_STARTER_COUNT} starters + {REDRAFT_BENCH_SLOTS} bench = {REDRAFT_ROSTER_SIZE} drafted. Yahoo&apos;s 2 IR spots are not drafted.</p>
        </div>
      </aside>
    </section>

    <RedraftPlayerBoard
      rankings={rankings}
      draftedPlayerIds={draft.playerIds}
      canDraft={canDraftCurrentPick}
      onDraft={draftPlayer}
      availabilityByPlayerId={availabilityByPlayerId}
    />
  </div>;
}
