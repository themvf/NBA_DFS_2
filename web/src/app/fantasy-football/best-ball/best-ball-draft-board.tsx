"use client";

import { useMemo } from "react";
import type { FantasyRankingRow } from "@/db/queries-fantasy-football";
import { buildBestBallDraftBoard } from "@/lib/fantasy-football/best-ball";

const POSITION_STYLES: Record<string, string> = {
  QB: "border-red-300 bg-red-100 text-red-950",
  RB: "border-blue-300 bg-blue-100 text-blue-950",
  WR: "border-emerald-300 bg-emerald-100 text-emerald-950",
  TE: "border-amber-300 bg-amber-100 text-amber-950",
};

export default function BestBallDraftBoard({
  rankings,
  playerIds,
  userSlot,
}: {
  rankings: FantasyRankingRow[];
  playerIds: number[];
  userSlot: number;
}) {
  const playerById = useMemo(
    () => new Map(rankings.map((player) => [player.playerId, player])),
    [rankings],
  );
  const rounds = useMemo(() => buildBestBallDraftBoard(playerIds), [playerIds]);
  const currentOverallPick = playerIds.length + 1;

  return (
    <section className="min-w-0 max-w-full space-y-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="text-xs font-bold uppercase tracking-widest text-blue-700">Live draft tracker</p>
          <h2 className="text-2xl font-black">Draft Results</h2>
          <p className="mt-1 text-sm text-muted-foreground">
            Read across each round. Even rounds reverse direction because this is a snake draft.
          </p>
        </div>
        <div className="flex flex-wrap gap-2 text-xs font-bold">
          {Object.entries(POSITION_STYLES).map(([position, className]) => (
            <span key={position} className={`rounded-md border px-2.5 py-1 ${className}`}>{position}</span>
          ))}
          <span className="rounded-md border-2 border-violet-500 bg-violet-50 px-2.5 py-1 text-violet-950">MY TEAM</span>
        </div>
      </div>

      <div className="max-h-[72vh] w-full max-w-full overflow-auto rounded-2xl border bg-card shadow-sm [contain:inline-size]">
        <table className="w-full min-w-[1700px] table-fixed border-separate border-spacing-0 text-center">
          <thead className="sticky top-0 z-30 bg-slate-950 text-white">
            <tr>
              <th className="sticky left-0 z-40 w-16 border-b border-r border-slate-700 bg-slate-950 p-2 text-xs uppercase tracking-wide">Rd.</th>
              {Array.from({ length: 12 }, (_, index) => {
                const slot = index + 1;
                const isMine = slot === userSlot;
                return (
                  <th key={slot} className={`w-[136px] border-b border-r p-2 ${isMine ? "border-violet-300 bg-violet-700" : "border-slate-700"}`}>
                    <span className="block text-[10px] font-medium uppercase tracking-wide text-slate-300">Slot {slot}</span>
                    <span className="block truncate text-xs font-black">{isMine ? "MY TEAM" : `Team ${slot}`}</span>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {rounds.map((round, roundIndex) => (
              <tr key={roundIndex + 1}>
                <th className="sticky left-0 z-20 border-b border-r bg-slate-100 p-2 text-sm font-black text-slate-700">{roundIndex + 1}</th>
                {round.map((cell) => {
                  const player = cell.playerId ? playerById.get(cell.playerId) : null;
                  const isMine = cell.teamSlot === userSlot;
                  const isCurrent = cell.overallPick === currentOverallPick && currentOverallPick <= 240;
                  const positionClass = player
                    ? POSITION_STYLES[player.position] ?? "border-slate-300 bg-slate-100 text-slate-950"
                    : "border-slate-200 bg-white text-slate-400";
                  return (
                    <td key={cell.teamSlot} className={`relative h-[92px] border-b border-r p-1 align-middle ${isMine ? "bg-violet-50" : "bg-card"}`}>
                      <div className={`flex h-full min-h-[82px] flex-col items-center justify-center rounded-md border px-2 py-1.5 ${positionClass} ${isMine ? "ring-2 ring-inset ring-violet-400" : ""} ${isCurrent ? "animate-pulse ring-4 ring-inset ring-blue-600" : ""}`}>
                        <span className="absolute left-2 top-1.5 text-[9px] font-bold opacity-55">#{cell.overallPick}</span>
                        {player ? (
                          <>
                            <span className="max-w-full text-balance text-sm font-black leading-tight">{player.name}</span>
                            <span className="mt-1 text-[10px] font-semibold uppercase leading-tight opacity-80">
                              {player.position} ({player.team ?? "FA"}) · Bye {player.byeWeek ?? "—"}
                            </span>
                          </>
                        ) : isCurrent ? (
                          <>
                            <span className="text-xs font-black text-blue-800">ON THE CLOCK</span>
                            <span className="mt-1 text-[10px] font-semibold text-blue-700">Pick {cell.overallPick}</span>
                          </>
                        ) : (
                          <span className="text-xs font-semibold">Open</span>
                        )}
                      </div>
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-xs text-muted-foreground">The header and round column stay visible while you scroll. Player colors represent position; the violet outline marks your team.</p>
    </section>
  );
}
