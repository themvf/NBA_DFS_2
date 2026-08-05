"use client";

import { useState } from "react";
import type { FantasyPercentileProfile } from "@/db/queries-fantasy-football";
import { fetchFantasyPercentileProfile } from "../actions";
import {
  PERCENTILE_PROFILE_GROUPS,
  percentileBarTone,
  percentileTone,
  type PercentileStatGroup,
} from "@/lib/fantasy-football/percentile-profile";

type LoadState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ready"; profile: FantasyPercentileProfile };

export default function PercentileProfileChip({
  playerId,
  position,
  season,
  scoring,
}: {
  playerId: number;
  position: string;
  season: number;
  scoring: string;
}) {
  const [open, setOpen] = useState(false);
  const [state, setState] = useState<LoadState>({ status: "idle" });
  const groups = PERCENTILE_PROFILE_GROUPS[position as "RB" | "WR" | "TE"];
  // QB/K/DST percentile profiles aren't built yet -- no chip at all rather
  // than a dead one that always says "unsupported."
  if (!groups) return null;

  const load = async () => {
    if (state.status === "loading" || state.status === "ready") return;
    setState({ status: "loading" });
    const result = await fetchFantasyPercentileProfile({ playerId, season, scoring });
    setState(result.ok ? { status: "ready", profile: result.profile } : { status: "error", message: result.error });
  };

  return (
    <details
      open={open}
      onToggle={(event) => {
        const next = event.currentTarget.open;
        setOpen(next);
        if (next) void load();
      }}
      className="mt-1 text-xs font-normal text-muted-foreground"
    >
      <summary className="cursor-pointer select-none font-semibold text-violet-700 hover:underline">Percentile profile</summary>
      {open && (
        <div className="mt-2 w-80 max-w-[85vw] rounded-lg border border-slate-800 bg-slate-950 p-3 text-left text-white shadow-sm">
          {state.status === "loading" && <p className="text-slate-400">Loading percentile profile...</p>}
          {state.status === "error" && <p className="text-red-400">{state.message}</p>}
          {state.status === "ready" && !state.profile.eligible && <p className="text-amber-400">{state.profile.reason}</p>}
          {state.status === "ready" && state.profile.eligible && <ProfileBody profile={state.profile} groups={groups} />}
        </div>
      )}
    </details>
  );
}

function ProfileBody({ profile, groups }: { profile: FantasyPercentileProfile; groups: PercentileStatGroup[] }) {
  return (
    <div className="space-y-3">
      <p className="text-[10px] uppercase tracking-wide text-slate-400">
        {profile.season} · {profile.games} games · percentile among {profile.positionPoolSize} qualifying {profile.position}s
      </p>
      {groups.map((group) => (
        <div key={group.label}>
          <p className="mb-1 text-[10px] font-bold uppercase tracking-wide text-slate-500">{group.label}</p>
          <div className="space-y-1.5">
            {group.stats.map((stat) => {
              const value = profile.stats[stat.key];
              if (!value) return null;
              return (
                <div key={stat.key} className="flex items-center gap-2">
                  <span className="w-28 shrink-0 text-slate-300">{stat.label}</span>
                  <div className="h-1.5 flex-1 rounded-full bg-slate-800">
                    <div className={`h-1.5 rounded-full ${percentileBarTone(value.percentile)}`} style={{ width: `${value.percentile ?? 0}%` }} />
                  </div>
                  <span className={`w-7 shrink-0 text-right font-mono font-bold ${percentileTone(value.percentile)}`}>{value.percentile ?? "—"}</span>
                  <span className="w-16 shrink-0 text-right font-mono text-slate-400">
                    {value.value === null ? "—" : value.value.toFixed(stat.decimals)}
                    {stat.suffix ?? ""}
                  </span>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}
