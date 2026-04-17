"use client";

import { useMemo } from "react";
import type { DfsPagePlayerRow, MlbGameEnvironmentCard, MlbPitcherSlateSignal } from "@/db/queries";

type Props = {
  cards: MlbGameEnvironmentCard[];
  players: DfsPagePlayerRow[];
  signals: MlbPitcherSlateSignal[];
  selectedGames: Set<string>;
  onToggleGame: (gameKey: string) => void;
  onSelectAll: () => void;
  onSelectNone: () => void;
};

type SpInfo = {
  playerId: number | null;
  name: string | null;
  hand: string | null;
  kPer9: number | null;
  xfip: number | null;
  era: number | null;
  signal: MlbPitcherSlateSignal | null;
};

function normalizeName(name: string | null | undefined): string {
  if (!name) return "";
  return name
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-zA-Z0-9]/g, "")
    .toLowerCase();
}

function classifyTotal(total: number | null): { bg: string; text: string; tag: string } {
  if (total == null) return { bg: "bg-gray-100", text: "text-gray-600", tag: "—" };
  if (total >= 10) return { bg: "bg-red-100", text: "text-red-800", tag: "Hot" };
  if (total >= 9) return { bg: "bg-orange-100", text: "text-orange-800", tag: "Warm" };
  if (total >= 8) return { bg: "bg-yellow-50", text: "text-yellow-800", tag: "Neutral" };
  return { bg: "bg-blue-50", text: "text-blue-800", tag: "Cold" };
}

function fmtOdds(ml: number | null): string {
  if (ml == null) return "—";
  return ml > 0 ? `+${ml}` : `${ml}`;
}

function fmtImplied(v: number | null): string {
  return v != null ? v.toFixed(1) : "—";
}

function fmtStat(v: number | null, digits = 2): string {
  return v != null ? v.toFixed(digits) : "—";
}

function windGlyph(direction: string | null): string {
  if (!direction) return "·";
  const d = direction.toLowerCase();
  if (d.includes("out")) return "↑";
  if (d.includes("in")) return "↓";
  if (d.includes("l to r") || d.includes("left")) return "→";
  if (d.includes("r to l") || d.includes("right")) return "←";
  return "·";
}

function formatWind(speed: number | null, direction: string | null): string | null {
  if (speed == null && !direction) return null;
  const glyph = windGlyph(direction);
  const parts: string[] = [];
  if (direction) parts.push(direction);
  if (speed != null) parts.push(`${speed} mph`);
  return `${glyph} ${parts.join(" ")}`.trim();
}

export default function MlbGameCardStrip({
  cards,
  players,
  signals,
  selectedGames,
  onToggleGame,
  onSelectAll,
  onSelectNone,
}: Props) {
  // Signal lookup by dk_player id
  const signalById = useMemo(() => {
    const map = new Map<number, MlbPitcherSlateSignal>();
    for (const s of signals) map.set(s.playerId, s);
    return map;
  }, [signals]);

  // SP resolver: for (teamAbbrev, spName) → dk_players row
  // Find pitcher in players array matching team + name (accent-normalized)
  const resolveSp = useMemo(() => {
    const spRows = players.filter(
      (p) => (p.eligiblePositions ?? "").includes("SP")
    );
    const byTeamName = new Map<string, DfsPagePlayerRow>();
    for (const p of spRows) {
      const key = `${(p.teamAbbrev ?? "").toUpperCase()}::${normalizeName(p.name)}`;
      byTeamName.set(key, p);
    }
    const byTeamHighestSalary = new Map<string, DfsPagePlayerRow>();
    for (const p of spRows) {
      const team = (p.teamAbbrev ?? "").toUpperCase();
      const existing = byTeamHighestSalary.get(team);
      if (!existing || (p.salary ?? 0) > (existing.salary ?? 0)) {
        byTeamHighestSalary.set(team, p);
      }
    }
    return (teamAbbrev: string | null, spName: string | null): DfsPagePlayerRow | null => {
      if (!teamAbbrev) return null;
      const team = teamAbbrev.toUpperCase();
      if (spName) {
        const hit = byTeamName.get(`${team}::${normalizeName(spName)}`);
        if (hit) return hit;
      }
      return byTeamHighestSalary.get(team) ?? null;
    };
  }, [players]);

  const enrichedCards = useMemo(() => {
    return cards.map((card) => {
      const gameKey =
        card.awayAbbrev && card.homeAbbrev
          ? `${card.awayAbbrev}@${card.homeAbbrev}`
          : "Unknown";
      const homeSpRow = resolveSp(card.homeAbbrev, card.homeSpName);
      const awaySpRow = resolveSp(card.awayAbbrev, card.awaySpName);
      const homeSp: SpInfo = {
        playerId: homeSpRow?.id ?? null,
        name: card.homeSpName ?? homeSpRow?.name ?? null,
        hand: card.homeSpHand,
        kPer9: card.homeSpKPer9,
        xfip: card.homeSpXfip,
        era: card.homeSpEra,
        signal: homeSpRow ? signalById.get(homeSpRow.id) ?? null : null,
      };
      const awaySp: SpInfo = {
        playerId: awaySpRow?.id ?? null,
        name: card.awaySpName ?? awaySpRow?.name ?? null,
        hand: card.awaySpHand,
        kPer9: card.awaySpKPer9,
        xfip: card.awaySpXfip,
        era: card.awaySpEra,
        signal: awaySpRow ? signalById.get(awaySpRow.id) ?? null : null,
      };
      return { card, gameKey, homeSp, awaySp };
    });
  }, [cards, resolveSp, signalById]);

  if (cards.length === 0) return null;

  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-baseline gap-2">
          <h2 className="text-sm font-semibold">Game Environment</h2>
          <span className="text-xs text-gray-500">
            {selectedGames.size}/{cards.length} selected · Click a card to toggle
          </span>
        </div>
        <div className="flex gap-2">
          <button
            onClick={onSelectAll}
            className="text-xs text-blue-600 hover:underline"
            type="button"
          >
            All
          </button>
          <button
            onClick={onSelectNone}
            className="text-xs text-gray-500 hover:underline"
            type="button"
          >
            None
          </button>
        </div>
      </div>

      <div className="flex gap-3 overflow-x-auto pb-2">
        {enrichedCards.map(({ card, gameKey, homeSp, awaySp }) => {
          const total = card.vegasTotal;
          const totalClass = classifyTotal(total);
          const selected = selectedGames.has(gameKey);
          const wind = formatWind(card.windSpeed, card.windDirection);

          return (
            <button
              key={card.matchupId}
              type="button"
              onClick={() => onToggleGame(gameKey)}
              className={`min-w-[280px] max-w-[300px] flex-shrink-0 rounded-lg border p-3 text-left transition-all ${
                selected
                  ? "border-blue-500 bg-white ring-2 ring-blue-200"
                  : "border-gray-200 bg-gray-50 opacity-70 hover:opacity-100 hover:border-gray-400"
              }`}
            >
              {/* Teams row */}
              <div className="flex items-center justify-between gap-2">
                <TeamColumn
                  abbrev={card.awayAbbrev}
                  logo={card.awayLogo}
                  implied={card.awayImplied}
                  ml={card.awayMl}
                  align="left"
                />
                <div className="flex flex-col items-center">
                  <div
                    className={`rounded px-2 py-0.5 text-[11px] font-semibold ${totalClass.bg} ${totalClass.text}`}
                    title={`Vegas O/U: ${total != null ? total.toFixed(1) : "—"} (${totalClass.tag})`}
                  >
                    {total != null ? total.toFixed(1) : "—"}
                  </div>
                  <div className="mt-0.5 text-[10px] uppercase tracking-wide text-gray-400">
                    O/U
                  </div>
                </div>
                <TeamColumn
                  abbrev={card.homeAbbrev}
                  logo={card.homeLogo}
                  implied={card.homeImplied}
                  ml={card.homeMl}
                  align="right"
                />
              </div>

              {/* Park / weather row */}
              <div className="mt-2 flex items-center justify-between gap-2 border-t border-gray-100 pt-2 text-[11px] text-gray-500">
                <span className="truncate" title={card.ballpark ?? ""}>
                  {card.ballpark ?? "—"}
                </span>
                <span className="whitespace-nowrap">
                  {card.weatherTemp != null ? `${card.weatherTemp}°F` : ""}
                  {wind ? ` · ${wind}` : ""}
                </span>
              </div>

              {/* Pitcher head-to-head */}
              <div className="mt-2 space-y-1 rounded bg-white/60 p-1.5">
                <SpRow label={card.awayAbbrev ?? "AWY"} sp={awaySp} />
                <SpRow label={card.homeAbbrev ?? "HM"} sp={homeSp} />
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function TeamColumn({
  abbrev,
  logo,
  implied,
  ml,
  align,
}: {
  abbrev: string | null;
  logo: string | null;
  implied: number | null;
  ml: number | null;
  align: "left" | "right";
}) {
  return (
    <div
      className={`flex flex-1 min-w-0 flex-col ${
        align === "right" ? "items-end text-right" : "items-start text-left"
      }`}
    >
      <div className="flex items-center gap-1.5">
        {align === "left" && logo && (
          <img src={logo} alt="" className="h-5 w-5" />
        )}
        <span className="font-mono text-sm font-semibold">{abbrev ?? "—"}</span>
        {align === "right" && logo && (
          <img src={logo} alt="" className="h-5 w-5" />
        )}
      </div>
      <div className="text-[11px] text-gray-500">
        <span className="tabular-nums">{fmtImplied(implied)}</span>
        <span className="ml-1 text-gray-400">({fmtOdds(ml)})</span>
      </div>
    </div>
  );
}

function SpRow({ label, sp }: { label: string; sp: SpInfo }) {
  if (!sp.name) {
    return (
      <div className="flex items-center justify-between text-[11px] text-gray-400">
        <span className="font-mono font-medium">{label}</span>
        <span className="italic">SP TBD</span>
      </div>
    );
  }
  const decision = sp.signal?.decisionBadge;
  const ceiling = sp.signal?.ceilingBadge;
  return (
    <div className="flex items-center justify-between gap-2 text-[11px]">
      <div className="flex min-w-0 items-center gap-1.5">
        <span className="font-mono text-[10px] font-medium text-gray-500">
          {label}
        </span>
        <span className="truncate font-medium text-gray-800" title={sp.name}>
          {sp.name}
        </span>
        {sp.hand && (
          <span className="rounded bg-gray-200 px-1 text-[9px] font-semibold text-gray-600">
            {sp.hand}
          </span>
        )}
      </div>
      <div className="flex flex-shrink-0 items-center gap-1.5 tabular-nums text-gray-600">
        <span title="K/9">K/9 {fmtStat(sp.kPer9, 1)}</span>
        <span className="text-gray-300">·</span>
        <span title="xFIP">xFIP {fmtStat(sp.xfip, 2)}</span>
        {decision && (
          <span className={`ml-1 rounded px-1 text-[9px] font-semibold ${decision.className}`} title={decision.title}>
            {decision.label}
          </span>
        )}
        {ceiling && (
          <span className={`rounded px-1 text-[9px] font-semibold ${ceiling.className}`} title={ceiling.title}>
            {ceiling.label}
          </span>
        )}
      </div>
    </div>
  );
}
