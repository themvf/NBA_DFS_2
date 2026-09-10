"use client";

import { useMemo, useState } from "react";
import type { NflArchetypeGameRow, NflArchetypePlayRow } from "@/db/queries";
import s from "../../cfb/cfb-terminal.module.css";
import n from "../nfl-terminal.module.css";
import p from "./pbp-archetype.module.css";

type Props = { games: NflArchetypeGameRow[]; gameId: string | null; plays: NflArchetypePlayRow[] };

// Colour carries MEANING, not decoration: scoring green, giveaway red, negative
// amber, neutral grey. Every chip also shows its text, so colour is never the
// only channel.
const PLAY_TONE: Record<string, string> = {
  EARLY_DOWN_EXPLOSIVE: "good", EARLY_DOWN_SUCCESS: "good", LATE_DOWN_CONVERSION: "good",
  EARLY_DOWN_FAILURE: "warn", LATE_DOWN_FAILURE: "warn",
  SACK: "warn", TURNOVER_PLAY: "bad", PENALTY: "warn",
  SPECIAL_TEAMS: "mute", KNEEL: "mute", SPIKE: "mute", NON_PLAY: "mute", EARLY_DOWN_MODEST: "",
};
const DRIVE_TONE: Record<string, string> = {
  METHODICAL_TD: "good", EXPLOSIVE_TD: "good", SHORT_FIELD_TD: "good",
  RED_ZONE_SETTLE_FG: "", LONG_FG: "", MISSED_FG: "warn", STALLED: "warn",
  THREE_AND_OUT: "warn", TURNOVER_GIVEAWAY: "bad", TURNOVER_ON_DOWNS: "bad",
  SCORE_AGAINST: "bad", CLOCK_EXPIRED: "mute", KNEEL_DOWN: "mute",
};

const label = (v: string) => v.replaceAll("_", " ");
const num = (v: number | null, digits = 0) => (v == null ? "—" : v.toFixed(digits));

export default function PbpArchetypeClient({ games, gameId, plays }: Props) {
  const [team, setTeam] = useState("all");
  const [playFilter, setPlayFilter] = useState("all");
  const game = games.find(g => g.gameId === gameId) ?? null;

  const playTypes = useMemo(
    () => [...new Set(plays.map(x => x.playArchetype))].sort(),
    [plays],
  );
  const rows = useMemo(
    () => plays.filter(x =>
      (team === "all" || x.posteam === team) &&
      (playFilter === "all" || x.playArchetype === playFilter)),
    [plays, team, playFilter],
  );

  return <div className={`${s.terminal} ${n.focus} ${p.wrap}`}>
    <section className={p.head}>
      <div>
        <h1 className={p.title}>{game ? `${game.awayTeam} @ ${game.homeTeam}` : "PBP ARCHETYPE"}</h1>
        <p className={p.meta}>
          {game
            ? `${game.season}${game.week ? ` · WK ${game.week}` : ""} · ${game.plays} plays · ${game.playVersion} + ${game.driveVersion}`
            : "Select a labelled game."}
        </p>
      </div>
      <p className={p.note}>
        Descriptive labels only. No prediction, no betting claim: these archetypes have not
        been tested against a closing line, and none of them is evidence of an edge.
      </p>
    </section>

    <section className={p.controls}>
      <label>GAME
        <select value={gameId ?? ""} onChange={e => { window.location.search = `?game=${encodeURIComponent(e.target.value)}`; }}>
          {!gameId && <option value="">Select…</option>}
          {games.map(g => <option key={g.gameId} value={g.gameId}>
            {g.season} {g.week ? `WK${g.week}` : ""} · {g.awayTeam} @ {g.homeTeam}
          </option>)}
        </select>
      </label>
      <label>TEAM
        <select value={team} onChange={e => setTeam(e.target.value)}>
          <option value="all">Both</option>
          {game && [game.awayTeam, game.homeTeam].map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </label>
      <label>PLAY ARCHETYPE
        <select value={playFilter} onChange={e => setPlayFilter(e.target.value)}>
          <option value="all">All</option>
          {playTypes.map(t => <option key={t} value={t}>{label(t)}</option>)}
        </select>
      </label>
      <span className={p.count}>{rows.length} of {plays.length} plays</span>
    </section>

    <div className={p.scroll}>
      <table className={p.table}>
        <thead><tr>
          {["Q", "Clock", "Off", "Dr", "Dn", "Dist", "Yd", "Type", "Gain",
            "Pers", "Box", "PLAY ARCHETYPE", "Pre-snap", "DRIVE ARCHETYPE", "Drive flags",
            "Drive QB", "EPA", "Description"]
            .map(h => <th key={h}>{h}</th>)}
        </tr></thead>
        <tbody>
          {rows.map((r, i) => {
            // A rule between drives makes the drive structure readable without
            // a second grouping column.
            const newDrive = i > 0 && rows[i - 1].drive !== r.drive;
            return <tr key={r.playId} className={newDrive ? p.driveBreak : undefined}>
              <td>{r.quarter ?? "—"}</td>
              <td className={p.mono}>{r.clock ?? "—"}</td>
              <td><strong>{r.posteam}</strong></td>
              <td>{r.drive ?? "—"}</td>
              <td>{r.down ?? "—"}</td>
              {/* Distance is meaningless without a down: a kickoff carries
                  ydstogo 0, and printing "0" invites reading it as 0 to go. */}
              <td>{r.down == null || r.ydstogo == null ? "—" : `${r.ydstogo}${r.distanceBucket && r.distanceBucket !== "unknown" ? ` ${r.distanceBucket[0].toUpperCase()}` : ""}`}</td>
              <td>{r.yardline100 ?? "—"}</td>
              <td>{r.playType ?? "—"}</td>
              <td data-gain={r.yardsGained == null ? "" : r.yardsGained > 0 ? "pos" : r.yardsGained < 0 ? "neg" : ""}>
                {num(r.yardsGained)}{r.explosive ? " ⚡" : ""}
              </td>
              {/* Personnel and box: what down-and-distance cannot say. Blank
                  where nflverse publishes no participation for the season. */}
              <td className={p.mono}>{r.personnelGrouping ?? "—"}</td>
              <td className={p.mono}>{r.defendersInBox == null ? "—" : r.defendersInBox.toFixed(0)}</td>
              <td><span className={p.chip} data-tone={PLAY_TONE[r.playArchetype] ?? ""}>{label(r.playArchetype)}</span>
                {r.hadSack && r.playArchetype !== "SACK"
                  ? <span className={p.flag} data-tone="bad" title="Strip sack: this play was both a sack and a lost fumble. TURNOVER_PLAY outranks SACK, so without this flag the sack would vanish from the count.">+SACK</span>
                  : null}</td>
              <td className={p.flags}>
                {r.goalLine ? <span className={p.flag} data-tone="good" title="Snap from inside the opponent's 5, run or pass">G-LINE</span> : null}
                {r.pressure ? <span className={p.flag} data-tone="bad" title="Quarterback was pressured">PRESS</span> : null}
                {r.blitz ? <span className={p.flag} data-tone="warn" title="Five or more pass rushers">BLITZ</span> : null}
                {r.formation && r.formation !== "SHOTGUN" ? <span className={p.flag}>{r.formation.replace("_", " ")}</span> : null}
                {r.coverageType ? <span className={p.flag}>{r.coverageType.replace("COVER_", "C")}</span> : null}
                {r.penaltyFirstDown ? <span className={p.flag} data-tone="warn" title="Penalty moved the chains">PEN 1ST</span> : null}
                {!r.goalLine && !r.pressure && !r.blitz && !r.coverageType && !r.penaltyFirstDown && (!r.formation || r.formation === "SHOTGUN") ? <span className={p.none}>—</span> : null}
              </td>
              <td>{r.driveArchetype
                ? <span className={p.chip} data-tone={DRIVE_TONE[r.driveArchetype] ?? ""}>{label(r.driveArchetype)}</span>
                : <span className={p.none} title="This play sits on a possession the drive labeller does not recognise as a drive — a conversion attempt after a return touchdown. Left blank rather than guessed.">no drive</span>}</td>
              {/* Independent flags, shown together because they ARE together:
                  a drive can be sacked AND penalised AND stopped short, and
                  none of the three outranks the others. */}
              <td className={p.flags}>
                {r.driveHadSack ? <span className={p.flag} data-tone="bad" title="A sack occurred on this drive">SACK</span> : null}
                {r.driveHadPenalty ? <span className={p.flag} data-tone="warn" title="A penalty wiped out a play on this drive">PEN</span> : null}
                {r.driveFailedShort ? <span className={p.flag} data-tone="warn" title="Final snap was 3rd/4th down with 2 or fewer to go">SHORT</span> : null}
                {r.driveTurnoverType ? <span className={p.flag} data-tone="bad">{r.driveTurnoverType === "interception" ? "INT" : "FUM"}</span> : null}
                {!r.driveHadSack && !r.driveHadPenalty && !r.driveFailedShort && !r.driveTurnoverType ? <span className={p.none}>—</span> : null}
              </td>
              <td>{r.driveQb ?? "—"}{r.driveQbIsStarter === false ? <span className={p.backup} title="Not this team's starting quarterback">◦</span> : null}</td>
              <td data-gain={r.epa == null ? "" : r.epa > 0 ? "pos" : r.epa < 0 ? "neg" : ""}>{num(r.epa, 2)}</td>
              <td className={p.desc} title={r.description ?? ""}>{r.description ?? "—"}</td>
            </tr>;
          })}
        </tbody>
      </table>
      {!rows.length && <div className={n.empty}>
        {plays.length ? "No plays match this filter." : "No labelled plays for this game."}
      </div>}
    </div>
  </div>;
}
